"""
Jembatan MQTT -> Direwolf KISS -> APRS radio.
Jalankan setelah main.py; Direwolf akan dijalankan otomatis jika belum aktif.
Install: pip install paho-mqtt
"""
import json
import logging
import re
import socket
import subprocess
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

# ====== KONFIGURASI ======
CALLSIGN = "YD3ADB-7"
MQTT_HOST = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_TOPIC = "suhu_ukdc"
KIRIM_TIAP = 15

# --- Posisi & teks APRS ---
LAT = -7.250445
LON = 112.768845
KOMENTAR = "Telemetry suhu UKDC"
STATUS = "Keito sukses"
SIMBOL_TABEL = "/"
SIMBOL_KODE = "I"
POSISI_TIAP = 600

# Direwolf berada di folder sejajar dengan "bot suhu".
BASE_DIR = Path(__file__).resolve().parent
DIREWOLF_DIR = (
    BASE_DIR.parent
    / "direwolf-1.8.1-a231971_x86_64"
    / "direwolf-1.8.1-a231971_x86_64"
)
DIREWOLF_EXE = DIREWOLF_DIR / "direwolf.exe"
DIREWOLF_CONFIG = BASE_DIR / "Direwolf.cfg"
KISS_HOST = "127.0.0.1"
KISS_PORT = 8001
KISS_CONNECT_TIMEOUT = 15
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("aprs-telemetry")

seq = 0
terakhir_kirim = 0
terakhir_posisi = 0
suhu_terakhir = None
tnc = None
send_lock = threading.Lock()


def encode_ax25_address(address, last):
    call, separator, ssid_text = address.partition("-")
    if not call or len(call) > 6 or not call.isalnum():
        raise ValueError(f"Alamat AX.25 tidak valid: {address}")

    ssid = int(ssid_text) if separator else 0
    if ssid < 0 or ssid > 15:
        raise ValueError(f"SSID AX.25 harus antara 0 dan 15: {address}")

    padded_call = call.upper().ljust(6)
    encoded_call = bytes(ord(character) << 1 for character in padded_call)
    extension = 0x60 | (ssid << 1) | int(last)
    return encoded_call + bytes((extension,))


def encode_ax25_frame(source, destination, information):
    if len(information) > 256:
        raise ValueError("Panjang informasi APRS melebihi 256 byte.")

    addresses = (
        encode_ax25_address(destination, last=False)
        + encode_ax25_address(source, last=True)
    )
    return addresses + b"\x03\xf0" + information


def encode_kiss_frame(ax25_frame):
    frame = b"\x00" + ax25_frame
    escaped = bytearray(b"\xc0")
    for value in frame:
        if value == 0xC0:
            escaped.extend(b"\xdb\xdc")
        elif value == 0xDB:
            escaped.extend(b"\xdb\xdd")
        else:
            escaped.append(value)
    escaped.append(0xC0)
    return bytes(escaped)


class DirewolfKiss:
    def __init__(self, connection):
        self.connection = connection

    def send_aprs(self, information):
        payload = information.encode("ascii")
        packet = encode_kiss_frame(
            encode_ax25_frame(CALLSIGN, "APRS", payload)
        )
        with send_lock:
            self.connection.sendall(packet)
        log.info("[KISS] paket APRS masuk ke Direwolf: %s", information)

    def close(self):
        self.connection.close()


def connect_kiss():
    try:
        return socket.create_connection(
            (KISS_HOST, KISS_PORT),
            timeout=1
        ), None
    except OSError:
        pass

    if not DIREWOLF_EXE.is_file():
        raise FileNotFoundError(f"Direwolf tidak ditemukan: {DIREWOLF_EXE}")
    if not DIREWOLF_CONFIG.is_file():
        raise FileNotFoundError(
            f"Konfigurasi Direwolf tidak ditemukan: {DIREWOLF_CONFIG}"
        )

    log.info("[Direwolf] menjalankan %s", DIREWOLF_EXE)
    process = subprocess.Popen(
        [str(DIREWOLF_EXE), "-c", str(DIREWOLF_CONFIG)],
        cwd=str(DIREWOLF_DIR)
    )

    deadline = time.monotonic() + KISS_CONNECT_TIMEOUT
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    "Direwolf berhenti saat startup "
                    f"(exit code {process.returncode})."
                )
            try:
                connection = socket.create_connection(
                    (KISS_HOST, KISS_PORT),
                    timeout=1
                )
                connection.settimeout(None)
                log.info("[KISS] terhubung ke Direwolf di %s:%s", KISS_HOST, KISS_PORT)
                return connection, process
            except OSError:
                time.sleep(0.25)

        raise TimeoutError(
            f"Direwolf tidak membuka KISS TCP di {KISS_HOST}:{KISS_PORT} "
            f"dalam {KISS_CONNECT_TIMEOUT} detik."
        )
    except Exception:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        raise


def format_lat(lat):
    """Derajat desimal -> DDMM.mmN/S."""
    hemi = "N" if lat >= 0 else "S"
    total = round(abs(lat) * 60 * 100)
    return f"{total // 6000:02d}{(total % 6000) / 100:05.2f}{hemi}"


def format_lon(lon):
    """Derajat desimal -> DDDMM.mmE/W."""
    hemi = "E" if lon >= 0 else "W"
    total = round(abs(lon) * 60 * 100)
    return f"{total // 6000:03d}{(total % 6000) / 100:05.2f}{hemi}"


def kirim(isi):
    if tnc is None:
        raise RuntimeError("Koneksi KISS Direwolf belum tersedia.")
    tnc.send_aprs(isi)


def kirim_definisi():
    """Kirim nama kanal, satuan, dan rumus konversi telemetry APRS."""
    tujuan = f":{CALLSIGN:<9}:"
    kirim(tujuan + "PARM.Suhu")
    kirim(tujuan + "UNIT.degC")
    kirim(tujuan + "EQNS.0,0.5,0,0,1,0,0,1,0,0,1,0,0,1,0")
    kirim(tujuan + "BITS.00000000,Telemetry Suhu")


def kirim_posisi():
    """Kirim paket posisi dan status ke radio."""
    global terakhir_posisi
    if LAT is None or LON is None:
        return

    posisi = (
        f"={format_lat(LAT)}{SIMBOL_TABEL}{format_lon(LON)}"
        f"{SIMBOL_KODE}{KOMENTAR[:43]}"
    )
    kirim(posisi)
    if STATUS:
        kirim(">" + STATUS[:60])
    terakhir_posisi = time.time()


def ambil_suhu(payload):
    """Terima suhu sebagai angka, JSON, atau teks yang memuat angka."""
    teks = payload.decode(errors="replace").strip()
    try:
        return float(teks)
    except ValueError:
        pass

    try:
        data = json.loads(teks)
        if isinstance(data, (int, float)):
            return float(data)
        if isinstance(data, dict):
            for key in ("temperature", "temp", "suhu", "value"):
                if key in data:
                    return float(data[key])
        if isinstance(data, str):
            teks = data
        else:
            raise ValueError("JSON tidak punya kunci suhu: " + teks)
    except json.JSONDecodeError:
        pass

    cocok = re.search(r"-?\d+(?:[.,]\d+)?", teks)
    if cocok:
        return float(cocok.group().replace(",", "."))
    raise ValueError("format payload tidak dikenali: " + repr(teks))


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("[MQTT] terhubung, subscribe %s", MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC)
    else:
        log.error("[MQTT] koneksi ditolak: %s", reason_code)


def on_message(client, userdata, msg):
    global seq, terakhir_kirim, suhu_terakhir
    log.info("[MQTT] payload mentah: %s", msg.payload[:100])

    try:
        suhu = ambil_suhu(msg.payload)
    except ValueError as exc:
        log.error("[MQTT] payload tidak valid: %s", exc)
        return

    suhu_terakhir = suhu
    log.info("[MQTT] %s = %s", msg.topic, suhu)

    if time.time() - terakhir_kirim < KIRIM_TIAP:
        return

    raw = max(0, min(255, round(suhu * 2)))
    seq = (seq + 1) % 1000
    try:
        kirim(f"T#{seq:03d},{raw:03d},000,000,000,000,00000000")
        terakhir_kirim = time.time()
    except (OSError, RuntimeError, ValueError):
        log.exception("[KISS] gagal mengirim telemetry ke Direwolf.")


def stop_direwolf(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def main():
    global tnc
    connection, direwolf_process = connect_kiss()
    connection.settimeout(None)
    tnc = DirewolfKiss(connection)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        kirim_definisi()
        if LAT is None or LON is None:
            log.info("[APRS] LAT/LON belum diisi; paket posisi tidak dikirim.")
        else:
            kirim_posisi()

        log.info("[MQTT] menghubungkan ke %s:%s", MQTT_HOST, MQTT_PORT)
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()

        while True:
            time.sleep(5)
            if time.time() - terakhir_posisi >= POSISI_TIAP:
                kirim_posisi()
    except KeyboardInterrupt:
        log.info("Program dihentikan oleh pengguna.")
    finally:
        client.loop_stop()
        client.disconnect()
        tnc.close()
        tnc = None
        stop_direwolf(direwolf_process)
        log.info("Koneksi MQTT/KISS ditutup.")


if __name__ == "__main__":
    main()
