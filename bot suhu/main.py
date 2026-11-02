import json
import logging
import os
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from pymodbus.exceptions import ModbusException
from pymodbus.client import ModbusTcpClient

CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "modbus": {
        "host": "10.10.0.100",
        "port": 502,
        "device_id": 1,
        "register_address": 0,
        "register_count": 2,
        "timeout_seconds": 3,
        "scale": 10
    },
    "mqtt": {
        "host": "192.168.1.10",
        "port": 1883,
        "keepalive_seconds": 60,
        "topic": "suhu_ukdc",
        "qos": 1,
        "client_id": "modbus-mqtt-sensor"
    },
    "poll_interval_seconds": 5,
    "retry_interval_seconds": 5
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("modbus-mqtt")


def load_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as file:
            json.dump(DEFAULT_CONFIG, file, indent=2)

        log.warning("config.json dibuat menggunakan konfigurasi contoh.")

    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def make_mqtt_client(cfg):
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=cfg.get("client_id", "modbus-mqtt-sensor")
    )

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30
    )

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            log.info("Berhasil terhubung ke MQTT Server.")
        else:
            log.error("Koneksi MQTT ditolak: %s", reason_code)

    def on_disconnect(
        client, userdata, disconnect_flags, reason_code, properties
    ):
        log.warning(
            "Koneksi MQTT terputus. Program akan mencoba menyambung kembali."
        )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    return client


def read_registers(client, address, count, device_id):
    try:
        return client.read_holding_registers(
            address=address,
            count=count,
            device_id=device_id
        )
    except TypeError:
        # Kompatibilitas dengan versi pymodbus yang lebih lama.
        return client.read_holding_registers(
            address=address,
            count=count,
            slave=device_id
        )


def main():
    cfg = load_config()

    modbus_cfg = cfg["modbus"]
    mqtt_cfg = cfg["mqtt"]

    scale = float(modbus_cfg.get("scale", 10))
    poll_interval = max(
        1, int(cfg.get("poll_interval_seconds", 5))
    )
    retry_interval = max(
        1, int(cfg.get("retry_interval_seconds", 5))
    )

    # Membuat koneksi Modbus ke sensor laboratorium.
    sensor = ModbusTcpClient(
        host=modbus_cfg["host"],
        port=int(modbus_cfg["port"]),
        timeout=float(modbus_cfg.get("timeout_seconds", 3))
    )

    # Membuat koneksi MQTT.
    mqtt_client = make_mqtt_client(mqtt_cfg)

    try:
        # Koneksi asinkron agar kegagalan MQTT tidak langsung
        # menghentikan program pada tahap connect().
        log.info(
            "Menginisialisasi MQTT %s:%s",
            mqtt_cfg["host"],
            mqtt_cfg["port"]
        )

        mqtt_client.connect_async(
            mqtt_cfg["host"],
            int(mqtt_cfg["port"]),
            int(mqtt_cfg.get("keepalive_seconds", 60))
        )

        mqtt_client.loop_start()

        log.info(
            "Monitoring dimulai. Interval pembacaan: %s detik.",
            poll_interval
        )

        while True:
            try:
                # 1. Hubungkan ke sensor Modbus.
                if not sensor.connected:
                    log.info(
                        "Menghubungkan ke sensor Modbus %s:%s",
                        modbus_cfg["host"],
                        modbus_cfg["port"]
                    )

                    if not sensor.connect():
                        log.error(
                            "Sensor Modbus tidak dapat dijangkau. "
                            "Coba lagi dalam %s detik.",
                            retry_interval
                        )
                        time.sleep(retry_interval)
                        continue

                    log.info("Berhasil terhubung ke sensor Modbus.")

                # 2. Baca dua register sensor.
                result = read_registers(
                    sensor,
                    int(modbus_cfg["register_address"]),
                    int(modbus_cfg["register_count"]),
                    int(modbus_cfg["device_id"])
                )

                if result.isError():
                    log.error(
                        "Pembacaan register Modbus gagal: %s",
                        result
                    )
                    sensor.close()
                    time.sleep(retry_interval)
                    continue

                registers = result.registers

                if len(registers) < 2:
                    log.error(
                        "Data sensor tidak lengkap: "
                        "diperlukan dua register."
                    )
                    time.sleep(retry_interval)
                    continue

                # Pemetaan register WiFi Lab Hardware:
                # Register 0 = kelembapan, unit 0.1 %RH
                # Register 1 = suhu, unit 0.1 derajat Celsius

                kelembapan = registers[0] / scale
                suhu = registers[1] / scale

                # 3. Membentuk data JSON.
                data = {
                    "suhu": round(suhu, 1),
                    "kelembapan": round(kelembapan, 1),
                    "timestamp": datetime.now(
                        timezone.utc
                    ).isoformat()
                }

                payload = json.dumps(
                    data,
                    ensure_ascii=False
                )

                log.info(
                    "Sensor: suhu=%s °C, kelembapan=%s %%RH",
                    data["suhu"],
                    data["kelembapan"]
                )

                # 4. Kirim data jika MQTT sudah terhubung.
                if mqtt_client.is_connected():
                    info = mqtt_client.publish(
                        mqtt_cfg["topic"],
                        payload=payload,
                        qos=int(mqtt_cfg.get("qos", 1))
                    )

                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        log.info(
                            "Data dikirim ke MQTT. Topic=%s",
                            mqtt_cfg["topic"]
                        )
                    else:
                        log.warning(
                            "Pengiriman MQTT gagal. Kode=%s",
                            info.rc
                        )
                else:
                    log.warning(
                        "MQTT belum terhubung. Data kali ini "
                        "tidak dikirim; program akan mencoba lagi."
                    )

            except (OSError, TimeoutError, ConnectionError, ModbusException) as exc:
                log.error(
                    "Kesalahan komunikasi: %s",
                    exc
                )
                sensor.close()

            except Exception:
                log.exception(
                    "Terjadi kesalahan saat membaca atau mengirim data."
                )

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("Program dihentikan oleh pengguna.")

    finally:
        sensor.close()

        mqtt_client.loop_stop()
        mqtt_client.disconnect()

        log.info("Koneksi ditutup.")


if __name__ == "__main__":
    main()