import os

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 30002))

SAMPLE_FILES_DIR = os.getenv("SAMPLE_FILES_DIR", "./example_files")