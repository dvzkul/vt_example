import os

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 30002))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "vt_example_mqtt_user")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "vt_example_mqtt_password")

SAMPLE_FILES_DIR = os.getenv("SAMPLE_FILES_DIR", "./example_files")