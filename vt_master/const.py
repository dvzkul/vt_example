import os

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 30002))


MONGO_HOST = os.getenv("MONGO_HOST", "mongo_db")
MONGO_PORT = int(os.getenv("MONGO_PORT", 30001))
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "vt_example_db")
MONGO_USER = os.getenv("MONGO_USER", "vt_example_db")
MONGO_PASS = os.getenv("MONGO_PASS", "vt_example_db_password")
MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB_NAME}?authSource=admin")