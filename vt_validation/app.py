import paho.mqtt.client as mqtt
import json
from pymongo import MongoClient
import const

from vt_shared import ValidationError

MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_USERNAME = const.MQTT_USERNAME
MQTT_PASSWORD = const.MQTT_PASSWORD
MQTT_TOPIC = "master_record/validate"

MONGO_URI = const.MONGO_URI
MONGO_DB_NAME = const.MONGO_DB_NAME

def initialize_services():
   global mqtt_client, mongo_client, db, collection

   mqtt_client = mqtt.Client(client_id="vt_validation_app")
   mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
   mqtt_client.on_message = on_message
   mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
   mqtt_client.subscribe(MQTT_TOPIC)


   mongo_client = MongoClient(MONGO_URI)
   db = mongo_client[MONGO_DB_NAME]
   collection = db["master_records_validation_errors"]

def on_message(client, userdata, msg):
    

   try:
       payload = msg.payload.decode('utf-8')
       record = json.loads(payload)
       collection.insert_one(record)
       print(f"Inserted record into MongoDB: {record}")
   except Exception as e:
       print(f"Error processing message: {e}")





initialize_services()
mqtt_client.loop_start()