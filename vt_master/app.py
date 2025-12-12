import paho.mqtt.client as mqtt
import json
from pymongo import MongoClient
import const

from vt_shared import MasterRecord

MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_TOPIC = "master_record/insert"

MONGO_URI = const.MONGO_URI
MONGO_DB_NAME = const.MONGO_DB_NAME

def initialize_services():
   global mqtt_client, mongo_client, db, collection

   mqtt_client = mqtt.Client()
   mqtt_client.on_message = on_message
   mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
   mqtt_client.subscribe(MQTT_TOPIC)


   mongo_client = MongoClient(MONGO_URI)
   db = mongo_client[MONGO_DB_NAME]
   collection = db["master_records"]

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
