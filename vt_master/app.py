import paho.mqtt.client as mqtt
import json
from pymongo import MongoClient
import const
import time


from vt_shared import MasterRecord, ValidationError

MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_USERNAME = const.MQTT_USERNAME
MQTT_PASSWORD = const.MQTT_PASSWORD

MQTT_TOPIC = "master_record/insert"
MQTT_VALIDATION_TOPIC = "master_record/validate"

MONGO_URI = const.MONGO_URI
MONGO_DB_NAME = const.MONGO_DB_NAME

def initialize_services():
   global mqtt_client, mongo_client, db, collection

   mqtt_client = mqtt.Client(client_id="vt_master_app")
   mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
   mqtt_client.on_message = on_message
   mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
   mqtt_client.subscribe(MQTT_TOPIC)

   mongo_client = MongoClient(MONGO_URI)
   db = mongo_client[MONGO_DB_NAME]
   collection = db["master_records"]

def validate_record(record: MasterRecord) -> list:
   errors = []
   if not record.first_name:
       errors.append("Missing first_name")
   if not record.last_name:
       errors.append("Missing last_name")
   if not record.ssn:
       errors.append("Missing ssn")
   if record.hourly_rate is None:
       errors.append("Missing hourly_rate")
   return errors

def on_message(client, userdata, msg):
   try:
       payload = msg.payload.decode('utf-8')
       
       try:
           record_obj = MasterRecord.from_json(payload)
       except Exception as e:
           print(f"Deserialization Error: {e}")
           return

       # Validate
       validation_errors = validate_record(record_obj)

       if validation_errors:
           print(f"Validation Failed for {record_obj.id}")
           
           # Convert MasterRecord -> ValidationError

           error_obj = ValidationError(
               **vars(record_obj), 
               errors=validation_errors
           )
           
           # Publish to validation topic
           client.publish(MQTT_VALIDATION_TOPIC, error_obj.to_json())
           
       else:
           # Valid Record -> Insert
           record_dict = json.loads(record_obj.to_json())
           collection.insert_one(record_dict)
           print(f"Inserted record into MongoDB: {record_obj.id}")

   except Exception as e:
       print(f"Error processing message: {e}")

initialize_services()
mqtt_client.loop_start()

while True:
    time.sleep(1)