import os
import time
import csv
import json
import uuid
import logging
import paho.mqtt.client as mqtt
from dataclasses import dataclass, field
import const

from vt_shared import MasterRecord


MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_USERNAME = const.MQTT_USERNAME
MQTT_PASSWORD = const.MQTT_PASSWORD
MQTT_TOPIC = "master_record/insert"


INPUT_DIR = const.SAMPLE_FILES_DIR
INPUT_FILENAME = "client_a.txt"
INPUT_FILE_PATH = os.path.join(INPUT_DIR, INPUT_FILENAME)
CHECK_INTERVAL_SECONDS = 300  # 5 minutes


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def fetch_from_sftp():
    """
    Placeholder method to fetch file from an SFTP location.
    In a real implementation, this would use paramiko or pysftp to 
    download the file to INPUT_FILE_PATH.
    """
    sftp_host = os.getenv("SFTP_HOST", "sftp.example.com")
    sftp_user = os.getenv("SFTP_USER", "user")
    sftp_pass = os.getenv("SFTP_PASS", "pass")
    
    logging.info(f"Connecting to SFTP at {sftp_host}...")
    # Logic to download file would go here

    logging.info("SFTP download skipped for demonstration (reading local disk).")


def parse_currency(value_str):
    """Parses a currency string like '$20.00' into a float."""
    clean_val = value_str.replace('$', '').replace(',', '').strip()
    try:
        return float(clean_val)
    except ValueError:
        return 0.0

def process_file(mqtt_client):
    if not os.path.exists(INPUT_FILE_PATH):
        logging.info(f"File not found: {INPUT_FILE_PATH}")
        return

    logging.info(f"Processing file: {INPUT_FILE_PATH}")
    
    try:
        with open(INPUT_FILE_PATH, mode='r', newline='', encoding='utf-8') as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter='\t')
            
            for row in reader:
                try:
                    record = MasterRecord(
                        id=str(uuid.uuid4()),
                        first_name=row['name_first'],
                        last_name=row['name_last'],
                        ssn=row['ssn'],
                        hourly_rate=parse_currency(row['hourly_rate']),
                        attributes={"source": "client_b_collector"}
                    )
                    
                 
                    mqtt_client.publish(MQTT_TOPIC, record.to_json())
                    logging.info(f"Published record for: {record.first_name} {record.last_name}")
                    
                except KeyError as e:
                    logging.error(f"Missing column in row: {e}")
                except Exception as e:
                    logging.error(f"Error processing row: {e}")
                    
        
    except Exception as e:
        logging.error(f"Failed to read file: {e}")


def main():

    client = mqtt.Client(client_id="client_b_collector")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    try:
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        client.loop_start()
        logging.info("Connected to MQTT Broker")
    except Exception as e:
        logging.error(f"Could not connect to MQTT Broker: {e}")
        return

    logging.info("Starting collector service loop...")
    
    while True:

        fetch_from_sftp()
        

        process_file(client)
        

        logging.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()