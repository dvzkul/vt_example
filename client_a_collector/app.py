import os
import time
import csv
import json
import uuid
import logging
import paho.mqtt.client as mqtt
import const
# Import the shared library
from vt_shared import MasterRecord

# --- Configuration ---
MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_TOPIC = "master_record/insert"


INPUT_DIR = os.getenv("INPUT_DIR", "/app/data")
INPUT_FILENAME = "client_a.csv"
INPUT_FILE_PATH = os.path.join(INPUT_DIR, INPUT_FILENAME)
CHECK_INTERVAL_SECONDS = 300  # 5 minutes


WORK_HOURS_PER_YEAR = 2080

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_from_sftp():
    """
    Placeholder method to fetch file from an SFTP location.
    """
    sftp_host = os.getenv("SFTP_HOST", "sftp.client-a.com")
    logging.info(f"Checking SFTP at {sftp_host} for {INPUT_FILENAME}...")
    # sftp.get(remote_path, INPUT_FILE_PATH)
    logging.info("SFTP download skipped (reading local disk).")

# --- Processing Logic ---
def parse_salary_to_hourly(value_str):
    """
    Parses a salary string like "20,000" into an hourly float.
    Assumes 2080 working hours/year.
    """
    if not value_str:
        return 0.0
    
    # Remove currency symbols and commas
    clean_val = value_str.replace('$', '').replace(',', '').strip()
    try:
        annual_salary = float(clean_val)
        return round(annual_salary / WORK_HOURS_PER_YEAR, 2)
    except ValueError:
        logging.warning(f"Could not parse salary: {value_str}")
        return 0.0

def process_file(mqtt_client):
    if not os.path.exists(INPUT_FILE_PATH):
        logging.info(f"File not found: {INPUT_FILE_PATH}")
        return

    logging.info(f"Processing file: {INPUT_FILE_PATH}")
    
    try:
        with open(INPUT_FILE_PATH, mode='r', newline='', encoding='utf-8') as csv_file:

            reader = csv.DictReader(csv_file)
            
            for row in reader:
                try:
                    # Logic specific to Client A (CSV)
                    hourly = parse_salary_to_hourly(row.get('base_salary'))
                    
                    record = MasterRecord(
                        id=str(uuid.uuid4()),
                        first_name=row['first_name'],
                        last_name=row['last_name'],
                        ssn=row['ssn'],
                        hourly_rate=hourly,
                        attributes={
                            "source": "client_a_csv",
                            "original_salary": row.get('base_salary')
                        }
                    )
                    
      
                    mqtt_client.publish(MQTT_TOPIC, record.to_json())
                    logging.info(f"Published record for: {record.first_name} {record.last_name} (${record.hourly_rate}/hr)")
                    
                except KeyError as e:
                    logging.error(f"Missing column in row: {e}")
                except Exception as e:
                    logging.error(f"Error processing row: {e}")
                    

        
    except Exception as e:
        logging.error(f"Failed to read file: {e}")


def main():
    client = mqtt.Client()
    
    try:
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        client.loop_start()
        logging.info("Connected to MQTT Broker")
    except Exception as e:
        logging.error(f"Could not connect to MQTT Broker: {e}")
        return

    logging.info("Starting Client A collector service...")
    
    while True:
        fetch_from_sftp()
        process_file(client)
        
        logging.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()