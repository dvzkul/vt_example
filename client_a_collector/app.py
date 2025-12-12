import os
import time
import csv
import json
import uuid
import logging
import paho.mqtt.client as mqtt
import const
# Import the shared library
from vt_shared import MasterRecord, ParseError

# --- Configuration ---
MQTT_BROKER_HOST = const.MQTT_BROKER_HOST  
MQTT_BROKER_PORT = const.MQTT_BROKER_PORT
MQTT_USERNAME = const.MQTT_USERNAME
MQTT_PASSWORD = const.MQTT_PASSWORD
MQTT_TOPIC = "master_record/insert"
MQTT_ERROR_TOPIC = "master_record/errors"


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
    I don't know the details.  I assume that we would just check
    for new files and download them as needed.
    """




    sftp_host = os.getenv("SFTP_HOST", "sftp.client-a.com")

    #I would then store the flat file somewhere S3 bucket, etc.
    #Then go on to process the file

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
    
    #I would actually process the file from where the SFTP put it
    
    if not os.path.exists(INPUT_FILE_PATH):
        logging.info(f"File not found: {INPUT_FILE_PATH}")
        return

    logging.info(f"Processing file: {INPUT_FILE_PATH}")

    
    file_name = os.path.basename(INPUT_FILE_PATH)
    
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
                    msg = f"Missing column in row: {e}"
                    logging.error(msg)
                    handle_error(mqtt_client, msg, row, file_name)
                    
                except Exception as e:
                    msg = f"Error processing row: {e}"
                    logging.error(msg)
                    handle_error(mqtt_client, msg, row, file_name)
                    

        
    except Exception as e:
        logging.error(f"Failed to read file: {e}")

def handle_error(mqtt_client, error_message, row=None, file_name=None):
    error_obj = ParseError(
            error=str(error_message),
            source="client_a_collector", 
            file_name=str(file_name),
            row_data=row if row else {}
        )


    mqtt_client.publish(MQTT_ERROR_TOPIC, error_obj.to_json())
    logging.error(f"Published error to MQTT: {error_message}")

def main():
    client = mqtt.Client(client_id="client_a_collector")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
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