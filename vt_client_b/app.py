import os
import time
import csv
import json
import uuid
import logging
import paho.mqtt.client as mqtt
from dataclasses import dataclass, field

# --- Configuration ---
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC = "master_record/insert"

# Input configuration
INPUT_DIR = os.getenv("INPUT_DIR", "/app/data")
INPUT_FILENAME = "client_a.txt"
INPUT_FILE_PATH = os.path.join(INPUT_DIR, INPUT_FILENAME)
CHECK_INTERVAL_SECONDS = 300  # 5 minutes

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Data Class Definition ---
# Re-defined here to ensure independence, or this could be moved to a shared library.
@dataclass
class MasterRecord:
    id: str
    first_name: str
    last_name: str
    ssn: str
    hourly_rate: float
    attributes: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "ssn": self.ssn,
            "hourly_rate": self.hourly_rate,
            "attributes": self.attributes
        })

# --- SFTP Service (Placeholder) ---
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
    # sftp.get(remote_path, INPUT_FILE_PATH)
    logging.info("SFTP download skipped for demonstration (reading local disk).")

# --- Processing Logic ---
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
                # Map TSV fields to MasterRecord fields
                # Input: name_first, name_last, ssn, hourly_rate
                try:
                    record = MasterRecord(
                        id=str(uuid.uuid4()), # Generate a unique ID
                        first_name=row['name_first'],
                        last_name=row['name_last'],
                        ssn=row['ssn'],
                        hourly_rate=parse_currency(row['hourly_rate']),
                        attributes={"source": "tsv_collector"}
                    )
                    
                    # Publish to MQTT
                    mqtt_client.publish(MQTT_TOPIC, record.to_json())
                    logging.info(f"Published record for: {record.first_name} {record.last_name}")
                    
                except KeyError as e:
                    logging.error(f"Missing column in row: {e}")
                except Exception as e:
                    logging.error(f"Error processing row: {e}")
                    
        # Optional: Rename or move file after processing to prevent re-reading
        # os.rename(INPUT_FILE_PATH, INPUT_FILE_PATH + ".processed")
        
    except Exception as e:
        logging.error(f"Failed to read file: {e}")

# --- Main Service Loop ---
def main():
    # Initialize MQTT Client
    client = mqtt.Client()
    
    try:
        client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
        client.loop_start()
        logging.info("Connected to MQTT Broker")
    except Exception as e:
        logging.error(f"Could not connect to MQTT Broker: {e}")
        return

    logging.info("Starting collector service loop...")
    
    while True:
        # 1. Attempt to fetch (Mocked)
        fetch_from_sftp()
        
        # 2. Process the file if it exists on disk
        process_file(client)
        
        # 3. Wait for next interval
        logging.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()