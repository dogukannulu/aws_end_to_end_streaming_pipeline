import boto3
import time
import logging
import argparse
import requests
import json
import sys

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KinesisStreamer:
    def __init__(self, region_name='eu-central-1'):
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)

    def send_record(self, stream_name, data, partition_key="No"):
        try:
            response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=partition_key
            )
            return response['SequenceNumber']
        except self.kinesis_client.exceptions.ResourceNotFoundException:
            logger.error(f"Kinesis stream '{stream_name}' not found. Please ensure the stream exists.")
            sys.exit(1)

def define_arguments():
    """
    Defines the command-line arguments 
    """
    parser = argparse.ArgumentParser(description="Send JSON data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=int, required=True, help="Time interval (in seconds) between two writes")
    parser.add_argument("--max_records", "-mr", type=int, default=150, help="Maximum number of records to write")
    parser.add_argument("--json_url", "-url", required=True, help="URL of the JSON data")
    args = parser.parse_args()

    return args

def send_json_to_kinesis(stream_name, interval, max_records, json_url):
    kinesis_streamer = KinesisStreamer()

    response = requests.get(json_url)
    response.raise_for_status()
    json_data = response.json()

    records_sent = 0
    try:
        for idx, record in enumerate(json_data, start=1):
            encoded_data = json.dumps(record).encode('utf-8')

            sequence_number = kinesis_streamer.send_record(stream_name, encoded_data)
            logger.info(f"Record sent: {sequence_number} - Record {idx}: {json.dumps(record)}")

            time.sleep(interval)

            records_sent += 1
            if records_sent >= max_records or max_records > len(json_data):
                break
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt signal. Stopping the JSON-to-Kinesis streaming process.")
        sys.exit(0)  # Gracefully exit the script

if __name__ == "__main__":
    args = define_arguments()

    logger.info(f"Processing JSON data from URL: {args.json_url}")
    send_json_to_kinesis(args.stream_name, args.interval, args.max_records, args.json_url)
