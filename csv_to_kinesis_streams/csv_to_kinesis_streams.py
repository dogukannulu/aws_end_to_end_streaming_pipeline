import boto3
import csv
import time
import logging
import argparse
import requests
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
    parser = argparse.ArgumentParser(description="Send CSV data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=int, required=True, help="Time interval (in seconds) between two writes")
    parser.add_argument("--max_rows", "-mr", type=int, default=150, help="Maximum number of rows to write (max: 150)")
    parser.add_argument("--csv_url", "-url", required=True, help="URL of the CSV file")
    args = parser.parse_args()

    return args


def send_csv_to_kinesis(stream_name, interval, max_rows, csv_url):
    kinesis_streamer = KinesisStreamer()

    response = requests.get(csv_url)
    response.raise_for_status()
    csv_data = response.text

    csv_reader = csv.reader(csv_data.splitlines())
    next(csv_reader)  # Skip the header row

    rows_written = 0
    try:
        for idx, row in enumerate(csv_reader, start=1):
            data = ','.join(row)
            encoded_data = f"{data}\n".encode('utf-8')  # Encode the data as bytes

            sequence_number = kinesis_streamer.send_record(stream_name, encoded_data)
            logger.info(f"Record sent: {sequence_number} - Row {idx}: {', '.join(row)}")

            time.sleep(interval)

            rows_written += 1
            if rows_written >= max_rows or max_rows > len(csv_data.splitlines()) - 1:
                break
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt signal. Stopping the CSV-to-Kinesis streaming process.")
        sys.exit(0)  # Gracefully exit the script


if __name__ == "__main__":
    args = define_arguments()

    logger.info(f"Processing CSV file from URL: {args.csv_url}")
    send_csv_to_kinesis(args.stream_name, args.interval, args.max_rows, args.csv_url)
