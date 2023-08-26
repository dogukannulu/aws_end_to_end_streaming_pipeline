import io
import re
import json
import pandas as pd
import boto3
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GlobalVariables:
    target_s3_bucket = "lambda-function-parquet-dogukan-ulu"
    target_s3_key = "dirty_books_parquet/books.parquet"


def load_df_from_s3(bucket_name, key):
    """
    Read a JSON from a S3 bucket & load into pandas dataframe
    """
    s3 = boto3.client('s3')
    logger.info("Starting S3 object retrieval process...")
    try:
        get_response = s3.get_object(Bucket=bucket_name, Key=key)
        logger.info("Object retrieved from S3 bucket successfully")
    except ClientError as e:
        logger.error(f"S3 object cannot be retrieved: {e}")
    
    json_data = get_response['Body'].read().decode('utf-8')
    
    data = json.loads(json_data)
    df = pd.DataFrame(data)

    return df


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f"S3 bucket is obtained from the event: {bucket}")
    logger.info(f"Object key is obtained from the event: {key}")

    df = load_df_from_s3(bucket_name=bucket, key=key)
    
    # Convert DataFrame to Parquet format
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    
    target_s3_bucket = GlobalVariables.target_s3_bucket
    target_s3_key = GlobalVariables.target_s3_key
    
    s3_client.upload_fileobj(parquet_buffer, target_s3_bucket, target_s3_key)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Parquet conversion and upload successful')
    }