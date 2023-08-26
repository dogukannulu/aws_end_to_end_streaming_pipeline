import io
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError


class S3Loader:
    def __init__(self):
        self.s3 = boto3.client('s3')


    def load_df_from_s3(self, bucket_name, key):
        """
        Read JSON from an S3 bucket & load into a pandas dataframe
        """
        print("Starting S3 object retrieval process...")
        try:
            get_response = self.s3.get_object(Bucket=bucket_name, Key=key)
            print("Object retrieved from S3 bucket successfully")
        except ClientError as e:
            print(f"S3 object cannot be retrieved: {e}")
            return None
        
        json_data = get_response['Body'].read().decode('utf-8')
        
        json_data_separated = json_data.replace('}{', '},{')
        json_data_final = f"[{json_data_separated}]"
        json_objects = json.loads(json_data_final)

        df = pd.DataFrame(json_objects)

        return df


class ParquetConverter:
    def to_parquet(self, df):
        """
        Convert DataFrame to Parquet format stored in memory buffer
        """
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        return parquet_buffer


class S3Uploader:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.target_s3_bucket = "aws-glue-dirty-books-parquet-dogukan-ulu"
        self.target_s3_key = "books_parquet/"

    def upload(self, buffer):
        """
        Upload Parquet data from memory buffer to S3
        """
        self.s3.upload_fileobj(buffer, self.target_s3_bucket, self.target_s3_key)
        

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"S3 bucket is obtained from the event: {bucket}")
        print(f"Object key is obtained from the event: {key}")

        s3_loader = S3Loader()
        df = s3_loader.load_df_from_s3(bucket_name=bucket, key=key)
        
        if df is None:
            return {
                'statusCode': 500,
                'body': json.dumps('Error loading data from S3')
            }
        
        parquet_converter = ParquetConverter()
        parquet_buffer = parquet_converter.to_parquet(df)
        
        s3_uploader = S3Uploader()
        s3_uploader.upload(parquet_buffer)

        return {
            'statusCode': 200,
            'body': json.dumps('Parquet conversion and upload successful')
        }
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred during processing')
        }
