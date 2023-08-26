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


class S3Uploader:
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.target_s3_bucket = "aws-glue-dirty-books-parquet-dogukan-ulu"
        self.target_s3_key = "books_parquet/books.parquet"

    def upload(self, buffer):
        """
        Upload Parquet data from memory buffer to S3
        """
        try:
            self.s3.put_object(Body=buffer.getvalue(), Bucket=self.target_s3_bucket, Key=self.target_s3_key)
            print("Parquet file uploaded into S3 successfully")
        except Exception as e:
            print(f"Error occured while uploading parquet into S3: {e}")
        

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"S3 bucket is obtained from the event: {bucket}")
        print(f"Object key is obtained from the event: {key}")

        s3_loader = S3Loader()
        df = s3_loader.load_df_from_s3(bucket_name=bucket, key=key)
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)

        print("JSON has been converted into parquet")
        
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
