import boto3
import os
import logging
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_r2_access():
    print("--- Starting R2 Connection Debug ---")
    
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    bucket_name = os.environ.get("R2_BUCKET_NAME", "test-audio")

    print(f"Account ID: {account_id}")
    print(f"Bucket Name: {bucket_name}")
    
    if access_key:
        print(f"Access Key ID: {access_key[:4]}...{access_key[-4:]}")
    else:
        print("ERROR: R2_ACCESS_KEY_ID is missing!")
        return

    if secret_key:
        print("Secret Key: [PRESENT]")
    else:
        print("ERROR: R2_SECRET_ACCESS_KEY is missing!")
        return

    print("\n--- Initializing Client ---")
    try:
        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )
        print("Client initialized.")
    except Exception as e:
        print(f"Error initializing client: {e}")
        return

    print("\n--- Attempting List Buckets ---")
    try:
        response = s3_client.list_buckets()
        print("Success! Buckets found:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")
    except ClientError as e:
        print(f"Error listing buckets: {e}")
        # Continue to try upload anyway, in case List is denied but Put is allowed (unlikely but possible)

    print(f"\n--- Attempting Put Object to {bucket_name} ---")
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key='debug_test_file.txt',
            Body=b'This is a test file to verify R2 write permissions.',
            ContentType='text/plain'
        )
        print("Success! Object uploaded.")
    except ClientError as e:
        print(f"Error uploading object: {e}")

if __name__ == "__main__":
    test_r2_access()
