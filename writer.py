import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config

# Načíta .env hodnoty do prostredia
load_dotenv()

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_KEY_ID = os.getenv("R2_KEY_ID")
R2_SECRET = os.getenv("R2_SECRET")

def get_r2_client():
    return boto3.client(
        "s3",
        aws_access_key_id=R2_KEY_ID,
        aws_secret_access_key=R2_SECRET,
        endpoint_url=R2_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )

def upload_to_r2(bucket, filename, data):
    """
    bucket (str): názov bucketu
    filename (str): cesta + názov súboru v buckete (napr. 'songs/xy.json')
    data (str|bytes): obsah súboru, ktorý zapisuješ (JSON string, binárne dáta atď.)
    """
    client = get_r2_client()
    # Ak je data string, prekonvertuj do bytes
    if isinstance(data, str):
        data = data.encode("utf-8")
    client.put_object(Bucket=bucket, Key=filename, Body=data)
