import os
import json
from datetime import datetime
from dotenv import load_dotenv
import boto3
from botocore.client import Config

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

def bronze_path(data_type, station, dt_obj):
    date_str = dt_obj.strftime("%d-%m-%Y")
    ts_str = dt_obj.strftime("%Y-%m-%dT%H-%M-%S")
    return f"bronze/{station}/{data_type}/{date_str}/{ts_str}.json"

def upload_bronze_station(bucket, data_type, station, timestamp, json_data):
    r2 = get_r2_client()
    key = bronze_path(data_type, station, timestamp)
    r2.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_data.encode("utf-8") if isinstance(json_data, str) else json.dumps(json_data).encode("utf-8"),
    )
