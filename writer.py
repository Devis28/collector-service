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
R2_BUCKET = os.getenv("R2_BUCKET")

session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_KEY_ID,
    aws_secret_access_key=R2_SECRET,
    config=Config(signature_version="s3v4"),
)

def save_data_to_r2(data, prefix):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"{prefix}/{timestamp}.json"
    try:
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=filename,
            Body=json.dumps(data, ensure_ascii=False),
            ContentType="application/json"
        )
        print(f"Saved to R2: {filename}")
    except Exception as e:
        print(f"Error uploading to R2: {e}")
