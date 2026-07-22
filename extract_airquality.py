import json
import os
import urllib.request
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
secrets = boto3.client("secretsmanager")

BUCKET = os.environ.get("BUCKET_NAME", "saqcp-data-lake-demo2026")
TABLE_NAME = os.environ.get("STATE_TABLE", "saqcp-ingestion-state")
SECRET_NAME = os.environ.get("SECRET_NAME", "saqcp/openaq-api-key")

# Confirmed working OpenAQ location IDs (Kentucky, Albuquerque NM, Delhi)
LOCATION_IDS = [225, 2178, 8118]

def get_api_key():
    resp = secrets.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(resp["SecretString"])["api_key"]

def lambda_handler(event, context):
    api_key = get_api_key()
    table = dynamodb.Table(TABLE_NAME)

    all_results = []
    for loc_id in LOCATION_IDS:
        url = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
        req = urllib.request.Request(url, headers={"X-API-Key": api_key})
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                data = json.loads(response.read().decode())
                all_results.append({"location_id": loc_id, "data": data})
        except Exception as e:
            print(f"Failed to fetch location {loc_id}: {e}")

    if all_results:
        date_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"raw/air_quality/{date_partition}/air_quality_{context.aws_request_id}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(all_results), ContentType="application/json")

    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    table.put_item(Item={
        "source_type": "air_quality",
        "last_run_timestamp": end_time,
        "records_fetched": len(all_results),
    })

    return {"statusCode": 200, "records_fetched": len(all_results)}