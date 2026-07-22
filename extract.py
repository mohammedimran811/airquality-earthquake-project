import json
import os
import urllib.request
import boto3
from datetime import datetime, timedelta, timezone

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

BUCKET = os.environ.get("BUCKET_NAME", "saqcp-data-lake-demo2026")
TABLE_NAME = os.environ.get("STATE_TABLE", "saqcp-ingestion-state")
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    resp = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("source_type").eq("earthquake"),
        ScanIndexForward=False,
        Limit=1,
    )
    if resp["Items"]:
        start_time = resp["Items"][0]["last_run_timestamp"]
    else:
        start_time = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")

    end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": "2.5",
    }
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{USGS_URL}?{query_string}"

    with urllib.request.urlopen(url, timeout=20) as response:
        data = json.loads(response.read().decode())

    features = data.get("features", [])

    if features:
        date_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        key = f"raw/earthquakes/{date_partition}/earthquakes_{context.aws_request_id}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data), ContentType="application/json")

    table.put_item(Item={
        "source_type": "earthquake",
        "last_run_timestamp": end_time,
        "records_fetched": len(features),
    })

    return {"statusCode": 200, "records_fetched": len(features), "s3_key": key if features else None}