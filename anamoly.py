import json
import os
import time
import boto3

athena = boto3.client("athena")
sns = boto3.client("sns")

DATABASE = "saqcp_curated_db"
OUTPUT_LOCATION = "s3://saqcp-data-lake-demo2026/athena-query-results/"
TOPIC_ARN = os.environ.get("TOPIC_ARN", "arn:aws:sns:us-east-1:548827541528:saqcp-anomaly-alerts")

QUERY = """
SELECT place, magnitude, aqi_value, distance_km, event_time
FROM environmental_events
WHERE magnitude >= 4.5 AND aqi_value >= 100 AND distance_km <= 50
ORDER BY event_time DESC
LIMIT 10
"""

def run_athena_query(query):
    exec_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )["QueryExecutionId"]

    for _ in range(20):
        status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if status != "SUCCEEDED":
        raise Exception(f"Athena query did not succeed: {status}")

    results = athena.get_query_results(QueryExecutionId=exec_id)
    rows = results["ResultSet"]["Rows"][1:]  # skip header
    return [[c.get("VarCharValue", "") for c in row["Data"]] for row in rows]

def lambda_handler(event, context):
    anomalies = run_athena_query(QUERY)

    if anomalies:
        message_lines = ["Environmental anomalies detected (M4.5+ quake within 50km of an aqi_value >= 100 reading):", ""]
        for row in anomalies:
            place, magnitude, aqi, dist, event_time = row
            message_lines.append(
                f"- {place}: M{magnitude} at {event_time}, reading={aqi} at {dist}km away"
            )
        message = "\n".join(message_lines)

        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject="SAQCP: Environmental anomaly detected",
            Message=message,
        )
        print(f"Published {len(anomalies)} anomalies to SNS")
    else:
        print("No anomalies detected this run")

    return {"anomalies_found": len(anomalies)}