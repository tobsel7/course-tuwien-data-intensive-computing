"""Profanity check handler: detects profane words and creates profanity violations"""
import json
import os
import boto3
from urllib.parse import unquote_plus

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)

PROFANE_WORD = "damn"

def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        if PROFANE_WORD in payload["processed_text"]:
            violation = {"violation_id": payload["review_id"], "user_id": payload["user_id"]}

            table_name = ssm.get_parameter(Name="/tables/profanity")["Parameter"]["Value"]
            dynamodb.Table(table_name).put_item(Item=violation)

            viol_bucket = ssm.get_parameter(Name="/buckets/violations")["Parameter"]["Value"]
            s3.put_object(Bucket=viol_bucket, Key=f"violations/{payload['review_id']}.json", Body=json.dumps(violation))
    return {"statusCode": 200}
