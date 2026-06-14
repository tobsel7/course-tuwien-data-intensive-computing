"""
This handler performs a profanity check on the preprocessed text.
If profanity is detected, it stores the violation in the violations S3 bucket
"""

import json
import os
import boto3
from urllib.parse import unquote_plus

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)

def contains_profanity(text):
    # TODO: do a real profanity check and not only look for the one word match
    return "damn" in text

def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        if contains_profanity(payload["processed_text"]):
            violation = {"violation_id": payload["review_id"], "user_id": payload["user_id"]}

            viol_bucket = ssm.get_parameter(Name="/buckets/violations")["Parameter"]["Value"]
            s3.put_object(Bucket=viol_bucket, Key=f"violations/{payload['review_id']}.json", Body=json.dumps(violation))
    return {"statusCode": 200}
