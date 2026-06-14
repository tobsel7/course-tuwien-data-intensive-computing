"""Profanity check handler: detects profane words and creates profanity violations"""
import json
import os
from urllib.parse import unquote_plus

import boto3

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL")
PROFANITY_TABLE = "profanity"
REVIEWS_TABLE = "reviews"
VIOLATIONS_BUCKET = "profanity-violations"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT_URL)

BAD = {'damn', 'crap', 'sucks', 'hate', 'shit', 'fuck'}

def handler(event, context):
    for record in (event or {}).get("Records", []):
        s3_record = record.get("s3", {})
        bucket = s3_record.get("bucket", {}).get("name")
        key = unquote_plus(s3_record.get("object", {}).get("key", ""))
        if not bucket or not key:
            continue

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        review_id = payload.get("review_id")
        if not review_id:
            continue

        review = dynamodb.Table(REVIEWS_TABLE).get_item(
            Key={"review_id": review_id},
            ConsistentRead=True,
        ).get("Item")
        if not review:
            continue

        found = [word for word in set(payload.get("processed_text", "").split()) if word in BAD]
        if not found:
            continue

        violation = {
            "violation_id": review_id,
            "review_id": review_id,
            "user_id": review.get("user_id", payload.get("user_id", "")),
            "words": found,
        }
        dynamodb.Table(PROFANITY_TABLE).put_item(Item=violation)
        s3.put_object(Bucket=VIOLATIONS_BUCKET, Key=f"violations/{review_id}.json", Body=json.dumps(violation))

    return {"statusCode": 200}



