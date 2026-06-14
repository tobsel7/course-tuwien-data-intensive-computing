"""Preprocessing handler: extracts relevant text and populates database."""
import json
import os
import re
from urllib.parse import unquote_plus

import boto3

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL")
PROCESSED_BUCKET = "processed-text"
REVIEWS_TABLE = "reviews"
USERS_TABLE = "users"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT_URL)

def simple_process(text):
    # lower, keep alnum and spaces, collapse whitespace
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def handler(event, context):
    records = (event or {}).get("Records", [])
    if not records:
        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    s3_record = records[0]["s3"]
    bucket = s3_record["bucket"]["name"]
    key = unquote_plus(s3_record["object"]["key"])

    review = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
    review_id = f"{review.get('reviewerID', 'unknown')}_{review.get('asin', '')}"
    user_id = review.get("reviewerID", "unknown")
    processed_text = simple_process(f"{review.get('summary') or ''} {review.get('reviewText') or ''}")
    processed_key = f"processed/{review_id}.json"

    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=processed_key,
        Body=json.dumps({
            "review_id": review_id,
            "user_id": user_id,
            "overall": review.get("overall"),
            "processed_text": processed_text,
        }),
    )

    dynamodb.Table(REVIEWS_TABLE).put_item(Item={
        "review_id": review_id,
        "user_id": user_id,
        "processed_text_key": processed_key,
        "sentiment": None,
    })
    dynamodb.Table(USERS_TABLE).update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET banned = if_not_exists(banned, :b)",
        ExpressionAttributeValues={":b": False},
    )

    return {"statusCode": 200, "body": json.dumps({"review_id": review_id})}


