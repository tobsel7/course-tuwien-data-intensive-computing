"""Sentiment analysis handler: evaluates review text and creates sentiment score"""

import json
import os
from urllib.parse import unquote_plus

import boto3

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL")
REVIEWS_TABLE = "reviews"

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT_URL)

POS = {'good', 'great', 'excellent', 'love', 'amazing', 'perfect'}
NEG = {'bad', 'terrible', 'hate', 'horrible', 'awful', 'sucks'}

def simple_sentiment(text):
    words = set(text.lower().split())
    score = sum(1 for w in words if w in POS) - sum(1 for w in words if w in NEG)
    if score > 0:
        return 'positive'
    if score < 0:
        return 'negative'
    return 'neutral'

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

        sentiment = simple_sentiment(payload.get("processed_text", ""))
        dynamodb.Table(REVIEWS_TABLE).update_item(
            Key={"review_id": review_id},
            UpdateExpression="SET sentiment = :s",
            ExpressionAttributeValues={":s": sentiment},
        )

    return {"statusCode": 200}


