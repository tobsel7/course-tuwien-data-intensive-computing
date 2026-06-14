"""Preprocessing handler: extracts relevant text and populates database."""
import json
import os
import boto3
from urllib.parse import unquote_plus

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)

def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        review = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        review_id = f"{review['reviewerID']}_{review['asin']}"
        processed_text = f"{review.get('summary', '')} {review.get('reviewText', '')}".lower()

        processed_bucket = ssm.get_parameter(Name="/buckets/processed")["Parameter"]["Value"]
        s3.put_object(
            Bucket=processed_bucket,
            Key=f"processed/{review_id}.json",
            Body=json.dumps({"review_id": review_id, "user_id": review['reviewerID'], "processed_text": processed_text})
        )

        table_name = ssm.get_parameter(Name="/tables/reviews")["Parameter"]["Value"]
        dynamodb.Table(table_name).put_item(Item={
            "review_id": review_id,
            "user_id": review['reviewerID'],
            "sentiment": None
        })
    return {"statusCode": 200}
