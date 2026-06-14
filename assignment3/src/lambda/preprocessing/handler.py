"""
This handler reads raw review files (one or many reviews).
For each review, it:
- stores the extracted text in a processed S3 bucket
- populates the reviews DynamoDB table
- populates the users DynamoDB table
"""

import json
import os
import boto3
from urllib.parse import unquote_plus

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)


def parse_reviews_payload(raw_text):
    """Parse one review, a JSON array, or JSONL content into a list of reviews."""
    raw_text = raw_text.strip()
    if not raw_text:
        return []

    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    reviews = []
    for line in raw_text.splitlines():
        line = line.strip()
        if line:
            reviews.append(json.loads(line))
    return reviews


def handler(event, context):
    processed_bucket = ssm.get_parameter(Name="/buckets/processed")["Parameter"]["Value"]
    reviews_table_name = ssm.get_parameter(Name="/tables/reviews")["Parameter"]["Value"]
    users_table_name = ssm.get_parameter(Name="/tables/users")["Parameter"]["Value"]

    reviews_table = dynamodb.Table(reviews_table_name)
    users_table = dynamodb.Table(users_table_name)

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        raw_payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        reviews = parse_reviews_payload(raw_payload)

        for review in reviews:
            user_id = review["reviewerID"]
            review_id = f"{user_id}_{review['asin']}"
            processed_text = f"{review.get('summary', '')} {review.get('reviewText', '')}".lower().strip()

            s3.put_object(
                Bucket=processed_bucket,
                Key=f"processed/{review_id}.json",
                Body=json.dumps({"review_id": review_id, "user_id": user_id, "processed_text": processed_text})
            )

            reviews_table.put_item(Item={
                "review_id": review_id,
                "user_id": user_id,
                "sentiment": None
            })

            # Keep existing ban status; only initialize user records if missing.
            users_table.update_item(
                Key={"user_id": user_id},
                UpdateExpression="SET banned = if_not_exists(banned, :default)",
                ExpressionAttributeValues={":default": False}
            )
    return {"statusCode": 200}
