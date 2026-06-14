"""Sentiment analysis handler: evaluates review text and creates sentiment score"""

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

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        text = payload["processed_text"]

        sentiment = "neutral"
        if "good" in text: sentiment = "positive"
        elif "bad" in text: sentiment = "negative"

        table_name = ssm.get_parameter(Name="/tables/reviews")["Parameter"]["Value"]
        dynamodb.Table(table_name).update_item(
            Key={"review_id": payload["review_id"]},
            UpdateExpression="SET sentiment = :s",
            ExpressionAttributeValues={":s": sentiment}
        )
    return {"statusCode": 200}
