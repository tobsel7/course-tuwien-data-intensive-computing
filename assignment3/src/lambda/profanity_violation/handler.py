"""Violation handler: bans users after a few profanity violations."""

import json
import os
import boto3
from urllib.parse import unquote_plus
from boto3.dynamodb.conditions import Attr

endpoint = os.environ.get("ENDPOINT")
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)
s3 = boto3.client("s3", endpoint_url=endpoint)

def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        user_id = payload["user_id"]

        prof_table = ssm.get_parameter(Name="/tables/profanity")["Parameter"]["Value"]
        count = dynamodb.Table(prof_table).scan(FilterExpression=Attr("user_id").eq(user_id))["Count"]

        if count >= 3:
            user_table = ssm.get_parameter(Name="/tables/users")["Parameter"]["Value"]
            dynamodb.Table(user_table).put_item(Item={"user_id": user_id, "banned": True})
    return {"statusCode": 200}
