"""Violation handler: bans users after a few profanity violations."""

import json
import os
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Attr

VIOLATION_THRESHOLD = 3

DYNAMODB_ENDPOINT_URL = os.environ.get("DYNAMODB_ENDPOINT_URL")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
PROFANITY_TABLE = "profanity"
USERS_TABLE = "users"

dynamodb = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT_URL)
s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def handler(event, context):
	for record in (event or {}).get("Records", []):
		s3_record = record.get("s3", {})
		bucket = s3_record.get("bucket", {}).get("name")
		key = unquote_plus(s3_record.get("object", {}).get("key", ""))
		if not bucket or not key:
			continue

		payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
		user_id = payload.get("user_id")
		if not user_id:
			continue

		count = dynamodb.Table(PROFANITY_TABLE).scan(
			FilterExpression=Attr("user_id").eq(user_id),
			ConsistentRead=True,
		).get("Count", 0)
		if count > VIOLATION_THRESHOLD:
			dynamodb.Table(USERS_TABLE).update_item(
				Key={"user_id": user_id},
				UpdateExpression="SET banned = :b",
				ExpressionAttributeValues={":b": True},
			)

	return {"statusCode": 200}

