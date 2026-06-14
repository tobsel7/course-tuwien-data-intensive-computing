"""Violation handler: bans users after a number of profanity violations.

This Lambda is triggered by an S3 notification on the violations bucket.
It reads the violation JSON, counts violations per user (simple scan, OK
for small test data) and sets `banned = true` on the corresponding `users`
table item when the threshold is exceeded.
"""

import json
import os
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Attr

VIOLATION_THRESHOLD = 3

# Respect endpoint overrides when running against MiniStack
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))
s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))

def get_param(name):
	try:
		return ssm.get_parameter(Name=name)['Parameter']['Value']
	except Exception:
		# fallback to environment variable or empty
		return os.environ.get(name.strip('/').upper(), '')

def handler(event, context):
	"""Process violation bucket records and ban users when needed."""
	profanity_table = dynamodb.Table(get_param('/tables/profanity') or 'profanity')
	users_table = dynamodb.Table(get_param('/tables/users') or 'users')

	for rec in event.get('Records', []):
		s3rec = rec.get('s3', {})
		bucket = s3rec.get('bucket', {}).get('name')
		key = unquote_plus(s3rec.get('object', {}).get('key', ''))
		if not bucket or not key:
			continue

		payload = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode())
		uid = payload.get('user_id')
		if not uid:
			continue

		# small-scale scan to count violations for the user
		resp = profanity_table.scan(FilterExpression=Attr('user_id').eq(uid), ConsistentRead=True)
		count = resp.get('Count', 0)
		if count > VIOLATION_THRESHOLD:
			users_table.update_item(
				Key={'user_id': uid},
				UpdateExpression='SET banned = :b',
				ExpressionAttributeValues={':b': True}
			)

	return {'statusCode': 200}

