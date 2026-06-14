"""Violation handler: bans users after a number of profanity violations.

This Lambda is triggered by DynamoDB stream on the `profanity` table.
It counts violations per user (simple scan, OK for small test data) and
sets `banned = true` on the corresponding `users` table item when the
threshold is exceeded.
"""

import os
import boto3
from boto3.dynamodb.conditions import Attr

VIOLATION_THRESHOLD = 3

# Respect endpoint overrides when running against MiniStack
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))

def get_param(name):
	try:
		return ssm.get_parameter(Name=name)['Parameter']['Value']
	except Exception:
		# fallback to environment variable or empty
		return os.environ.get(name.strip('/').upper(), '')

def handler(event, context):
	"""Process DynamoDB stream records and ban users when needed."""
	profanity_table = dynamodb.Table(get_param('/tables/profanity') or 'profanity')
	users_table = dynamodb.Table(get_param('/tables/users') or 'users')

	users = set()
	for rec in event.get('Records', []):
		new_img = rec.get('dynamodb', {}).get('NewImage')
		if not new_img:
			continue
		# DynamoDB stream represents attributes as { 'S': 'value' }
		user_attr = new_img.get('user_id')
		if isinstance(user_attr, dict):
			uid = user_attr.get('S')
		else:
			uid = user_attr
		if uid:
			users.add(uid)

	for uid in users:
		# small-scale scan to count violations for the user
		resp = profanity_table.scan(FilterExpression=Attr('user_id').eq(uid))
		count = resp.get('Count', 0)
		if count > VIOLATION_THRESHOLD:
			users_table.update_item(
				Key={'user_id': uid},
				UpdateExpression='SET banned = :b',
				ExpressionAttributeValues={':b': True}
			)

	return {'statusCode': 200}

