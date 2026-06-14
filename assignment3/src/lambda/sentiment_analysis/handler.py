"""Simplified sentiment analysis Lambda.
Naive rule-based sentiment and update of reviews table.
"""

import json
import os
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))

POS = {'good', 'great', 'excellent', 'love', 'amazing', 'perfect'}
NEG = {'bad', 'terrible', 'hate', 'horrible', 'awful', 'sucks'}

def get_param(name):
    try:
        return ssm.get_parameter(Name=name)['Parameter']['Value']
    except Exception:
        return os.environ.get(name.strip('/').upper(), '')

def simple_sentiment(text):
    words = set(text.lower().split())
    score = sum(1 for w in words if w in POS) - sum(1 for w in words if w in NEG)
    if score > 0:
        return 'positive'
    if score < 0:
        return 'negative'
    return 'neutral'

def handler(event, context):
    print('sentiment invoked')
    reviews_table = dynamodb.Table(get_param('/tables/reviews') or 'reviews')

    for r in event.get('Records', []):
        rec = r.get('s3', {})
        bucket = rec.get('bucket', {}).get('name')
        key = unquote_plus(rec.get('object', {}).get('key', ''))
        if not bucket or not key:
            continue

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode())
        review_id = payload.get('review_id')
        processed_text = payload.get('processed_text', '')
        if not review_id:
            continue

        sentiment = simple_sentiment(processed_text)
        reviews_table.update_item(Key={'review_id': review_id}, UpdateExpression='SET sentiment = :s', ExpressionAttributeValues={':s': sentiment})

    return {'statusCode': 200}


