"""Simplified sentiment analysis Lambda.
Naive rule-based sentiment and update of reviews table.
"""

import os
import boto3

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
    processed_bucket = get_param('/buckets/processed')

    for r in event.get('Records', []):
        # expect stream record with keys
        keys = r.get('dynamodb', {}).get('Keys', {})
        review_id = keys.get('review_id', {}).get('S') if keys else None
        if not review_id:
            continue

        item = reviews_table.get_item(Key={'review_id': review_id}, ConsistentRead=True).get('Item')
        if not item:
            continue
        pkey = item.get('processed_text_key')
        if not pkey:
            continue

        bucket = processed_bucket or os.environ.get('DEFAULT_S3_BUCKET')
        try:
            txt = s3.get_object(Bucket=bucket, Key=pkey)['Body'].read().decode()
        except Exception:
            continue

        sentiment = simple_sentiment(txt)
        reviews_table.update_item(Key={'review_id': review_id}, UpdateExpression='SET sentiment = :s', ExpressionAttributeValues={':s': sentiment})

    return {'statusCode': 200}


