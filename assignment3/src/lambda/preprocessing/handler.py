"""Simple preprocessing Lambda.
Reads a review JSON from S3, creates a minimal processed text string,
writes it to a processed bucket and records entries in DynamoDB.
"""

import json
import os
import re
from urllib.parse import unquote_plus
import boto3

# lightweight clients (respect endpoint env vars if set)
s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))

def get_param(name):
    try:
        return ssm.get_parameter(Name=name)['Parameter']['Value']
    except Exception:
        # fallback to environment variable (simple, for local runs)
        return os.environ.get(name.strip('/').upper(), '')

def simple_process(text):
    # lower, keep alnum and spaces, collapse whitespace
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def handler(event, context):
    print('preprocessing invoked')
    rec = event['Records'][0]['s3']
    bucket = rec['bucket']['name']
    key = unquote_plus(rec['object']['key'])

    obj = s3.get_object(Bucket=bucket, Key=key)
    review = json.loads(obj['Body'].read().decode())

    review_id = f"{review.get('reviewerID','unknown')}_{review.get('asin','')}"
    user_id = review.get('reviewerID', 'unknown')

    processed = simple_process(((review.get('summary') or '') + ' ' + (review.get('reviewText') or '')).strip())

    processed_bucket = get_param('/buckets/processed')
    processed_key = f"processed/{review_id}.json"
    processed_payload = {
        'review_id': review_id,
        'user_id': user_id,
        'overall': review.get('overall'),
        'processed_text': processed,
    }
    s3.put_object(Bucket=processed_bucket, Key=processed_key, Body=json.dumps(processed_payload))

    # write minimal records to DynamoDB
    reviews_table = dynamodb.Table(get_param('/tables/reviews') or 'reviews')
    users_table = dynamodb.Table(get_param('/tables/users') or 'users')

    users_table.update_item(Key={'user_id': user_id}, UpdateExpression='SET banned = if_not_exists(banned, :b)', ExpressionAttributeValues={':b': False})

    reviews_table.put_item(Item={
        'review_id': review_id,
        'user_id': user_id,
        'processed_text_key': processed_key,
        'sentiment': None
    })


    return {'statusCode': 200, 'body': json.dumps({'review_id': review_id})}


