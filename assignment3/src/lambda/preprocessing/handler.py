"""Simple preprocessing Lambda.
Reads a review JSON from S3, creates a minimal processed text string,
writes it to a processed bucket and records entries in DynamoDB.
"""

import json
import os
import re
import boto3

# lightweight clients (respect endpoint env vars if set)
s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))
lambda_client = boto3.client('lambda', endpoint_url=os.environ.get('MINISTACK_ENDPOINT'))

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

def invoke_downstream(function_name, review_id, user_id, processed_key):
    event = {
        'Records': [{
            'eventName': 'INSERT',
            'eventSource': 'aws:dynamodb',
            'dynamodb': {
                'Keys': {'review_id': {'S': review_id}},
                'NewImage': {
                    'review_id': {'S': review_id},
                    'user_id': {'S': user_id},
                    'processed_text_key': {'S': processed_key},
                    'sentiment': {'NULL': True},
                },
            },
        }]
    }
    resp = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(event).encode('utf-8'),
    )
    if resp.get('FunctionError'):
        raise RuntimeError(f"{function_name} failed: {resp['FunctionError']}")

def handler(event, context):
    print('preprocessing invoked')
    review = event

    review_id = f"{review.get('reviewerID','unknown')}_{review.get('asin','')}"
    user_id = review.get('reviewerID', 'unknown')

    processed = simple_process(((review.get('summary') or '') + ' ' + (review.get('reviewText') or '')).strip())

    processed_bucket = get_param('/buckets/processed')
    processed_key = f"processed/{review_id}.txt"
    s3.put_object(Bucket=processed_bucket, Key=processed_key, Body=processed)

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

    invoke_downstream('sentiment_analysis', review_id, user_id, processed_key)
    invoke_downstream('profanity_check', review_id, user_id, processed_key)

    return {'statusCode': 200, 'body': json.dumps({'review_id': review_id})}


