"""Simple profanity check Lambda.
Checks processed text for coarse bad words and writes violation items.
"""

import json
import os
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('DYNAMODB_ENDPOINT_URL'))
ssm = boto3.client('ssm', endpoint_url=os.environ.get('SSM_ENDPOINT_URL'))

BAD = {'damn', 'crap', 'sucks', 'hate', 'shit', 'fuck'}

def get_param(name):
    try:
        return ssm.get_parameter(Name=name)['Parameter']['Value']
    except Exception:
        return os.environ.get(name.strip('/').upper(), '')

def handler(event, context):
    print('profanity_check invoked')
    reviews_table = dynamodb.Table(get_param('/tables/reviews') or 'reviews')
    profanity_table = dynamodb.Table(get_param('/tables/profanity') or 'profanity')
    violations_bucket = get_param('/buckets/violations')

    for r in event.get('Records', []):
        rec = r.get('s3', {})
        bucket = rec.get('bucket', {}).get('name')
        key = unquote_plus(rec.get('object', {}).get('key', ''))
        if not bucket or not key:
            continue

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode())
        review_id = payload.get('review_id')
        user_id = payload.get('user_id', '')
        txt = payload.get('processed_text', '')
        if not review_id:
            continue

        item = reviews_table.get_item(Key={'review_id': review_id}, ConsistentRead=True).get('Item')
        if not item:
            continue

        found = [w for w in set(txt.split()) if w in BAD]
        if not found:
            continue

        violation = {'violation_id': review_id, 'review_id': review_id, 'user_id': item.get('user_id', user_id), 'words': found}
        profanity_table.put_item(Item=violation)
        s3.put_object(Bucket=violations_bucket, Key=f"violations/{review_id}.json", Body=json.dumps(violation))

    return {'statusCode': 200}



