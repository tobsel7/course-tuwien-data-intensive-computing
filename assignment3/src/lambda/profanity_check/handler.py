"""Simple profanity check Lambda.
Checks processed text for coarse bad words and writes violation items.
"""

import json
import os
import boto3

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
    processed_bucket = get_param('/buckets/processed')

    for r in event.get('Records', []):
        keys = r.get('dynamodb', {}).get('Keys', {})
        review_id = keys.get('review_id', {}).get('S') if keys else None
        if not review_id:
            continue

        item = reviews_table.get_item(Key={'review_id': review_id}).get('Item')
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

        found = [w for w in set(txt.split()) if w in BAD]
        if not found:
            continue

        profanity_table.put_item(Item={'violation_id': f"{review_id}", 'review_id': review_id, 'user_id': item.get('user_id', ''), 'words': ','.join(found)})

    return {'statusCode': 200}



