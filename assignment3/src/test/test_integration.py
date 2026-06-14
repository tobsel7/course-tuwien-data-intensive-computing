"""
Integration test for the complete flow:
upload review -> preprocessing -> sentiment/profanity -> violation handling
"""

import json
import os
import time
from pathlib import Path

import boto3

# Get configuration from environment
MINISTACK_ENDPOINT = os.environ.get('MINISTACK_ENDPOINT', 'http://localhost:4566')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', MINISTACK_ENDPOINT)
DYNAMODB_ENDPOINT_URL = os.environ.get('DYNAMODB_ENDPOINT_URL', MINISTACK_ENDPOINT)
SSM_ENDPOINT_URL = os.environ.get('SSM_ENDPOINT_URL', MINISTACK_ENDPOINT)

# Initialize AWS clients
s3_client = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL, region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', endpoint_url=DYNAMODB_ENDPOINT_URL, region_name='us-east-1')
ssm_client = boto3.client('ssm', endpoint_url=SSM_ENDPOINT_URL, region_name='us-east-1')
lambda_client = boto3.client('lambda', endpoint_url=MINISTACK_ENDPOINT, region_name='us-east-1')


def get_ssm_parameter(name, fallback=None):
    """Get SSM parameter value or fallback to environment variable or provided fallback."""
    try:
        return ssm_client.get_parameter(Name=name)['Parameter']['Value']
    except Exception:
        # env var name without slashes, uppercased
        envname = name.strip('/').upper().replace('/', '_')
        return os.environ.get(envname, fallback)


def wait_for_lambda(function_name, timeout=30):
    """Wait for a Lambda function to become active."""
    waiter = lambda_client.get_waiter('function_active')
    waiter.wait(FunctionName=function_name)


def test_reviews_devset():
    """Smoke test: upload devset fixture. On the cluster one can verify the log output."""
    input_bucket = get_ssm_parameter('/buckets/input')

    reviews_path = Path(__file__).resolve().parents[2] / 'data' / 'reviews_devset.json'
    with reviews_path.open('rb') as f:
        put_resp = s3_client.put_object(
            Bucket=input_bucket,
            Key='reviews_devset.json',
            Body=f.read(),
        )


def test_whole_flow():
    """Test full flow with one uploaded file containing three reviews for one user."""
    input_bucket = get_ssm_parameter('/buckets/input')
    reviews_table = dynamodb.Table(get_ssm_parameter('/tables/reviews'))
    profanity_table = dynamodb.Table(get_ssm_parameter('/tables/profanity'))
    users_table = dynamodb.Table(get_ssm_parameter('/tables/users'))

    user_id = 'test_user_1'
    reviews = [
        {
            'reviewerID': user_id,
            'reviewerName': 'Test User',
            'asin': 'B001234567',
            'overall': 1,
            'summary': 'Bad',
            'reviewText': 'damn, bad',
        },
        {
            'reviewerID': user_id,
            'reviewerName': 'Test User',
            'asin': 'B001234568',
            'overall': 1,
            'summary': 'Bad',
            'reviewText': 'damn, bad',
        },
        {
            'reviewerID': user_id,
            'reviewerName': 'Test User',
            'asin': 'B001234569',
            'overall': 1,
            'summary': 'Bad',
            'reviewText': 'damn, bad',
        },
    ]

    payload = '\n'.join(json.dumps(review) for review in reviews)

    s3_client.put_object(
        Bucket=input_bucket,
        Key='reviews/test_user_1_batch.jsonl',
        Body=payload,
    )

    time.sleep(4)

    for review in reviews:
        review_id = f"{review['reviewerID']}_{review['asin']}"

        review_resp = reviews_table.get_item(Key={'review_id': review_id}, ConsistentRead=True)
        assert 'Item' in review_resp

        violation_resp = profanity_table.get_item(Key={'violation_id': review_id}, ConsistentRead=True)
        assert 'Item' in violation_resp

    user_resp = users_table.get_item(Key={'user_id': user_id}, ConsistentRead=True)
    assert 'Item' in user_resp
    assert user_resp['Item'].get('banned') is True


if __name__ == '__main__':
    print("Starting integration test...")
    wait_for_lambda('preprocessing')
    wait_for_lambda('sentiment_analysis')
    wait_for_lambda('profanity_check')
    wait_for_lambda('profanity_violation')
    test_whole_flow()
    test_reviews_devset()
    print("Test completed.")
