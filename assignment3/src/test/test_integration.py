"""
Integration tests for assignment 3.
Tests the complete processing flow: preprocessing -> sentiment/profanity -> violation handling.
"""

import json
import os
import sys
import time
import boto3
from pathlib import Path

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
    waiter = lambda_client.get_waiter('function_active')
    waiter.wait(FunctionName=function_name)

def test_preprocessing_flow():
    # Minimal preprocessing test: upload one review and assert it appears in DynamoDB
    input_bucket = get_ssm_parameter('/buckets/input')
    processed_bucket = get_ssm_parameter('/buckets/processed')
    reviews_table = dynamodb.Table(get_ssm_parameter('/tables/reviews'))
    users_table = dynamodb.Table(get_ssm_parameter('/tables/users'))

    test_review = {'reviewerID': 'test_user_1', 'reviewerName': 'Test User', 'asin': 'B001234567', 'overall': 5, 'summary': 'Good', 'reviewText': 'Works well'}
    review_id = f"{test_review['reviewerID']}_{test_review['asin']}"

    s3_client.put_object(Bucket=input_bucket, Key=f"reviews/{review_id}.json", Body=json.dumps(test_review))
    time.sleep(2)

    resp = reviews_table.get_item(Key={'review_id': review_id})
    assert 'Item' in resp
    item = resp['Item']
    assert item['user_id'] == test_review['reviewerID']
    assert 'processed_text_key' in item
    return review_id, test_review['reviewerID']

def test_sentiment_analysis_flow(review_id, user_id):
    reviews_table = dynamodb.Table(get_ssm_parameter('/tables/reviews'))
    time.sleep(2)
    resp = reviews_table.get_item(Key={'review_id': review_id})
    assert 'Item' in resp and resp['Item'].get('sentiment') is not None

def test_profanity_check_flow(review_id, user_id):
    profanity_table = dynamodb.Table(get_ssm_parameter('/tables/profanity'))
    resp = profanity_table.scan(FilterExpression='user_id = :u', ExpressionAttributeValues={':u': user_id})
    assert resp.get('Count', 0) == 0

def test_profanity_violation_with_bad_review():
    input_bucket = get_ssm_parameter('/buckets/input')
    profanity_table = dynamodb.Table(get_ssm_parameter('/tables/profanity'))

    test_review = {'reviewerID': 'bad_user', 'reviewerName': 'Bad User', 'asin': 'B001234568', 'overall': 1, 'summary': 'Bad', 'reviewText': 'This sucks damn crap'}
    review_id = f"{test_review['reviewerID']}_{test_review['asin']}"
    s3_client.put_object(Bucket=input_bucket, Key=f"reviews/{review_id}.json", Body=json.dumps(test_review))
    time.sleep(2)

    resp = profanity_table.scan(FilterExpression='review_id = :r', ExpressionAttributeValues={':r': review_id})
    assert resp.get('Count', 0) > 0
    return test_review['reviewerID']

def test_user_banning():
    input_bucket = get_ssm_parameter('/buckets/input')
    users_table = dynamodb.Table(get_ssm_parameter('/tables/users'))
    user_id = 'ban_test_user'
    for i in range(4):
        review = {'reviewerID': user_id, 'reviewerName': 'B', 'asin': f'B00{i}', 'overall': 1, 'summary': 'Bad', 'reviewText': 'sucks damn'}
        s3_client.put_object(Bucket=input_bucket, Key=f"reviews/{user_id}_{review['asin']}.json", Body=json.dumps(review))
        time.sleep(1)

    time.sleep(3)
    resp = users_table.get_item(Key={'user_id': user_id})
    assert 'Item' in resp and resp['Item'].get('banned', False) is True

def test_data_import_from_devset():
    """Test importing reviews from the devset."""
    print("\n=== Testing Import from reviews_devset.json ===")

    input_bucket = get_ssm_parameter('/buckets/input')
    reviews_table_name = get_ssm_parameter('/tables/reviews')

    reviews_table = dynamodb.Table(reviews_table_name)

    # Path to the devset
    # Import from provided devset path (must be provided via ENV 'DEVSET_PATH' or first CLI arg)
    devset_path = os.environ.get('DEVSET_PATH') or (sys.argv[1] if len(sys.argv) > 1 else os.path.join(Path(__file__).resolve().parents[1], 'data', 'reviews_devset.json'))
    if not os.path.exists(devset_path):
        print(f"Devset not found at {devset_path}, skipping import test")
        return

    with open(devset_path, 'r') as f:
        reviews = [json.loads(line) for line in f if line.strip()]

    input_bucket = get_ssm_parameter('/buckets/input')
    for review in reviews[:5]:
        rid = f"{review.get('reviewerID','u')}_{review.get('asin','a')}"
        s3_client.put_object(Bucket=input_bucket, Key=f"devset/{rid}.json", Body=json.dumps(review))
    time.sleep(3)
    resp = reviews_table.scan()
    print(f"Imported sample, reviews table count={resp.get('Count',0)}")

if __name__ == '__main__':
    print("Starting integration tests...")
    print(f"MINISTACK_ENDPOINT: {MINISTACK_ENDPOINT}")
    print(f"S3_ENDPOINT_URL: {S3_ENDPOINT_URL}")

    try:
        # Wait for Lambdas to be available
        print("\nWaiting for Lambda functions...")
        wait_for_lambda('preprocessing')
        wait_for_lambda('sentiment_analysis')
        wait_for_lambda('profanity_check')
        wait_for_lambda('profanity_violation')

        # Run tests
        review_id, user_id = test_preprocessing_flow()
        test_sentiment_analysis_flow(review_id, user_id)
        test_profanity_check_flow(review_id, user_id)
        test_profanity_violation_with_bad_review()
        test_user_banning()
        test_data_import_from_devset()

        print("\n=== All Tests Completed ===")

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


