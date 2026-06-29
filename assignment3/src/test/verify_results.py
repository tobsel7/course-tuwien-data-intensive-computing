import os
import boto3
from collections import Counter

# Set endpoint for MiniStack/LocalStack
MINISTACK_ENDPOINT = os.environ.get('MINISTACK_ENDPOINT', 'http://localhost:4566')
SSM_ENDPOINT_URL = os.environ.get('SSM_ENDPOINT_URL', MINISTACK_ENDPOINT)
DYNAMODB_ENDPOINT_URL = os.environ.get('DYNAMODB_ENDPOINT_URL', MINISTACK_ENDPOINT)

# Initialize clients with dummy credentials for local MiniStack/LocalStack
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET = os.environ.get('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')

ssm_client = boto3.client(
    'ssm', 
    endpoint_url=SSM_ENDPOINT_URL, 
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET
)
dynamodb = boto3.resource(
    'dynamodb', 
    endpoint_url=DYNAMODB_ENDPOINT_URL, 
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET
)

def get_ssm_parameter(name):
    """Get parameter value from SSM Parameter Store."""
    try:
        return ssm_client.get_parameter(Name=name)['Parameter']['Value']
    except Exception as e:
        print(f"Error fetching SSM parameter {name}: {e}")
        # Fallback names
        fallback_map = {
            '/tables/reviews': 'reviews',
            '/tables/profanity': 'profanity',
            '/tables/users': 'users'
        }
        return fallback_map.get(name)

def get_sentiment_counts(table_name):
    """Scan reviews table and count sentiments."""
    table = dynamodb.Table(table_name)
    counts = Counter()
    
    scan_params = {
        'ProjectionExpression': 'sentiment'
    }
    
    while True:
        response = table.scan(**scan_params)
        for item in response.get('Items', []):
            sentiment = item.get('sentiment')
            # If sentiment is not populated yet or is None
            if sentiment is None:
                sentiment = 'unprocessed/None'
            counts[sentiment] += 1
            
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break
            
    return dict(counts)

def get_profanity_count(table_name):
    """Scan profanity table and count total items."""
    table = dynamodb.Table(table_name)
    count = 0
    
    scan_params = {
        'Select': 'COUNT'
    }
    
    while True:
        response = table.scan(**scan_params)
        count += response.get('Count', 0)
        
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break
            
    return count

def get_banned_users(table_name):
    """Scan users table and return a list of banned user IDs."""
    from boto3.dynamodb.conditions import Attr
    table = dynamodb.Table(table_name)
    banned_users = []
    
    scan_params = {
        'FilterExpression': Attr('banned').eq(True),
        'ProjectionExpression': 'user_id'
    }
    
    while True:
        response = table.scan(**scan_params)
        for item in response.get('Items', []):
            banned_users.append(item.get('user_id'))
            
        if 'LastEvaluatedKey' in response:
            scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            break
            
    return banned_users

def main():
    print("=" * 50)
    print("Fetching DynamoDB Table Names from SSM...")
    reviews_table = get_ssm_parameter('/tables/reviews')
    profanity_table = get_ssm_parameter('/tables/profanity')
    users_table = get_ssm_parameter('/tables/users')
    
    print(f"Reviews Table:   {reviews_table}")
    print(f"Profanity Table: {profanity_table}")
    print(f"Users Table:     {users_table}")
    print("=" * 50)
    
    print("\nQuerying results...")
    
    # 1. Sentiment counts
    sentiments = get_sentiment_counts(reviews_table)
    print("\n[1] Sentiment Analysis Results:")
    for sentiment, count in sentiments.items():
        print(f"  - {sentiment}: {count}")
        
    # 2. Profanity violation counts
    profanity_violations = get_profanity_count(profanity_table)
    print(f"\n[2] Reviews failing profanity check: {profanity_violations}")
    
    # 3. Banned users
    banned_users = get_banned_users(users_table)
    print(f"\n[3] Banned Users (Total: {len(banned_users)}):")
    if banned_users:
        for user in banned_users:
            print(f"  - {user}")
    else:
        print("  - None")
        
    print("\n" + "=" * 50)

if __name__ == '__main__':
    main()
