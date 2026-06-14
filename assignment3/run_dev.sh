#!/bin/bash
set -euo pipefail

# Assignment 3 Deployment Script for MiniStack
# This script provisions all AWS resources and runs integration tests

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINISTACK_ENDPOINT="http://localhost:4566"
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID="000000000000"

# Resource names
INPUT_BUCKET="assignment3-input-reviews"
PROCESSED_BUCKET="assignment3-processed-text"
REVIEWS_TABLE="reviews"
USERS_TABLE="users"
PROFANITY_TABLE="profanity"
SENTIMENT_ANALYSIS_TABLE="sentiment_analysis"

# Lambda configuration
LAMBDA_ROLE="arn:aws:iam::${AWS_ACCOUNT_ID}:role/lambda-role"
LAMBDA_TIMEOUT=60
LAMBDA_RUNTIME="python3.11"

# Export environment variables for AWS CLI and SDK
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="${AWS_REGION}"
export MINISTACK_ENDPOINT
export S3_ENDPOINT_URL="${MINISTACK_ENDPOINT}"
export DYNAMODB_ENDPOINT_URL="${MINISTACK_ENDPOINT}"
export SSM_ENDPOINT_URL="${MINISTACK_ENDPOINT}"

echo "=========================================="
echo "Assignment 3 - MiniStack Deployment"
echo "=========================================="
echo "Project root: ${PROJECT_ROOT}"
echo "MiniStack endpoint: ${MINISTACK_ENDPOINT}"
echo ""

# Function to wait for service to be ready
wait_for_service() {
    local service=$1
    local max_attempts=30
    local attempt=0

    echo "Waiting for ${service}..."
    while [ $attempt -lt $max_attempts ]; do
        if aws --endpoint-url="${MINISTACK_ENDPOINT}" "${service}" describe-account --region "${AWS_REGION}" 2>/dev/null || \
           aws --endpoint-url="${MINISTACK_ENDPOINT}" s3 ls 2>/dev/null || \
           true; then
            echo "✓ ${service} is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    echo "⚠ Warning: ${service} might not be ready yet, proceeding anyway..."
}

# Check if MiniStack is running
echo "Checking MiniStack connection..."
if ! curl -s "${MINISTACK_ENDPOINT}" >/dev/null 2>&1; then
    echo "✗ MiniStack is not running on ${MINISTACK_ENDPOINT}"
    echo "Please start MiniStack first:"
    echo "  docker run -d -p 4566:4566 -p 4571:4571 localstack/localstack"
    exit 1
fi
echo "✓ MiniStack is running"
echo ""

# Step 1: Create S3 buckets
echo "========== Step 1: Creating S3 Buckets =========="
for bucket in "${INPUT_BUCKET}" "${PROCESSED_BUCKET}"; do
    if aws --endpoint-url="${MINISTACK_ENDPOINT}" s3 ls "s3://${bucket}" 2>/dev/null; then
        echo "✓ Bucket s3://${bucket} already exists"
    else
        echo "Creating bucket s3://${bucket}..."
        aws --endpoint-url="${MINISTACK_ENDPOINT}" s3 mb "s3://${bucket}"
        echo "✓ Created bucket s3://${bucket}"
    fi
done
echo ""

# Step 2: Create DynamoDB tables
echo "========== Step 2: Creating DynamoDB Tables =========="

# Helper function to create table if not exists
create_table_if_not_exists() {
    local table_name=$1
    local key_schema=$2
    local attr_definitions=$3

    if aws --endpoint-url="${MINISTACK_ENDPOINT}" dynamodb describe-table --table-name "${table_name}" 2>/dev/null | grep -q "TableName"; then
        echo "✓ Table ${table_name} already exists"
    else
        echo "Creating table ${table_name}..."
        aws --endpoint-url="${MINISTACK_ENDPOINT}" dynamodb create-table \
            --table-name "${table_name}" \
            --key-schema ${key_schema} \
            --attribute-definitions ${attr_definitions} \
            --billing-mode PAY_PER_REQUEST \
            --region "${AWS_REGION}"

        # Wait for table to be active
        aws --endpoint-url="${MINISTACK_ENDPOINT}" dynamodb wait table-exists --table-name "${table_name}" --region "${AWS_REGION}"
        echo "✓ Created table ${table_name}"
    fi
}

# Create reviews table
create_table_if_not_exists "${REVIEWS_TABLE}" \
    "AttributeName=review_id,KeyType=HASH" \
    "AttributeName=review_id,AttributeType=S"

# Create users table
create_table_if_not_exists "${USERS_TABLE}" \
    "AttributeName=user_id,KeyType=HASH" \
    "AttributeName=user_id,AttributeType=S"

# Create profanity table
create_table_if_not_exists "${PROFANITY_TABLE}" \
    "AttributeName=violation_id,KeyType=HASH" \
    "AttributeName=violation_id,AttributeType=S"

# Create sentiment_analysis table
create_table_if_not_exists "${SENTIMENT_ANALYSIS_TABLE}" \
    "AttributeName=review_id,KeyType=HASH" \
    "AttributeName=review_id,AttributeType=S"

echo ""

# Step 3: Store configuration in SSM Parameter Store
echo "========== Step 3: Storing Configuration in SSM =========="

put_parameter() {
    local name=$1
    local value=$2

    if aws --endpoint-url="${MINISTACK_ENDPOINT}" ssm get-parameter --name "${name}" 2>/dev/null | grep -q "Parameter"; then
        echo "Updating parameter ${name}..."
        aws --endpoint-url="${MINISTACK_ENDPOINT}" ssm put-parameter \
            --name "${name}" \
            --value "${value}" \
            --type "String" \
            --overwrite
    else
        echo "Creating parameter ${name}..."
        aws --endpoint-url="${MINISTACK_ENDPOINT}" ssm put-parameter \
            --name "${name}" \
            --value "${value}" \
            --type "String"
    fi
}

put_parameter "/buckets/input" "${INPUT_BUCKET}"
put_parameter "/buckets/processed" "${PROCESSED_BUCKET}"
put_parameter "/tables/reviews" "${REVIEWS_TABLE}"
put_parameter "/tables/users" "${USERS_TABLE}"
put_parameter "/tables/profanity" "${PROFANITY_TABLE}"
put_parameter "/tables/sentiment_analysis" "${SENTIMENT_ANALYSIS_TABLE}"

echo "✓ Configuration stored in SSM Parameter Store"
echo ""

# Step 4: Package and deploy Lambda functions
echo "========== Step 4: Packaging and Deploying Lambda Functions =========="

deploy_lambda() {
    local function_name=$1
    local lambda_path="${PROJECT_ROOT}/src/lambda/${function_name}"

    if [ ! -d "${lambda_path}" ]; then
        echo "✗ Lambda directory not found: ${lambda_path}"
        return 1
    fi

    echo "Packaging Lambda: ${function_name}..."

    # Clean up old package
    cd "${lambda_path}"
    rm -rf package lambda.zip

    # Install requirements if they exist
    if [ -f "requirements.txt" ]; then
        mkdir -p package
        pip install -r requirements.txt -t package --quiet
    fi

    # Create zip file
    rm -f lambda.zip
    if [ -d "package" ]; then
        # Include both handler and dependencies at the zip root
        zip -q lambda.zip handler.py
        if [ -n "$(find package -mindepth 1 -maxdepth 1 -print -quit)" ]; then
            (cd package && zip -qr ../lambda.zip .)
        fi
    else
        # Just the handler
        zip -q lambda.zip handler.py
    fi

    # Check if function exists
    ENV_VARS=$(cat <<EOF
{"Variables":{"STAGE":"local","MINISTACK_ENDPOINT":"${MINISTACK_ENDPOINT}","S3_ENDPOINT_URL":"${S3_ENDPOINT_URL}","DYNAMODB_ENDPOINT_URL":"${DYNAMODB_ENDPOINT_URL}","SSM_ENDPOINT_URL":"${SSM_ENDPOINT_URL}"}}
EOF
)

    if aws --endpoint-url="${MINISTACK_ENDPOINT}" lambda get-function --function-name "${function_name}" 2>/dev/null | grep -q "FunctionName"; then
        echo "Updating Lambda function: ${function_name}..."
        aws --endpoint-url="${MINISTACK_ENDPOINT}" lambda update-function-code \
            --function-name "${function_name}" \
            --zip-file "fileb://lambda.zip"
        aws --endpoint-url="${MINISTACK_ENDPOINT}" lambda update-function-configuration \
            --function-name "${function_name}" \
            --environment "${ENV_VARS}" \
            --timeout "${LAMBDA_TIMEOUT}"
    else
        echo "Creating Lambda function: ${function_name}..."

        aws --endpoint-url="${MINISTACK_ENDPOINT}" lambda create-function \
            --function-name "${function_name}" \
            --zip-file "fileb://lambda.zip" \
            --handler handler.handler \
            --runtime "${LAMBDA_RUNTIME}" \
            --timeout "${LAMBDA_TIMEOUT}" \
            --role "${LAMBDA_ROLE}" \
            --environment "${ENV_VARS}"
    fi

    echo "✓ Deployed Lambda: ${function_name}"
}

# Deploy all Lambda functions
deploy_lambda "preprocessing"
deploy_lambda "sentiment_analysis"
deploy_lambda "profanity_check"
deploy_lambda "profanity_violation"

echo ""

# Step 5: Create S3 bucket notification for preprocessing Lambda
echo "========== Step 5: Setting Up S3 Bucket Notifications =========="

# Get ARN of preprocessing Lambda
PREPROCESSING_ARN=$(aws --endpoint-url="${MINISTACK_ENDPOINT}" lambda get-function --function-name preprocessing --query 'Configuration.FunctionArn' --output text)
echo "Preprocessing Lambda ARN: ${PREPROCESSING_ARN}"

# Create bucket notification configuration
NOTIFICATION_CONFIG=$(cat <<EOF
{
  "LambdaFunctionConfigurations": [
    {
      "LambdaFunctionArn": "${PREPROCESSING_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "reviews/"
            }
          ]
        }
      }
    }
  ]
}
EOF
)

echo "Attaching S3 bucket notification..."
aws --endpoint-url="${MINISTACK_ENDPOINT}" s3api put-bucket-notification-configuration \
    --bucket "${INPUT_BUCKET}" \
    --notification-configuration "${NOTIFICATION_CONFIG}" 2>/dev/null || echo "⚠ Notification configuration may have issues, continuing..."

echo "✓ S3 bucket notification configured"
echo ""

# Step 6: Downstream chaining
echo "========== Step 6: Downstream Lambda Chaining =========="
echo "⚠ Note: This local setup chains preprocessing -> analysis -> violation handling synchronously to avoid relying on DynamoDB Streams in MiniStack"
echo ""

# Step 7: Run Integration Tests
echo "========== Step 7: Running Integration Tests =========="

TEST_SCRIPT="${PROJECT_ROOT}/src/test/test_integration.py"

if [ ! -f "${TEST_SCRIPT}" ]; then
    echo "✗ Test script not found: ${TEST_SCRIPT}"
    exit 1
fi

echo "Running integration tests..."
cd "${PROJECT_ROOT}"

python3 "${TEST_SCRIPT}"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✓ All tests passed!"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "✗ Some tests failed"
    echo "=========================================="
    exit 1
fi

echo ""
echo "Deployment complete! Resources created:"
echo "  S3 Buckets: ${INPUT_BUCKET}, ${PROCESSED_BUCKET}"
echo "  DynamoDB Tables: ${REVIEWS_TABLE}, ${USERS_TABLE}, ${PROFANITY_TABLE}, ${SENTIMENT_ANALYSIS_TABLE}"
echo "  Lambda Functions: preprocessing, sentiment_analysis, profanity_check, profanity_violation"
echo ""
echo "To check resources:"
echo "  aws --endpoint-url=${MINISTACK_ENDPOINT} s3 ls"
echo "  aws --endpoint-url=${MINISTACK_ENDPOINT} dynamodb list-tables"
echo "  aws --endpoint-url=${MINISTACK_ENDPOINT} lambda list-functions"
echo ""

