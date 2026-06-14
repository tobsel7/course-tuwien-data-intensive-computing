#!/bin/bash
set -e
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

ENDPOINT="http://localhost:4566"
AWS="aws --endpoint-url=$ENDPOINT"
ROLE="arn:aws:iam::000000000000:role/lambda-role"

echo "Creating S3 buckets..."
$AWS s3 mb s3://input-reviews || true
$AWS s3 mb s3://processed-text || true
$AWS s3 mb s3://profanity-violations || true

echo "Creating DynamoDB tables..."
$AWS dynamodb create-table --table-name reviews --attribute-definitions AttributeName=review_id,AttributeType=S --key-schema AttributeName=review_id,KeyType=HASH --billing-mode PAY_PER_REQUEST || true
$AWS dynamodb create-table --table-name users --attribute-definitions AttributeName=user_id,AttributeType=S --key-schema AttributeName=user_id,KeyType=HASH --billing-mode PAY_PER_REQUEST || true
$AWS dynamodb create-table --table-name profanity --attribute-definitions AttributeName=violation_id,AttributeType=S --key-schema AttributeName=violation_id,KeyType=HASH --billing-mode PAY_PER_REQUEST || true

echo "Storing configuration in SSM..."
$AWS ssm put-parameter --name /buckets/input --type String --value input-reviews --overwrite
$AWS ssm put-parameter --name /buckets/processed --type String --value processed-text --overwrite
$AWS ssm put-parameter --name /buckets/violations --type String --value profanity-violations --overwrite
$AWS ssm put-parameter --name /tables/reviews --type String --value reviews --overwrite
$AWS ssm put-parameter --name /tables/users --type String --value users --overwrite
$AWS ssm put-parameter --name /tables/profanity --type String --value profanity --overwrite

deploy_lambda() {
    NAME=$1
    DIR="src/lambda/$NAME"
    echo "Deploying $NAME..."
    (cd "$DIR" && rm -f lambda.zip && zip -q lambda.zip handler.py)
    $AWS lambda create-function --function-name "$NAME" --runtime python3.11 --role "$ROLE" --handler handler.handler --zip-file "fileb://$DIR/lambda.zip" --environment "Variables={ENDPOINT=$ENDPOINT,STAGE=local}" || \
    $AWS lambda update-function-code --function-name "$NAME" --zip-file "fileb://$DIR/lambda.zip"
    $AWS lambda update-function-configuration --function-name "$NAME" --environment "Variables={ENDPOINT=$ENDPOINT,STAGE=local}"
}

deploy_lambda preprocessing
deploy_lambda sentiment_analysis
deploy_lambda profanity_check
deploy_lambda profanity_violation

echo "Configuring S3 notifications..."
PRE_ARN=$($AWS lambda get-function --function-name preprocessing --query 'Configuration.FunctionArn' --output text)
SENT_ARN=$($AWS lambda get-function --function-name sentiment_analysis --query 'Configuration.FunctionArn' --output text)
PROF_ARN=$($AWS lambda get-function --function-name profanity_check --query 'Configuration.FunctionArn' --output text)
VIOL_ARN=$($AWS lambda get-function --function-name profanity_violation --query 'Configuration.FunctionArn' --output text)

$AWS s3api put-bucket-notification-configuration --bucket input-reviews --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"$PRE_ARN\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"
$AWS s3api put-bucket-notification-configuration --bucket processed-text --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"$SENT_ARN\",\"Events\":[\"s3:ObjectCreated:*\"]},{\"LambdaFunctionArn\":\"$PROF_ARN\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"
$AWS s3api put-bucket-notification-configuration --bucket profanity-violations --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"LambdaFunctionArn\":\"$VIOL_ARN\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"

echo "Running integration tests..."
python3 src/test/test_integration.py
