"""
This handler reads raw review files (single review JSON per file).
It:
- stores the extracted text in a processed S3 bucket
- populates the reviews DynamoDB table
- populates the users DynamoDB table
"""

import json
import os
import string
import boto3
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from urllib.parse import unquote_plus

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)

# Ensure NLTK resources can be loaded locally from packaged directory
nltk_data_path = os.path.join(os.path.dirname(__file__), "nltk_data")
nltk.data.path.append(nltk_data_path)

# Ensure NLTK resources are loaded/downloaded
for resource in ["punkt", "punkt_tab", "stopwords", "wordnet", "omw-1.4"]:
    try:
        if resource == "punkt":
            nltk.data.find("tokenizers/punkt")
        elif resource == "punkt_tab":
            nltk.data.find("tokenizers/punkt_tab")
        else:
            nltk.data.find(f"corpora/{resource}")
    except LookupError:
        # Fallback to downloading if not found locally
        import ssl
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context
        nltk.download(resource, quiet=True, download_dir=nltk_data_path)

lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words("english"))


def preprocess_review_text(review):
    # Combine summary and reviewText
    text = f"{review.get('summary', '')} {review.get('reviewText', '')}".lower().strip()
    
    # Tokenization
    tokens = word_tokenize(text)
    
    # Stop word removal and Lemmatization
    cleaned_tokens = []
    for token in tokens:
        # Ignore punctuation
        if token in string.punctuation:
            continue
        # Ignore stop words
        if token in stop_words:
            continue
        # Lemmatize
        cleaned_tokens.append(lemmatizer.lemmatize(token))
        
    return " ".join(cleaned_tokens)


def handler(event, context):
    processed_bucket = ssm.get_parameter(Name="/buckets/processed")["Parameter"]["Value"]
    reviews_table_name = ssm.get_parameter(Name="/tables/reviews")["Parameter"]["Value"]
    users_table_name = ssm.get_parameter(Name="/tables/users")["Parameter"]["Value"]

    reviews_table = dynamodb.Table(reviews_table_name)
    users_table = dynamodb.Table(users_table_name)

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        raw_payload = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        review = json.loads(raw_payload)

        user_id = review["reviewerID"]
        review_id = f"{user_id}_{review['asin']}"
        processed_text = preprocess_review_text(review)

        s3.put_object(
            Bucket=processed_bucket,
            Key=f"processed/{review_id}.json",
            Body=json.dumps({"review_id": review_id, "user_id": user_id, "processed_text": processed_text})
        )

        reviews_table.put_item(Item={
            "review_id": review_id,
            "user_id": user_id,
            "sentiment": None
        })

        # create only if missing
        users_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET banned = if_not_exists(banned, :default)",
            ExpressionAttributeValues={":default": False}
        )
    return {"statusCode": 200}
