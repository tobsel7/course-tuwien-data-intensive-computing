"""
This handler performs sentiment analysis on the preprocessed text.
It stores the sentiment in the reviews DynamoDB table.
"""

import json
import os
import boto3
import nltk
from urllib.parse import unquote_plus
from nltk.sentiment.vader import SentimentIntensityAnalyzer

endpoint = os.environ.get("ENDPOINT")
s3 = boto3.client("s3", endpoint_url=endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint)
ssm = boto3.client("ssm", endpoint_url=endpoint)

# Ensure NLTK resources can be loaded locally from packaged directory
nltk_data_path = os.path.join(os.path.dirname(__file__), "nltk_data")
nltk.data.path.append(nltk_data_path)

try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    import ssl
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context
    nltk.download("vader_lexicon", quiet=True, download_dir=nltk_data_path)

sentiment_analyzer = SentimentIntensityAnalyzer()

def determine_review_sentiment(text):
    if not text:
        return "neutral"

    scores = sentiment_analyzer.polarity_scores(str(text))
    compound = scores["compound"]

    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        payload = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode())
        text = payload["processed_text"]

        sentiment = determine_review_sentiment(text)

        table_name = ssm.get_parameter(Name="/tables/reviews")["Parameter"]["Value"]
        dynamodb.Table(table_name).update_item(
            Key={"review_id": payload["review_id"]},
            UpdateExpression="SET sentiment = :s",
            ExpressionAttributeValues={":s": sentiment}
        )
    return {"statusCode": 200}
