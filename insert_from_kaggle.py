import csv
from decimal import Decimal
import boto3

# Set the access keys
access_key_id = 'AKIA5WVQT66QRM47OT7N'
secret_access_key = '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'

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
dynamoTable = dynamodb.Table('BC_Tweets')

with open('Bitcoin_tweets.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Extract the relevant fields from the CSV row
        user_name = row['user_name']
        user_location = row['user_location']
        user_description = row['user_description']
        user_created = row['user_created']
        user_followers = row['user_followers']
        user_friends = row['user_friends']
        user_favorites = row['user_favourites']
        user_verified = row['user_verified']
        date = row['date']
        text = row['text']
        hashtags = row['hashtags']
        source = row['source']
        is_retweet = row['is_retweet']

        # Build the DynamoDB item
        item = {
            'user_name': user_name,
            'user_location': user_location,
            'user_description': user_description,
            'user_created': user_created,
            'user_followers': Decimal(user_followers),
            'user_friends': int(user_friends),
            'user_favorites': int(user_favorites),
            'user_verified': user_verified,
            'date': date,
            'text': text,
            'hashtags': hashtags,
            'source': source,
            'is_Retweet': is_retweet
        }

        # Make sure that user_location is not an empty string value
        if not item['user_location']:
            del item['user_location']

        # Insert the item into the DynamoDB table
        dynamoTable.put_item(Item=item)

print('Data insertion complete.')
