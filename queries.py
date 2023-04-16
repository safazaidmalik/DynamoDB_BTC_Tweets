main.pyimport csv
from decimal import Decimal
import boto3

# Set the access keys
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1', aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
dynamoTable = dynamodb.Table('BC_Tweets')


def query1(username):
    response = dynamoTable.query(
        KeyConditionExpression='user_name = :user',
        ExpressionAttributeValues={
            ':user': username
        }
    )

    # Print the tweets
    tweets = response['Items']
    for tweet in tweets:
        print(tweet['text'])


def query2(location):
    response = dynamoTable.query(
        IndexName='user_location',
        KeyConditionExpression='user_location = :location',
        ExpressionAttributeValues={
            ':location': location
        }
    )

    tweets = response['Items']

    # Print the tweets
    tweets = response['Items']
    for tweet in tweets:
        print(tweet['text'])


def query3(k):
    # Query for the first 10 items
    response = dynamoTable.scan(Limit=k)

    # Print the items
    for item in response['Items']:
        print(item['user_name'])

    return response


def query4(query3_response):
    # Print the items
    for item in query3_response['Items']:
        print(item['text'])


def query5(k, tags):
    response = dynamoTable.query(
        IndexName='hashtags-index',
        KeyConditionExpression='hashtags = :hashtags',
        ExpressionAttributeValues={
            ':hashtags': tags
        }
    )

    # Print the tweets
    tweets = response['Items']
    i = 0
    for tweet in tweets:
        if tweet['hashtags'] in htags:
            print(tweet['text'])
            i += 1
        if i == k:
            break


def query6(threshold):
    response = dynamoTable.scan(
        FilterExpression='user_followers < :val',
        ExpressionAttributeValues={
            ':val': threshold
        }
    )

    for item in response['Items']:
        dynamoTable.delete_item(
            Key={
                'user_name': item['user_name'],
                'user_followers': item['user_followers']
            }
        )


# query1('wakawaka')
# query2('United States')
# result = query3(5)
# query4(result)

# htags = ['Bitcoin']
htags = 'Bitcoin'
query5(5, htags)

# query6(1000)
