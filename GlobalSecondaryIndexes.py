import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
dynamoTable = dynamodb.Table('BC_Tweets')


# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')


def createLocationGSI():
    # Define the table name and index name
    table_name = 'BC_Tweets'
    index_name = 'user_location'

    # Define the attributes for the GSI
    key_schema = [
        {
            'AttributeName': 'user_location',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'user_name',
            'KeyType': 'RANGE'
        }
    ]

    # Define the provisioned throughput for the GSI
    provisioned_throughput = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }

    # Define the GSI options
    options = {
        'Projection': {
            'ProjectionType': 'INCLUDE',
            'NonKeyAttributes': ['text']
        }
    }

    # Create the GSI
    response = dynamodb.update_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'user_location',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'user_name',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': index_name,
                    'KeySchema': key_schema,
                    'ProvisionedThroughput': provisioned_throughput,
                    'Projection': options['Projection']
                }
            }
        ]
    )


def createTagGSI():

    # Define the table name and index name
    table_name = 'BC_Tweets'
    index_name = 'hashtags-index'

    # Define the attributes for the GSI
    key_schema = [
        {
            'AttributeName': 'hashtags',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'text',
            'KeyType': 'RANGE'
        }
    ]

    # Define the provisioned throughput for the GSI
    provisioned_throughput = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }

    # Define the GSI options
    options = {
        'Projection': {
            'ProjectionType': 'INCLUDE',
            'NonKeyAttributes': ['user_name']
        }
    }

    # Create the GSI
    response = dynamodb.update_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'hashtags',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'text',
                'AttributeType': 'S'
            }
        ],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': index_name,
                    'KeySchema': key_schema,
                    'ProvisionedThroughput': provisioned_throughput,
                    'Projection': options['Projection']
                }
            }
        ]
    )

    # Print the response
    print(response)


# Define the Global Secondary Index on the "User followers" attribute
def createUserFollowersGSI(k):

    index_name = 'user-followers-index'
    key_schema = [
        {
            'AttributeName': 'user_followers',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'user_name',
            'KeyType': 'RANGE'
        }
    ]
    projection = {
        'ProjectionType': 'INCLUDE',
        'NonKeyAttributes': ['text']
    }
    dynamoTable.update(
        AttributeDefinitions=[
            {'AttributeName': 'user_followers',
             'AttributeType': 'N'},

            {'AttributeName': 'user_name',
             'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': index_name,
                    'KeySchema': key_schema,
                    'Projection': projection,
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                }
            }
        ]
    )

    # Query the GSI with the descending sort order
    response = dynamoTable.query(
        IndexName=index_name,
        KeyConditionExpression=Attr('user_followers').attribute_exists(),
        ExpressionAttributeNames={
            '#follower': 'user_followers'
        },
        ScanIndexForward=False,
        Limit=k,
        ProjectionExpression='user_name, user_followers'
    )

    # Print the top k users with the most followers
    for item in response['Items']:
        print(item['user_name'], item['user_followers'])
