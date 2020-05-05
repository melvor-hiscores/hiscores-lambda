import json
import boto3
import datetime

client = boto3.client('dynamodb')

def extract_username(event):
    
    # for parsing username
    PATH_PARAMETERS_KEY = 'pathParameters'
    USERNAME_KEY = 'username'
    FMT_USERNAME = '{ \"' + USERNAME_KEY + '\": \"<username>\" }'
    
    if (PATH_PARAMETERS_KEY not in event.keys()):
        raise Exception('500 Internal Server Error | APIGateway must have \'' + PATH_PARAMETERS_KEY + '\' as a key')
        
    try:
        return event[PATH_PARAMETERS_KEY][USERNAME_KEY]
    except Exception as e:
        raise Exception('400 Bad Request | APIGateway username must be in the format : ' + FMT_USERNAME)

def extract_data(event):
    
    # for parsing data
    BODY_KEY = 'body'
    DATA_KEY = 'data'
    FMT_DATA = '{ \"' + DATA_KEY + '\": \"<base64 encoded, gzipped data>\" }'
    
    if (BODY_KEY not in event.keys()):
        raise Exception('500 Internal Server Error | APIGateway must have \'' + BODY_KEY + '\' as a key')

    try:
        return json.loads(event[BODY_KEY])[DATA_KEY]
    except:
        raise Exception('400 Bad Request | Request body must be in the format : ' + FMT_DATA)
        
def add_data_for_user(username, data):
    response = client.put_item(
        TableName = 'MelvorHiscores',
        Item = {
            'username': {
                'S' : username
            },
            'data': {
                'S' : data
            },
            'updt_dt_tm': {
                'S' : str(datetime.datetime.utcnow())
            }
        }
    )
    print(f'melvorhiscores-poststats Processed the request for user {username} and got the response {response}')

def lambda_handler(event, context):

    try:
        username = extract_username(event)
        print('username : ' + username)
        data = extract_data(event)
        print('data : ' + data)
        
        add_data_for_user(username, data)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': None
        }
    except Exception as e:
        print(f'melvorhiscores-poststats Exception was caught : {str(e)}')
        
        try:
            errorCd = str(e)[0:3] # Get the status code from the first 3 characters of the string
            errorMsg = str(e).split(' | ')[1] # Get the status code from the first 3 characters of the string
            return {
                'statusCode': errorCd, 
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST'
                },
                'body': errorMsg
            }
        except:
            
            return {
                'statusCode': '500',
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST'
                },
                'body': 'An unknown exception occurred'
            }
