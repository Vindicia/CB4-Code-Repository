import json
import boto3
import botocore 
import datetime
from boto3.dynamodb.conditions import Key, Attr
import logging
import decimal

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)
dbRegion = "us-west-1"


def lambda_handler(event, context):
    client = boto3.client('dynamodb')
    log.info('START_LAMBDA')
    log.info(event)
    name = event['name']
    value = event['value']
    dbName = event['cacheName']
    environment = event['environment']
    
    # Check that the correct parameters were supplied 
    dbName = event['cacheName']
    if not dbName: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "errorMessage" : "ERROR: No cacheName passed in please retry"
        }
    
    environment = event['environment']
    if not environment: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "errorMessage" : "ERROR: No environment passed in please retry"
        }

    name = event['name']
    if not name: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "errorMessage" : "ERROR: No MetaData Name passed in please retry"
        }

    value = event['value']
    if not value: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "errorMessage" : "ERROR: No MetaData value passed in please retry"
        }
    
    filter = 'metadata.' + name
    response = client.list_tables()
    tables = response['TableNames']
    jsonResponse = ""
    if dbName in tables: 
       print "Fetching {} = {} from {} in Region {}".format(name, value, dbName, dbRegion) 
       dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
       table = dynamodb.Table(dbName)
       try:
           response = table.scan(
             FilterExpression = Attr(filter).eq(value) & Attr('environmentId').eq(environment) 
           )
       except botocore.exceptions.ClientError as e:
           print(e.response['Error']['Message'])
       else:
           item = response['Items']
           jsonResponse = json.dumps(item, indent=4, cls=DecimalEncoder)
           jsonResponse = json.loads(jsonResponse.decode('string-escape').strip('"'))
    else: 
       print "No Table {} in Region {}".format(dbName, dbRegion) 
       return { 
           "statusCode": 500, 
           "environment": environment,
           "cacheName": dbName,
           "response": "Error Cache does not exist" } 
       
    return {
        "statusCode": 200,
        "cacheName": dbName,
        "environment": environment,
        "response": jsonResponse } 
