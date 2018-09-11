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
    objectId = event['id']
    objectType = event['type']
    dbName = event['cacheName']
    environment = event['environment']
    
    # Check that the correct parameters were supplied 
    if not objectId: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "id": objectId,
           "type": objectType,
           "errorMessage" : "ERROR: No id passed in please retry"
        }
    
    if not objectType: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "id": objectId,
           "type": objectType,
           "errorMessage" : "ERROR: No type passed in please retry"
        }

    if not dbName: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "id": objectId,
           "type": objectType,
           "errorMessage" : "ERROR: No cacheName Name passed in please retry"
        }

    if not environment: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "id": objectId,
           "type": objectType,           
           "errorMessage" : "ERROR: No environment value passed in please retry"
        }
    
    if objectType == 'product': 
        filter = 'merchantProductId' 
    elif objectType == 'billingPlan': 
        filter = 'merchantBillingPlanId' 
    elif objectType == 'campaign': 
        filter = 'campaignId' 
    else: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": environment,
           "id": objectId,
           "type": objectType,           
           "errorMessage" : "ERROR: No type match"
        }    
        
    response = client.list_tables()
    tables = response['TableNames']
    jsonResponse = ""
    if dbName in tables: 
       print "Fetching {} = {} from {} in Region {}".format(objectType, objectId, dbName, dbRegion) 
       dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
       table = dynamodb.Table(dbName)
       try:
           response = table.get_item(
                 Key={
                        filter : objectId,
                        'environmentId': environment
                 }
           )
       except botocore.exceptions.ClientError as e:
           print "ERROR: {}".format(e.response['Error']['Message'])
           return {
               "statusCode": 406,
               "cacheName": dbName,
               "environment": environment,
               "id": objectId,
               "type": objectType,               
               "errorMessage" : "ERROR: Unknown DB Error"
           }
       else:
           if 'Item' in response: 
               item = response['Item']
               jsonResponse = json.dumps(item, indent=4, cls=DecimalEncoder)
               jsonResponse = json.loads(jsonResponse.decode('string-escape').strip('"'))
           else: 
               return {
                   "statusCode": 206,
                   "cacheName": dbName,
                   "environment": environment,
                   "id": objectId,
                   "type": objectType,            
                   "errorMessage" : "NO RECORD EXISTS IN CACHE"
               }               
    else: 
       print "No Table {} in Region {}".format(dbName, dbRegion) 
       return { 
           "statusCode": 500, 
           "environment": environment,
           "cacheName": dbName,
           "id": objectId,
           "type": objectType,          
           "response": "Error Cache does not exist" } 
       
    return {
        "statusCode": 200,
        "cacheName": dbName,
        "environment": environment,
        "id": objectId,
        "type": objectType,        
        "response": jsonResponse } 

