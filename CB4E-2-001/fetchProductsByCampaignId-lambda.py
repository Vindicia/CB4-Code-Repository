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

dbRegion = 'us-west-1'


def lambda_handler(event, context):
    client = boto3.client('dynamodb')
    log.info('START_LAMBDA')
    log.info(event)
    
    # initialize some local varibales 
    jsonResponse = ""
    item = []
    
    if not event: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": envId,
           "errorMessage" : "ERROR: No attributes passed in please retry"
        }
    else: 
        
       # Check that the correct parameters were supplied 
       campaignId = event['campaignId']
       envId = event['environment']
       dbName = event['cacheName']
    
    if not campaignId: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": envId,
           "errorMessage" : "ERROR: No campaignId passed in please retry"
        }
        
    if not envId: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": envId,
           "errorMessage" : "ERROR: No environment passed in please retry"
        }
        
    if not dbName: 
        return {
           "statusCode": 400,
           "cacheName": dbName,
           "environment": envId,
           "errorMessage" : "ERROR: No cacheName passed in please retry"
        }   

    # Set the search string to include the envId prefix     
    searchCampaignId = campaignId    
        
    # Check to see if the table exists     
    print "Checking for table: {}".format(dbName)    
    conn = client.list_tables()
    tables = conn['TableNames']
    
    # if the table exists execute the search request 
    if dbName in tables: 
       dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
       table = dynamodb.Table(dbName)
       try:
          print "Fetching Products with Campaign Id {} from {} in Region {}".format(searchCampaignId, dbName, dbRegion)  
          response = table.get_item(
             Key={
               'campaignId': searchCampaignId,
               'environmentId': envId
             },
             ProjectionExpression="eligible_products",
          )
       except botocore.exceptions.ClientError as e:
           print "ERROR: {}".format(e.response['Error']['Message'])
       else:
           # Need to add handling here for no result returned 
           print response
           item = response['Item']
           jsonResponse = json.dumps(item, indent=4, cls=DecimalEncoder)
           jsonResponse = json.loads(jsonResponse.decode('string-escape').strip('"'))
    else: 
       print "No Table {} in Region {}".format(dbName, dbRegion) 
       return {
           "statusCode": 500,
           "cacheName": dbName,
           "environment": envId,
           "errorMessage" : "Cache does not exist"
       }
       
    x = 0;
    products = []
    for product in item['eligible_products']:
       products.append({'merchantProductId': item['eligible_products'][x]['id']})
       x += 1
    
    return {
           "statusCode": 200,
           "products": products,
           "environment": envId,
           "cacheName": dbName
    }
