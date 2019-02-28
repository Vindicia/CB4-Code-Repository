import json
import boto3
import botocore 
import logging
import os
import datetime
import hmacValidation as auth
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

# Grab the HMAC from the environment variable 
hmac_key = os.environ['hmac_key']

def lambda_handler(event, context):
    log.info('START_LAMBDA')
    log.info(event)
    client = boto3.client('dynamodb')
    dbName = 'entitlementsCache'
    
    # Perform HMAC Validation
    webHook = event['headers']['X-Webhook-Signature']
    if not auth.Authoriser(webHook, hmac_key).validate(event['body']):
        log.info('HMAC CHECK FAILED')
        raise Exception("403 - Not Authorised")
        return 403
    
    # Convert entire body to unicode to avoid string indice error 
    if isinstance(event['body'], (unicode, str)):
        event['body'] = json.loads(event['body'].decode('string-escape').strip('"'))
        log.info('UNICODE PAYLOAD: %s', event)
        log.info('HTTP HEADERS: %s', event['headers'])
    else: 
        log.warning('ERROR PROCESSING EVENT BODY')

    # Grab the account id and the entitlement id and check if it already exists
    # Read the object so you have the status log and can append to it
    merchantAccountId = event['body']['content']['merchantAccountId']
    merchantEntitlementId = event['body']['content']['merchantEntitlementId'][0]
    
    response = client.list_tables()
    tables = response['TableNames']
    jsonResponse = ""
    if dbName in tables: 
       print "Fetching Account {} with Entitlement {} from {} in Region {}".format(merchantAccountId, merchantEntitlementId, dbName, dbRegion) 
       dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
       table = dynamodb.Table(dbName)
       try:
           response = table.get_item(
                 Key={
                        'merchantAccountId' : merchantAccountId,
                        'merchantEntitlementId': merchantEntitlementId
                 }
           )    
       except botocore.exceptions.ClientError as e:
           print "ERROR: {}".format(e.response['Error']['Message'])
           return {
               "statusCode": 406,
               "cacheName": dbName,
               "errorMessage" : "ERROR: Unknown DB Error"
           }
       else:    
           if 'Item' in response: 
               item = response['Item']
               jsonResponse = json.dumps(item, indent=4, cls=DecimalEncoder)
               jsonResponse = json.loads(jsonResponse.decode('string-escape').strip('"'))
               log.info(jsonResponse) 
           else: 
               log.info("NO RECORD EXISTS IN CACHE")
               return {
                   "statusCode": 206,
                   "cacheName": dbName,
                   "merchantAccountId": merchantAccountId,
                   "merchantEntitlementId": merchantEntitlementId,            
                   "errorMessage" : "NO RECORD EXISTS IN CACHE"
               } 
               
          

    autoBillItemVID = event['body']['content']['autoBillItemVid'] 
    endTimestamp = event['body']['content']['endTimestamp']
    entitlementSource = event['body']['content']['entitlementSource']
    eventTimestamp = event['body']['header']['event_timestamp']
    merchantAutoBillId = event['body']['content']['merchantAutobillId']
    merchantAutoBillItemId = event['body']['content']['merchantAutoBillItemId']
    merchantBillingPlanId = event['body']['content']['merchantBillingPlanId']
    merchantProductId = event['body']['content']['merchantProductId']
    messageId = event['body']['header']['message_id']
    startTimestamp = event['body']['content']['startTimestamp']

    # We only process stop entitlement notifications with the assumption that entitlements 
    # are started immediately after successful purchase, and using the REST API startEntitlement

    # Get the status, if it is already on return a 400 error 
    # Note that 'status' is a reserved word in DynamoDB syntax 
    status = item['entitled']
    if status == "0": 
        print("Error: 400 Entitlement Already Inactive")
        return {
                'statusCode': 400,
                'body': json.dumps('Error: Entitlement Already Inactive')
        }
        print("Already Turned On Error")
    
    #get the status log 
    status_log = item['status_log']
    #print(status_log)
    
    ent_stop_ts_aware = datetime.datetime.now()
    ent_stop_ts_aware = ent_stop_ts_aware.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Append the new status with the old status log 
    status_log['data'].append(
            {  
                           "action": 'stop',
                           "merchantAutoBillId": merchantAutoBillId,
                           "merchantAutoBillItemId": merchantAutoBillItemId,
                           "entitlementSource": 'Product',
                           "merchantProductId": merchantProductId, 
                           "autoBillItemVID": autoBillItemVID, 
                           "startTimestamp": startTimestamp,
                           "endTimestamp": ent_stop_ts_aware,
                           "eventTimestamp": eventTimestamp,
                           "messageId": messageId,
                           "cacheEntryTimestamp": ent_stop_ts_aware,
                           "source": "CB4-stopEntitlementListner"
           })
        
    # Update the status log counter 
    status_log['totalCount'] = str(len(status_log['data']))
    
    # Update the table
    table.update_item(
            Key={
                    'merchantAccountId': merchantAccountId,
                    'merchantEntitlementId': merchantEntitlementId
                },
                   UpdateExpression='SET entitled = :val1, status_log = :val2',
                   ExpressionAttributeValues={
                       ':val1': "0",
                       ':val2': status_log
                }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Entitlement Stop Processed Successfully')
    }