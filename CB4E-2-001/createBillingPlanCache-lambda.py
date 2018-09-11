import pycurl
import StringIO
import base64
import sys
import json
import boto3
import botocore 
import datetime
import time 
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)

# This list controls what attributes we put into the cache
# You can edit this list 
# Possible values are: {"status", "end_of_life", "created", "descriptions", "prices", "entitlements", "credits", "metadata"} 
attributeCache = {"status", "description", "vid", "created", "end_of_life", "times_to_run", "billing_descriptor", "periods",
                  "used_on_subscriptions", "entitlements", "metadata"} 
#attributeCache = {"description", "created", "metadata"}

# If the value of the override is equal to ALL then we just store the entire product JSON 
# and ignore the attribute list 
attributeOverride = "NO"
#attributeOverride = "ALL"

# Only cache active billing plans option
onlyCacheActiveBillingPlans = True

dbRegion = "us-west-1"
#dbName = "test1-productCatalog"
#envId = "100"

def lambda_handler(event, context):
    log.info('START_LAMBDA')
    log.info(event)
    login = event['body']['credential']
    envId = event['body']['environment']
    dbName = event['body']['cacheName']

    if onlyCacheActiveBillingPlans == True: 
       print "Only Caching ACTIVE Billing Plans...."
    else: 
       print "Caching all Billing Plans...."
    
    ## Check to see if that table exists in that region 
    ## If it does not exist, attempt to create it
    client = boto3.client('dynamodb', region_name=dbRegion)
    clientTablesList = client.list_tables()
    tables = clientTablesList['TableNames']
   
    if dbName in tables: 
       print "Loading Billing Plan Cache {} in Region {}".format(dbName, dbRegion) 
    else: 
       #print "Missing {} in Region {}".format(dbName, dbRegion) 
       print "Creating Billing Plan Cache Table {} in Region {} and Environment {}".format(dbName, dbRegion, envId) 
       client.create_table(
           AttributeDefinitions=[
               {
                   'AttributeName': 'merchantBillingPlanId',
                   'AttributeType': 'S'
               },
               {           
                   'AttributeName': 'environmentId',
                   'AttributeType': 'S'
               }
           ],
           TableName=dbName,
           KeySchema=[
               {
                   'AttributeName': 'merchantBillingPlanId',
                   'KeyType': 'HASH'
               },
               {
                   'AttributeName': 'environmentId',
                   'KeyType': 'RANGE'
               }
           ],
           ProvisionedThroughput={
               'ReadCapacityUnits': 5,
               'WriteCapacityUnits': 5
           },
       )
       # Wait until the table exists.
       # Wait until the table exists.
       waiter = client.get_waiter('table_exists')
       waiter.wait(TableName=dbName, 
           WaiterConfig={
               'Delay': 1,
               'MaxAttempts': 50
           }
       )
   
    # Encode the login and password for REST 
    login64 = base64.b64encode(b'' + login)
    
    # Set the CashBox URL target 
    url = "https://api.prodtest.vindicia.com"
    
    # Set how many products to fetch at a time from the command line parameters
    command = '/billing_plans?limit=100'
    
    # Initial some of the variables needed to do the fetch and db load 
    fetchComplete = False
    totalNumberOfBillingPlansFetched = 1
    batchList = {}
    
    while fetchComplete != True:
       target = url + command 
       #print "Fetch-> ", target
       # Prepare to make the first request where we find out how many products there are in total 
       response = StringIO.StringIO()
       c = pycurl.Curl() 
       c.setopt(c.URL, target)
       c.setopt(c.WRITEFUNCTION, response.write)
       c.setopt(c.HTTPHEADER, ['Authorization: Basic ' + login64, 'Content-Type: application/json'])

       # Turn on to see more of the action behind the scenes 
       c.setopt(c.VERBOSE, False)

       # Execute the REST API 
       c.perform()

       # Close the Channel 
       c.close()

       # Parse the response from the REST API call 
       event =  json.loads(response.getvalue())

       if 'next' in event: 
          command = event['next']
          numberOfBillingPlansFetched = 0
       else: 
          break
      
       if 'total_count' in event:
          billingPlanCount = event['total_count']
          numberOfBillingPlansFetched += len(event['data'])
      
          x = 0
          billingPlanCounter = 1
          while billingPlanCounter <= numberOfBillingPlansFetched:
             merchantBillingPlanId = event['data'][x]['id']
             merchantBillingPlanStatus = event['data'][x]['status']     

             # Check if the option to only cache active products is set
             if (onlyCacheActiveBillingPlans == True and merchantBillingPlanStatus == "Active") or (onlyCacheActiveBillingPlans == False):
         
                if 'description' in event['data'][x]:
                   description = event['data'][x]['description']
                else: 
                   description = None   

                if 'vid' in event['data'][x]:
                   vid = event['data'][x]['vid']
                else: 
                   vid = None                  
                                  
                if 'created' in event['data'][x]:
                   created = event['data'][x]['created']
                else: 
                   created = None 
                                       
                if 'end_of_life' in event['data'][x]:
                   endOfLifeTimestamp = event['data'][x]['end_of_life']
                else: 
                   endOfLifeTimestamp = None    

                if 'times_to_run' in event['data'][x]:
                   timesToRun = event['data'][x]['times_to_run']
                else: 
                   timesToRun = None    

                if 'billing_descriptor' in event['data'][x]:
                   billingDescriptor = event['data'][x]['billing_descriptor']
                else: 
                   billingDescriptor = None   

                if 'used_on_subscriptions' in event['data'][x]:
                   usedOnSubscriptions = event['data'][x]['used_on_subscriptions']
                else: 
                   usedOnSubscriptions = None  

                # Deal with the error regarding amounts as decimal from DynamoDB 
                if 'periods' in event['data'][x]:
                   periods = event['data'][x]['periods']['data']
                   p = 0
                   periodCounter = event['data'][x]['periods']['total_count']
                   while p < periodCounter:                      
                      if 'prices' in event['data'][x]['periods']['data'][p]: 
                         pp = 0
                         priceCounter = event['data'][x]['periods']['data'][p]['prices']['total_count']
                         while pp < priceCounter: 
                            price = event['data'][x]['periods']['data'][p]['prices']['data'][pp]['amount']
                            newPrice = Decimal(str(price))
                            event['data'][x]['periods']['data'][p]['prices']['data'][pp]['amount'] = newPrice
                            pp += 1
                      p += 1
                else: 
                   periods = None              

                # Check to see if there are any entitlements              
                if 'entitlements' in event['data'][x]:
                   entitlements = event['data'][x]['entitlements']['data']
                else: 
                   entitlements = None   
            
                # Check to see if there are any metadata
                if 'metadata' in event['data'][x]:
                   metadata = event['data'][x]['metadata']
                else: 
                   metadata = None         
                   
                # prepare the batch of items based on attributeCache settings  
                item = {'merchantBillingPlanId': merchantBillingPlanId}
                item['environmentId'] = envId
         
                # Only load the data explicitly placed in the attributeCache list 
                if 'status' in attributeCache: 
                   item['status'] = merchantBillingPlanStatus       

                if 'description' in attributeCache: 
                   item['descriptions'] = description
          
                if 'vid' in attributeCache: 
                   item['vid'] = vid
        
                if 'created' in attributeCache: 
                   item['created'] = created
 
                if 'end_of_life' in attributeCache: 
                   item['end_of_life'] = endOfLifeTimestamp               

                if 'times_to_run' in attributeCache: 
                   item['times_to_run'] = timesToRun

                if 'billing_descriptor' in attributeCache: 
                   item['billing_descriptor'] = billingDescriptor
         
                if 'periods' in attributeCache: 
                   item['periods'] = periods
            
                if 'entitlements' in attributeCache: 
                   item['entitlements'] = entitlements
               
                if 'metadata' in attributeCache: 
                   item['metadata'] = metadata               
            
                if 'used_on_subscriptions' in attributeCache: 
                   item['used_on_subscriptions'] = usedOnSubscriptions             
            
                # Check to see if the ALL value is set to override 
                if attributeOverride == 'ALL':
                   item = {}
                   item = {'merchantBillingPlanId': merchantBillingPlanId}
                   item = {'environmentId': envId} 
                   item['attributes'] = event['data'][x]      

                # Add an item to the table
                # print item
                batchList['item' + str(totalNumberOfBillingPlansFetched)] = item 
         
             # Increment the counters and flush the lists 
             totalNumberOfBillingPlansFetched += 1
             billingPlanCounter += 1
             x += 1
             status = ""
             description = ""
             vid = ""
             created = ""
             endOfLifeTimestamp = ""
             timesToRun = ""
             billingDescriptor = ""
             periods = ""
             entitlements = ""         
             metadata = ""
             usedOnSubscriptions = ""
             merchantBillingPlanStatus = ""
             merchantBillingPlanId = ""
             item = {}
             
             if totalNumberOfBillingPlansFetched == billingPlanCount: 
                fetchComplete = True        

       # Close the response channel 
       response.close()
       
       # Load the database in batch mode 
       dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
       table = dynamodb.Table(dbName)  
       with table.batch_writer() as batch:
           for key in batchList:
               batch.put_item(Item=batchList[key])
               
    return {
        "statusCode": 200,
        "billingPlanCacheName": dbName,
        "billingPlanCacheRegion" : dbRegion,
        "environmentId" : envId
    }

