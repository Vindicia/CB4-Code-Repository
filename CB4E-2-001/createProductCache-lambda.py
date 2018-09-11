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
attributeCache = {"status", "end_of_life", "created", "descriptions", "prices", "entitlements", "credits", "metadata", "vid"} 
#attributeCache = {"descriptions", "prices", "metadata"} 

# If the value of the override is equal to ALL then we just store the entire product JSON 
# and ignore the attribute list 
attributeOverride = "NO"
#attributeOverride = "ALL"

# Only cache active products option
onlyCacheActiveProducts = False

dbRegion = "us-west-1"
#dbName = "test1-productCatalog"
#envId = "100"

def lambda_handler(event, context):
    log.info('START_LAMBDA')
    log.info(event)
    login = event['body']['credential']
    envId = event['body']['environment']
    dbName = event['body']['cacheName']

    if onlyCacheActiveProducts == True: 
       print "Only Caching ACTIVE Products...."
    else: 
       print "Caching all Products...."
    
    ## Check to see if that table exists in that region 
    ## If it does not exist, attempt to create it
    client = boto3.client('dynamodb', region_name=dbRegion)
    clientTablesList = client.list_tables()
    tables = clientTablesList['TableNames']
   
    if dbName in tables: 
       print "Loading Product Cache {} in Region {}".format(dbName, dbRegion) 
    else: 
       #print "Missing {} in Region {}".format(dbName, dbRegion) 
       print "Creating Table {} in Region {} and Environment {}".format(dbName, dbRegion, envId) 
       client.create_table(
           TableName=dbName,
           AttributeDefinitions=[
               {
                   'AttributeName': 'merchantProductId',
                   'AttributeType': 'S'
               },
               {           
                   'AttributeName': 'environmentId',
                   'AttributeType': 'S'
               }
           ],
           KeySchema=[
               {
                   'AttributeName': 'merchantProductId',
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
    command = '/products?limit=100' 
    
    # Initial some of the variables needed to do the fetch and db load 
    fetchComplete = False
    totalNumberOfProductsFetched = 1
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
          numberOfProductsFetched = 0
       else: 
          break
      
       if 'total_count' in event:
          productCount = event['total_count']
          numberOfProductsFetched += len(event['data'])
      
          x = 0
          productCounter = 1
          while productCounter <= numberOfProductsFetched:
             merchantProductId = event['data'][x]['id']
             merchantProductStatus = event['data'][x]['status']     

             # Check if the option to only cache active products is set
             if (onlyCacheActiveProducts == True and merchantProductStatus == "Active") or (onlyCacheActiveProducts == False):
         
                if 'end_of_life' in event['data'][x]:
                   endOfLifeTimestamp = event['data'][x]['end_of_life']
                else: 
                   endOfLifeTimestamp = None    
            
                if 'created' in event['data'][x]:
                   created = event['data'][x]['created']
                else: 
                   created = None 
            
                # Check to see if there are any metadata, and if so, get the count
                # loop through them and capture the results 
                if 'metadata' in event['data'][x]:
                   metadata = event['data'][x]['metadata']
                else: 
                   metadata = None         
         
                # Check to see if there are any descriptions
                if 'descriptions' in event['data'][x]:
                   descriptions = event['data'][x]['descriptions']['data']
                else: 
                   descriptions = None
               
                # Check to see if there are any prices             
                # TODO Getting an error on the amounts being float - need to loop through and update them 
                # 'amount': Decimal(str(amount))
              
                if 'prices' in event['data'][x]:
                   p = 0
                   priceCounter = event['data'][x]['prices']['total_count']
                   while p < priceCounter: 
                      price = event['data'][x]['prices']['data'][p]['amount']
                      newPrice = Decimal(str(price))
                      event['data'][x]['prices']['data'][p]['amount'] = newPrice
                      p += 1
                  
                   prices = event['data'][x]['prices']['data']
                else: 
                   prices = None

               # Check to see if there are any credits               
                if 'credit_granted' in event['data'][x]:
                    credits = event['data'][x]['credit_granted']
                else: 
                   credits = None
               
                # Check to see if there are any entitlements, and if so, get the count
                # loop through them and capture the results                
                if 'entitlements' in event['data'][x]:
                   entitlements = event['data'][x]['entitlements']['data']
                else: 
                   entitlements = None        
        
                # Check to see if there are any metadata
                if 'metadata' in event['data'][x]:
                   metadata = event['data'][x]['metadata']  
            
                if 'vid' in event['data'][x]:
                   vid = event['data'][x]['vid']
                else: 
                   vid = None                 
            
                # prepare the item for the cache based on attributeCache dict 
                item = {'merchantProductId': merchantProductId}
                item['environmentId'] = envId
         
                # Only load the data explicitly placed in the attributeCache list 
                if 'descriptions' in attributeCache: 
                   item['descriptions'] = descriptions
         
                if 'prices' in attributeCache: 
                   item['prices'] = prices
        
                if 'metadata' in attributeCache: 
                   item['metadata'] = metadata
         
                if 'credits' in attributeCache: 
                   item['credits'] = credits
            
                if 'entitlements' in attributeCache: 
                   item['entitlements'] = entitlements
             
                if 'created' in attributeCache: 
                   item['created'] = created
            
                if 'end_of_life' in attributeCache: 
                   item['end_of_life'] = endOfLifeTimestamp
               
                if 'status' in attributeCache: 
                   item['status'] = merchantProductStatus       
            
                if 'vid' in attributeCache: 
                   item['vid'] = vid                
            
                # Check to see if the ALL value is set to override 
                if attributeOverride == 'ALL':
                   item = {}
                   item = {'merchantProductId': merchantProductId}
                   item = {'environmentId': envId} 
                   item['attributes'] = event['data'][x]      

                # Add an item to the batch list 
                batchList['item' + str(totalNumberOfProductsFetched)] = item 
         
             # Increment the counters and flush the lists 
             totalNumberOfProductsFetched += 1
             productCounter += 1
             x += 1
             descriptions = ""
             prices = ""
             metadata = ""
             credits = ""
             entitlements = ""
             created = ""
             endOfLifeTimestamp = ""
             merchantProductStatus = ""
             
             if totalNumberOfProductsFetched == productCount: 
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
        "productCacheName": dbName,
        "productCacheRegion" : dbRegion,
        "environmentId" : envId
    }
