#!/usr/bin/env python
##########################################################################################
##
##     NAME: entitlementsCacheBuilder.py 
##   AUTHOR: Liam Maxwell 
##     DATE: 2018-09-20
##  VERSION: 1.0
##  PURPOSE: Loop through all the accounts in the sandbox get their entitlement history 
##           and write to a AWS DynamoDB table.  
##
##           Edit the login variable with your rest username and password 
##
##########################################################################################
# Import the libraries that we need for this to work 
import pycurl
import StringIO
import base64
import sys
import json
import boto3
import datetime 
import pytz
from decimal import Decimal


start_ts = datetime.datetime.now()
timezone = pytz.timezone("America/Chicago")
start_ts_aware = timezone.localize(start_ts)


# Your CashBox username and password "username:password"
login = "username:password"

# Checking the command line parameters to make sure they are sending the right number or variables 
if (len(sys.argv) < 2): 
	print "ERROR: Wrong Number of Parameters: ", len(sys.argv)
	print "Usage => ", sys.argv[0], " cacheName"
	print "Example => ", sys.argv[0], " entitlementsCache"
	exit(0)

cacheRegion = "us-west-1"
cacheName = sys.argv[1]

## Check to see if that table exists in that region 
## If it does not exist, attempt to create it
client = boto3.client('dynamodb', region_name=cacheRegion)
clientTablesList = client.list_tables()
tables = clientTablesList['TableNames']

if cacheName in tables: 
   print "Loading Cache {} in Region {}".format(cacheName, cacheRegion) 
else: 
   print "Creating Cache {} in Region {} ".format(cacheName, cacheRegion)    
   client.create_table(
       AttributeDefinitions=[
           {
               'AttributeName': 'merchantAccountId',
               'AttributeType': 'S'
           },
           {           
               'AttributeName': 'merchantEntitlementId',
               'AttributeType': 'S'
           }
       ],
       TableName=cacheName,
       KeySchema=[
           {
               'AttributeName': 'merchantAccountId',
               'KeyType': 'HASH'
           },
           {
               'AttributeName': 'merchantEntitlementId',
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
   waiter.wait(TableName=cacheName, 
        WaiterConfig={
              'Delay': 1,
              'MaxAttempts': 50
           }
        )
   
# Call the CashBox REST API and loop through the products 
# If a product status is active, load it into json and the db 

# Encode the login and password for REST 
login64 = base64.b64encode(b'' + login)

# Set the CashBox URL target 
url = "https://api.prodtest.vindicia.com"

# Set how many products to fetch at a time from the command line parameters
batchSize = 1
accountCommand = "/accounts?limit=" + str(batchSize)


# Initial some of the variables needed to do the fetch and db load 
fetchComplete = False
totalNumberOfAccountsFetched = 1
numberOfAccountsFetched = 0
entitlementCounter = 0
totalNumberOfEntitlements = 0

while fetchComplete != True:
   target = url + accountCommand 
   #print "Fetch-> ", target
   # Prepare to make the first request where we find out how many products there are in total 
   accountResponse = StringIO.StringIO()
   c = pycurl.Curl() 
   c.setopt(c.URL, target)
   c.setopt(c.WRITEFUNCTION, accountResponse.write)
   c.setopt(c.HTTPHEADER, ['Authorization: Basic ' + login64, 'Content-Type: application/json'])

   # Turn on to see more of the action behind the scenes 
   c.setopt(c.VERBOSE, False)

   # Execute the REST API 
   c.perform()

   # Close the Channel 
   c.close()

   # Parse the response from the REST API call 
   account =  json.loads(accountResponse.getvalue())

   # In case we have multiple fetches grab the next command off the event 
   if 'next' in account: 
      accountCommand = account['next']
   else: 
      break
      print "Break Out"
      
   if 'total_count' in account:
      accountCount = account['total_count']
      numberOfAccountsFetched = len(account['data'])
      
      x = 0
      accountCounter = 1
      while accountCounter <= numberOfAccountsFetched:

         merchantAccountId = account['data'][x]['id']
         entitlementCommand = "/entitlements?account=" + merchantAccountId + "&show_all=1"
         target = url + entitlementCommand
         entitlementResponse = StringIO.StringIO()
         c = pycurl.Curl() 
         c.setopt(c.URL, target)
         c.setopt(c.WRITEFUNCTION, entitlementResponse.write)
         c.setopt(c.HTTPHEADER, ['Authorization: Basic ' + login64, 'Content-Type: application/json'])
         # Turn on to see more of the action behind the scenes 
         c.setopt(c.VERBOSE, False)

         # Execute the REST API 
         c.perform()

         # Close the Channel 
         c.close()

         # Parse the response from the REST API call 
         entitlement =  json.loads(entitlementResponse.getvalue())
         #print "ENTITLEMENT" 
         #print json.dumps(entitlement)
         
         if 'total_count' in entitlement: 
            entitlementCount = entitlement['total_count']
            entitlementCounter = 1
            
            y = 0
            while entitlementCounter <= entitlementCount:
               
               # Need to check for the presence of some attributes that may be missing 
               # depending on what kind of entitlement this is.  For example a billing 
               # plan based entitlement will not send through a product or autobill item
               # whereas a product based entitlement will
               # Determine what kind of entitlement this is and set the attributes accordingly 
               if 'subscription' in entitlement['data'][y]:
                   merchantAutoBillId = entitlement['data'][y]['subscription']['id']
               else: 
                   merchantAutoBillId = None
                   
               if 'billing_plan' in entitlement['data'][y]:
                   billingPlanId = entitlement['data'][y]['billing_plan']['id']
                   entitlementSource = "BillingPlan"
               else: 
                   billingPlanId = None
                   entitlementSource = None
               
               if 'subscription_item' in entitlement['data'][y]:
                  
                  if 'id' in entitlement['data'][y]['subscription_item']:
                     merchantAutoBillItemId = entitlement['data'][y]['subscription_item']['id']
                  else: 
                     merchantAutoBillItemId = None
                     
                  autoBillItemVid = entitlement['data'][y]['subscription_item']['vid']
                  merchantProductId = entitlement['data'][y]['product']['id']
                  entitlementSource = "Product"
               else: 
                  merchantAutoBillItemId = None
                  autoBillItemVid = None
                  merchantProductId = None
                  entitlementSource = None
                  
               
               # Set status on, and turn it off if needed 
               entitlementStatus = 1
               entitlementAction = "start"
               if entitlement['data'][y]['active'] == False:
                  entitlementStatus = 0
                  entitlementAction = "stop" 
               
               # Generate the item for the cache 
               # Set the time for the status log update 
               # 
               # Only process product level entitlements 
               if entitlementSource  == 'Product': 
                  totalNumberOfEntitlements += 1 
                  ent_start_ts = datetime.datetime.now()
                  ent_start_ts_aware = timezone.localize(ent_start_ts)
                  ent_start_ts_aware = ent_start_ts_aware.strftime("%Y-%m-%dT%H:%M:%S%z")
                  item = {'merchantAccountId': merchantAccountId}
                  item['merchantEntitlementId'] = entitlement['data'][y]['id'] 
                  item['entitled'] = entitlementStatus 
                  item['status_log'] = {
                     "data": [
                        {
                           "action": entitlementAction,
                           "merchantAutoBillId": merchantAutoBillId,
                           #"merchantBillingPlanId": billingPlanId,
                           "merchantAutoBillItemId": merchantAutoBillItemId,
                           "entitlementSource": entitlementSource,
                           "merchantProductId": merchantProductId, 
                           "autoBillItemVid": autoBillItemVid, 
                           "startTimestamp": entitlement['data'][y]['starts'],
                           "endTimestamp": entitlement['data'][y]['ends'],
                           "eventTimestamp": entitlement['data'][y]['ends'],
                           "messageId": "CloudCacheBuilder-Initiated-Log",
                           "cacheEntryTimestamp": ent_start_ts_aware,
                           "source": "cacheBuilder"
                        }
                      ],
                      "totalCount": 1
                   }

                  # Add an item to the table
                  dynamodb = boto3.resource('dynamodb', region_name=cacheRegion)
                  table = dynamodb.Table(cacheName)
                  ret = table.put_item(Item=item) 
                  print "#:", totalNumberOfAccountsFetched, "Account --> ", merchantAccountId, "Entitlement --> ", entitlement['data'][y]['id'], " : ", entitlementStatus

                  y += 1
                  entitlementCounter += 1
            
         # Check to see if we are done processing  
         if totalNumberOfAccountsFetched == accountCount: 
             fetchComplete = True
             break
            
         # There are still records to process to refresh the vars 
         else: 
             # Increment the counters and flush the lists 
             totalNumberOfAccountsFetched += 1
             accountCounter += 1
             x += 1
             descriptions = ""
             prices = ""
             metadata = ""
             credits = ""
             entitlements = ""
             created = ""
             endOfLifeTimestamp = ""
             merchantAccountId = ""
             entitlement = ""
         
# Close the response channel 
accountResponse.close()
entitlementResponse.close()
end_ts = datetime.datetime.now()
duration = end_ts - start_ts
print "    Start Time: ", start_ts
print "      End Time: ", end_ts
print "      Duration: ", duration 
print "  Entitlements: ", totalNumberOfEntitlements
