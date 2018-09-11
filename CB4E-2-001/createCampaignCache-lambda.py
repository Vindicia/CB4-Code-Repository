import pycurl
import StringIO
import base64
import json
import boto3
from decimal import Decimal
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)

# This list controls what attributes we put into the cache
# You can edit this list 
attributeCache = {"campaign_type", "offer_start_date", "cycles", "name", "vid", "promotion_code", "eligible_billing_plans", 
                  "promotion_code_aliases", "offer_end_date", "coupon_code_quantity", "percentage_discount", "state", 
                  "restrict_to_new_subscription", "eligible_products", "description", "flat_amount_discount"}

# If the value of the override is equal to ALL then we just store the entire product JSON 
# and ignore the attribute list 
attributeOverride = "NO"
#attributeOverride = "ALL"

# Only cache active campaigns option
onlyCacheActiveCampaigns = True

dbRegion = "us-west-1"
#dbName = "test1-productCatalog"
#envId = "100"

def lambda_handler(event, context):
    log.info('START_LAMBDA')
    log.info(event)
    login = event['body']['credential']
    envId = event['body']['environment']
    dbName = event['body']['cacheName']

    if onlyCacheActiveCampaigns == True: 
       print "Only Caching ACTIVE Campaigns...."
    else: 
       print "Caching all Campaigns...."
    
    ## Check to see if that table exists in that region 
    ## If it does not exist, attempt to create it
    client = boto3.client('dynamodb', region_name=dbRegion)
    clientTablesList = client.list_tables()
    tables = clientTablesList['TableNames']
   
    if dbName in tables: 
       print "Loading Campaign Cache {} in Region {}".format(dbName, dbRegion) 
    else: 
       #print "Missing {} in Region {}".format(dbName, dbRegion) 
       print "Creating Campaign Cache Table {} in Region {} and Environment {}".format(dbName, dbRegion, envId) 
       client.create_table(
           AttributeDefinitions=[
               {
                   'AttributeName': 'campaignId',
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
                   'AttributeName': 'campaignId',
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
    command = '/campaigns?limit=20'
    
    # Initial some of the variables needed to do the fetch and db load 
    fetchComplete = False
    totalNumberOfCampaignsFetched = 1
    
    while fetchComplete != True:
       target = url + command 
       print "Fetch-> ", target
       # Prepare to make the first request where we find out how many products there are in total 
       response = StringIO.StringIO()
       c = pycurl.Curl() 
       c.setopt(c.URL, str(target))
       c.setopt(c.WRITEFUNCTION, response.write)
       c.setopt(c.HTTPHEADER, ['Authorization: Basic ' + login64, 'Content-Type: application/json'])

       # Turn on to see more of the action behind the scenes 
       c.setopt(c.VERBOSE, False)

       # Execute the REST API 
       c.perform()

       # Close the Channel 
       c.close()

       # Parse the response from the REST API call 
       cashBoxEvent =  json.loads(response.getvalue())
       #log.info(cashBoxEvent)

       if 'next' in cashBoxEvent: 
          command = cashBoxEvent['next']
          numberOfCampaignsFetched = 0
       else: 
          break
      
       if 'total_count' in cashBoxEvent:
          campaignCount = cashBoxEvent['total_count']
          numberOfCampaignsFetched += len(cashBoxEvent['data'])
      
          x = 0
          campaignCounter = 1
          while campaignCounter <= numberOfCampaignsFetched:
             campaignId = cashBoxEvent['data'][x]['campaign_id']
             campaignState = cashBoxEvent['data'][x]['state']     

             # Check if the option to only cache active products is set
             if (onlyCacheActiveCampaigns == True and campaignState == "Active") or (onlyCacheActiveCampaigns == False):
             
                if 'campaign_type' in cashBoxEvent['data'][x]:
                   campaignType = cashBoxEvent['data'][x]['campaign_type']
                else: 
                   campaignType = None   
  
                if 'description' in cashBoxEvent['data'][x]:
                   description = cashBoxEvent['data'][x]['description']
                else: 
                   description = None   

                if 'vid' in cashBoxEvent['data'][x]:
                   vid = cashBoxEvent['data'][x]['vid']
                else: 
                   vid = None                  
                                  
                if 'offer_start_date' in cashBoxEvent['data'][x]:
                   startDate = cashBoxEvent['data'][x]['offer_start_date']
                else: 
                   startDate = None 
                                  
                if 'offer_end_date' in cashBoxEvent['data'][x]:
                   endDate = cashBoxEvent['data'][x]['offer_end_date']
                else: 
                   endDate = None 
                                       
                if 'cycles' in cashBoxEvent['data'][x]:
                   cycles = cashBoxEvent['data'][x]['cycles']
                else: 
                   cycles = None    

                if 'name' in cashBoxEvent['data'][x]:
                   name = cashBoxEvent['data'][x]['name']
                else: 
                   name = None    

                if 'promotion_code' in cashBoxEvent['data'][x]:
                   promotionCode = cashBoxEvent['data'][x]['promotion_code']
                else: 
                   promotionCode = None   

                if 'eligible_billing_plans' in cashBoxEvent['data'][x]:
                   eligibleBillingPlans = cashBoxEvent['data'][x]['eligible_billing_plans']
                else: 
                   eligibleBillingPlans = None   

                if 'eligible_products' in cashBoxEvent['data'][x]:
                   eligibleProducts = cashBoxEvent['data'][x]['eligible_products']
                else: 
                   eligibleProducts = None 

                if 'campaign_id' in cashBoxEvent['data'][x]:
                   campaignId = cashBoxEvent['data'][x]['campaign_id']
                else: 
                   campaignId = None   

                if 'promotion_code_aliases' in cashBoxEvent['data'][x]:
                   promotionCodeAliases = cashBoxEvent['data'][x]['promotion_code_aliases']
                else: 
                   promotionCodeAliases = None  

                if 'coupon_code_quantity' in cashBoxEvent['data'][x]:
                   couponCodeQuantity = cashBoxEvent['data'][x]['coupon_code_quantity']
                else: 
                   couponCodeQuantity = None  

                if 'percentage_discount' in cashBoxEvent['data'][x]:
                   percentageDiscount = cashBoxEvent['data'][x]['percentage_discount']
                else: 
                   percentageDiscount = None  

                # Deal with the error regarding amounts as decimal from DynamoDB 
                if 'flat_amount_discount' in cashBoxEvent['data'][x]:
                   price = cashBoxEvent['data'][x]['flat_amount_discount']['amount']
                   newPrice = Decimal(str(price))
                   cashBoxEvent['data'][x]['flat_amount_discount']['amount'] = newPrice
                   flatAmountDiscount = cashBoxEvent['data'][x]['flat_amount_discount']
                else: 
                   flatAmountDiscount = None              
           
                if 'restrict_to_new_subscription' in cashBoxEvent['data'][x]:
                   restrict = cashBoxEvent['data'][x]['restrict_to_new_subscription']
                else: 
                   restrict = None   
             
                # Load the database 
                #print "--ADD ITEM {} TO ENV {} CAMPAIGN CACHE :{}".format(totalNumberOfCampaignsFetched, envId, campaignId)  
                #print event['data'][x]
                dynamodb = boto3.resource('dynamodb', region_name=dbRegion)
                table = dynamodb.Table(dbName)

                item = {'campaignId': campaignId}
                item['environmentId'] = envId
         
                # Only load the data explicitly placed in the attributeCache list 
                if 'campaign_type' in attributeCache: 
                   item['campaign_type'] = campaignState       

                if 'description' in attributeCache: 
                   item['description'] = description   

                if 'vid' in attributeCache: 
                   item['vid'] = vid   

                if 'offer_start_date' in attributeCache: 
                   item['offer_start_date'] = startDate   

                if 'offer_end_date' in attributeCache: 
                   item['offer_end_date'] = endDate     

                if 'cycles' in attributeCache: 
                   item['cycles'] = cycles

                if 'name' in attributeCache: 
                   item['name'] = name         

                if 'promotion_code' in attributeCache: 
                   item['promotion_code'] = promotionCode   

                if 'eligible_billing_plans' in attributeCache: 
                   item['eligible_billing_plans'] = eligibleBillingPlans

                if 'promotion_code_aliases' in attributeCache: 
                   item['promotion_code_aliases'] = promotionCodeAliases

                if 'coupon_code_quantity' in attributeCache: 
                   item['coupon_code_quantity'] = couponCodeQuantity

                if 'percentage_discount' in attributeCache: 
                   item['percentage_discount'] = percentageDiscount               

                if 'flat_amount_discount' in attributeCache: 
                   item['flat_amount_discount'] = flatAmountDiscount    

                if 'restrict_to_new_subscription' in attributeCache: 
                   item['restrict_to_new_subscription'] = restrict   

                if 'eligible_products' in attributeCache: 
                   item['eligible_products'] = eligibleProducts  

                if 'state' in attributeCache: 
                   item['state'] = campaignState                

                # Check to see if the ALL value is set to override 
                if attributeOverride == 'ALL':
                   item = {}
                   item = {'campaignId': campaignId}
                   item = {'environmentId': envId} 
                   item['attributes'] = event['data'][x]      

                # Add an item to the table
                # print item
                ret = table.put_item(Item=item)
                
             #else: 
                 #print "---> SKIP ITEM {} Campaign {} with a status of {}".format(totalNumberOfCampaignsFetched, campaignId, campaignState)    
         
             # Increment the counters and flush the lists 
             totalNumberOfCampaignsFetched += 1
             campaignCounter += 1
             x += 1
             campaignType = ""
             description = ""
             vid = ""
             endDate = ""
             startDate = ""
             cycles = ""
             name = ""
             promotionCode = ""
             eligibleBillingPlans = ""
             eligibleProducts = ""
             campaignId = ""
             promotionCodeAliases = ""
             couponCodeQuantity = ""
             percentageDiscount = ""
             flatAmountDiscount = ""
             restrict = ""
             campaignState = ""
             
             if totalNumberOfCampaignsFetched == campaignCount: 
                fetchComplete = True        

       # Close the response channel 
       response.close()
   
    return {
        "statusCode": 200,
        "campaignCacheName": dbName,
        "campaignCacheRegion" : dbRegion,
        "environmentId" : envId
    }


