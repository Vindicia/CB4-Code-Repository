import logging 
from hashlib import sha256
import base64
import hmac

def create_signed_token(secret_key, string):
    string_to_sign = string.encode('utf-8')
    hashed = hmac.new(secret_key,string_to_sign, sha256)
    return hashed.hexdigest()

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)

# This is a working Lambda function that takes the merchantPaymentMethodId and HMAC as input 
# and returns a Payment Method Token 
def lambda_handler(event, context):
    log.info(event)
    
    # The json.body should contain the vin_session_id as the merchantPaymentMethodId 
    merchantPaymentMethodId = event['body']['vin_session_id']
    
    # The json.body should contain the vin_session_hash as the HMAC key 
    hmac_key = bytes(event['body']['vin_session_hash']).encode("utf-8")
    
    signature = '#POST#/payment_methods'
    token = merchantPaymentMethodId + signature
    encodedToken = bytes(token).encode("utf-8")
    signedToken = create_signed_token(hmac_key, encodedToken)
    return signedToken
