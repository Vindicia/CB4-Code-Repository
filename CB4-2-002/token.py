#!/usr/bin/env python
import hashlib
import base64
import hmac

hmac_key = '6ea8ad9d0a330a0c62e8e868188b4871bb978d6b'
merchantPaymentMethodId = 'LHM001002003'
signature = '#POST#/payment_methods'

token = merchantPaymentMethodId + signature

signedToken = hmac.new(hmac_key, token, hashlib.sha256).hexdigest()

print "Token: " + merchantPaymentMethodId 
print "Full Token Value: " + token 
print "Signed: " + signedToken

# e5faf9894228ddda71f9c336de179f4b8c9faeadf6bf5363e17f219bddd005bb
