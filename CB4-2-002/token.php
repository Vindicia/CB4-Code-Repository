<?php

# This is effectively the merchantPaymentMethodId along with part of the URL string 
$message = 'LHM001002003#POST#/payment_methods';

# This is the merchant HMAC key 
$secret = '6ea8ad9d0a330a0c62e8e868188b4871bb978d6b';

print "php\n";

print hash_hmac('SHA256', $message, $secret) . "\n";

?>