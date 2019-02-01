
#! /usr/bin/perl
use Digest::SHA qw(hmac_sha256_hex hmac_sha512_hex);

my $private_key = '6ea8ad9d0a330a0c62e8e868188b4871bb978d6b';  
my $token = LHM001002003;
my $one_time_token = "$token#POST#/payment_methods";
my $signed_one_time_token = hmac_sha256_hex($one_time_token, $private_key);
print "Token: $token\nFull Token Value: $one_time_token\nSigned: $signed_one_time_token\n";
