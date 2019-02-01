<script>
  var otl_hmac_key = "A39485F85039D394059B948390";
  var vin_session_id = "CC403930495";
  var vin_session_hash = CryptoJS.HmacSHA512(vin_session_id + "#POST#/payment_methods", otl_hmac_key);
</script>