#!/bin/bash
# Delete all timeseries points

generate_json_payload_data()
{
DATETIME_NOW=$(date +"%Y-%m-%dT%H:%M:%SZ")
cat <<EOF
{
  "start": "2022-01-01T00:00:00Z",
  "stop": "$DATETIME_NOW",
  "predicate": "_measurement=\"exchange-rate\""
}
EOF
}

curl --request POST \
  "http://localhost:8086/api/v2/delete?org=persuader-technology&bucket=automata"  \
  --header "Authorization: Token q3cfJCCyfo4RNJuyg72U-3uEhrv3qkKQcDOesoyeIDg2BCUpmn-mjReqaGwO7GOebhd58wYVkopi5tcgCj8t5w==" \
  --header "Content-Type: application/json" \
  --data "$(generate_json_payload_data)"
