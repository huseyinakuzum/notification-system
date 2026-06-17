#!/usr/bin/env bash
# Post an N-item batch to the API and print the batch_id.
# Usage: scripts/send.sh [count] [channel] [priority]
#   scripts/send.sh                # 10 email/normal
#   scripts/send.sh 1000           # 1000 email/normal (the batch ceiling)
#   scripts/send.sh 50 sms high    # 50 sms/high
set -euo pipefail

COUNT="${1:-10}"
CHANNEL="${2:-email}"
PRIORITY="${3:-normal}"
HOST="${API_BASE_URL:-http://localhost:8080}"

# Build the JSON array with jq so content stays valid and unique per item.
payload="$(jq -n \
  --argjson n "$COUNT" \
  --arg ch "$CHANNEL" \
  --arg pr "$PRIORITY" \
  '[range(0; $n) | {
      recipient: ("user-\(.)@example.com"),
      channel: $ch,
      content: ("load test item \(.) @ \(now|floor)"),
      priority: $pr
   }]')"

echo "POST $HOST/notifications  ($COUNT x $CHANNEL/$PRIORITY)" >&2
curl -sS -X POST "$HOST/notifications" \
  -H 'Content-Type: application/json' \
  -d "$payload" | jq .
