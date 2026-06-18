#!/usr/bin/env bash
# Continuous randomized load generator for the notification system.
# Random channel, priority, and batch size; a fraction is scheduled for the near
# future to exercise the scheduler -> CDC flip path.
#
# Usage: scripts/loadtest.sh [min_sleep_ds] [max_sleep_ds]
#   min_sleep_ds / max_sleep_ds are deciseconds (tenths of a second) between
#   requests. Defaults to 2..7 (0.2s-0.7s). Lower them to push more load:
#   scripts/loadtest.sh 0 1   # near-flat-out
set -u

API="${API_BASE_URL:-http://localhost:8080}/notifications"
chans=(sms email push)
prios=(high normal low)

SLEEP_MIN=${1:-2}
SLEEP_SPAN=$(( ${2:-7} - SLEEP_MIN + 1 ))
[ "$SLEEP_SPAN" -lt 1 ] && SLEEP_SPAN=1

rnd() { echo $(( RANDOM % $1 )); }

item() {
  local ch=${chans[$(rnd 3)]}
  local pr=${prios[$(rnd 3)]}
  local sched=""
  # ~25% scheduled 3-12s in the future
  if [ "$(rnd 4)" -eq 0 ]; then
    local secs=$(( 3 + RANDOM % 10 ))
    local at
    at=$(date -u -v+${secs}S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "+${secs} seconds" +%Y-%m-%dT%H:%M:%SZ)
    sched=",\"scheduled_at\":\"$at\""
  fi
  printf '{"recipient":"+1555%05d","channel":"%s","content":"load-%d","priority":"%s"%s}' \
    "$((RANDOM%100000))" "$ch" "$RANDOM" "$pr" "$sched"
}

count=0
while true; do
  if [ "$(rnd 10)" -lt 6 ]; then
    body=$(item)                       # 60% single
  else
    n=$(( 2 + RANDOM % 7 ))            # 40% batch of 2-8
    body="["
    for ((j=0;j<n;j++)); do
      [ "$j" -gt 0 ] && body+=","
      body+=$(item)
    done
    body+="]"
  fi
  curl -s -o /dev/null -X POST "$API" -H 'Content-Type: application/json' -d "$body"
  count=$((count+1))
  [ $((count % 50)) -eq 0 ] && echo "$(date -u +%H:%M:%S) sent=$count"
  sleep 0.$(( SLEEP_MIN + RANDOM % SLEEP_SPAN ))
done
