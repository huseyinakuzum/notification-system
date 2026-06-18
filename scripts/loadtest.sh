#!/usr/bin/env bash
# Configurable randomized load generator for the notification system.
#
# Forks N worker loops that POST notifications forever (or for a fixed duration),
# randomizing channel, priority, batch size, and a scheduled fraction so every
# lane and the scheduler -> CDC flip path stay exercised.
#
# All knobs are flags (with env fallbacks). Run with -h for the list.
#
#   scripts/loadtest.sh                  # 1 worker, ~0.2-0.7s pace, forever
#   scripts/loadtest.sh -w 4 -d 120      # 4 workers for 120s
#   scripts/loadtest.sh -w 8 -m 0 -M 1   # 8 workers near flat-out
#   scripts/loadtest.sh -b 1 -B 20 -s 80 # batches up to 20, 80% scheduled
set -u

API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
WORKERS="${WORKERS:-1}"
DURATION="${DURATION:-0}"          # seconds; 0 = run until interrupted
MIN_SLEEP_DS="${MIN_SLEEP_DS:-2}"  # deciseconds (tenths of a second)
MAX_SLEEP_DS="${MAX_SLEEP_DS:-7}"
BATCH_MIN="${BATCH_MIN:-2}"
BATCH_MAX="${BATCH_MAX:-8}"
SINGLE_PCT="${SINGLE_PCT:-60}"     # % of requests that are single (rest batched)
SCHED_PCT="${SCHED_PCT:-25}"       # % of items scheduled into the near future
CHANNELS="${CHANNELS:-sms,email,push}"
PRIORITIES="${PRIORITIES:-high,normal,low}"

usage() {
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  cat <<EOF

Flags (env fallback in parens):
  -u URL   API base URL (API_BASE_URL)            [$API_BASE_URL]
  -w N     parallel workers (WORKERS)             [$WORKERS]
  -d SECS  run duration, 0=forever (DURATION)     [$DURATION]
  -m DS    min sleep, deciseconds (MIN_SLEEP_DS)  [$MIN_SLEEP_DS]
  -M DS    max sleep, deciseconds (MAX_SLEEP_DS)  [$MAX_SLEEP_DS]
  -b N     min batch size (BATCH_MIN)             [$BATCH_MIN]
  -B N     max batch size (BATCH_MAX)             [$BATCH_MAX]
  -s PCT   % single sends, rest batched (SINGLE_PCT) [$SINGLE_PCT]
  -S PCT   % items scheduled (SCHED_PCT)          [$SCHED_PCT]
  -c LIST  comma-separated channels (CHANNELS)    [$CHANNELS]
  -p LIST  comma-separated priorities (PRIORITIES)[$PRIORITIES]
  -h       this help
EOF
}

while getopts "u:w:d:m:M:b:B:s:S:c:p:h" opt; do
  case "$opt" in
    u) API_BASE_URL="$OPTARG" ;;
    w) WORKERS="$OPTARG" ;;
    d) DURATION="$OPTARG" ;;
    m) MIN_SLEEP_DS="$OPTARG" ;;
    M) MAX_SLEEP_DS="$OPTARG" ;;
    b) BATCH_MIN="$OPTARG" ;;
    B) BATCH_MAX="$OPTARG" ;;
    s) SINGLE_PCT="$OPTARG" ;;
    S) SCHED_PCT="$OPTARG" ;;
    c) CHANNELS="$OPTARG" ;;
    p) PRIORITIES="$OPTARG" ;;
    h) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

API="$API_BASE_URL/notifications"
IFS=',' read -r -a chans <<< "$CHANNELS"
IFS=',' read -r -a prios <<< "$PRIORITIES"
SLEEP_SPAN=$(( MAX_SLEEP_DS - MIN_SLEEP_DS + 1 )); [ "$SLEEP_SPAN" -lt 1 ] && SLEEP_SPAN=1
BATCH_SPAN=$(( BATCH_MAX - BATCH_MIN + 1 )); [ "$BATCH_SPAN" -lt 1 ] && BATCH_SPAN=1

rnd() { echo $(( RANDOM % $1 )); }

item() {
  local ch=${chans[$(rnd ${#chans[@]})]}
  local pr=${prios[$(rnd ${#prios[@]})]}
  local sched=""
  if [ "$(rnd 100)" -lt "$SCHED_PCT" ]; then
    local secs=$(( 3 + RANDOM % 10 ))
    local at
    at=$(date -u -v+${secs}S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "+${secs} seconds" +%Y-%m-%dT%H:%M:%SZ)
    sched=",\"scheduled_at\":\"$at\""
  fi
  printf '{"recipient":"+1555%05d","channel":"%s","content":"load-%d","priority":"%s"%s}' \
    "$((RANDOM%100000))" "$ch" "$RANDOM" "$pr" "$sched"
}

worker() {
  local id=$1 count=0
  while true; do
    local body
    if [ "$(rnd 100)" -lt "$SINGLE_PCT" ]; then
      body=$(item)
    else
      local n=$(( BATCH_MIN + RANDOM % BATCH_SPAN )) body="[" j
      for ((j=0;j<n;j++)); do [ "$j" -gt 0 ] && body+=","; body+=$(item); done
      body+="]"
    fi
    curl -s -o /dev/null -X POST "$API" -H 'Content-Type: application/json' -d "$body"
    count=$((count+1))
    [ $((count % 50)) -eq 0 ] && echo "$(date -u +%H:%M:%S) w$id sent=$count"
    sleep 0.$(( MIN_SLEEP_DS + RANDOM % SLEEP_SPAN ))
  done
}

pids=()
for ((w=1;w<=WORKERS;w++)); do worker "$w" & pids+=("$!"); done
echo "started $WORKERS worker(s) -> $API (duration=${DURATION}s, pace=${MIN_SLEEP_DS}-${MAX_SLEEP_DS}ds)"

cleanup() { kill "${pids[@]}" 2>/dev/null; }
trap cleanup INT TERM EXIT

if [ "$DURATION" -gt 0 ]; then
  sleep "$DURATION"
else
  wait
fi
