#!/usr/bin/env bash
# Guided demo: builds and starts the three services, Kafka and Postgres, walks through
# the main behaviours and checks each one. Needs only Docker. The first run builds the
# images (a few minutes); later runs take about a minute.
set -euo pipefail
cd "$(dirname "$0")/.."

STATE="$PWD/.demo-state"   # inside the project: Docker Desktop/Colima only share the home directory
rm -rf "$STATE" && mkdir -p "$STATE" && echo 1 > "$STATE/next_step"
py() {
  docker run --rm --network saga_default -v "$PWD/demo:/demo:ro" -v "$STATE:/tmp" -w /demo \
    -e DEMO_STEP="$(cat "$STATE/next_step")" python:3.13-alpine python -u demo.py "$1"
}
quiet() { "$@" >/dev/null 2>&1; }
kafka() { docker compose exec -T kafka /opt/kafka/bin/"$@" 2>/dev/null; }

echo "Building and starting order, payment ×2, inventory ×2, Kafka and Postgres (fresh data)…"
quiet docker compose down -v
quiet docker compose up -d --build --wait
py basics

quiet docker compose stop payment-service
py outage_place
quiet docker compose start payment-service
quiet docker compose up -d --wait payment-service
py outage_resume

n=$(cat "$STATE/next_step")
echo
echo -e "\033[1m$n. A record that cannot be parsed goes straight to the dead-letter topic, without retries\033[0m"
kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.commands \
  --reader-property parse.key=true --reader-property key.separator='|' <<< "poison-key|{broken json"
for _ in $(seq 1 30); do
  dlt=$(kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic payments.commands.DLT | awk -F: '{s += $3} END {print s + 0}')
  [ "$dlt" -ge 1 ] && break; sleep 1
done
[ "$dlt" -ge 1 ] && echo -e "   \033[32m✓\033[0m payments.commands.DLT has $dlt record(s)" || { echo "   ✗ nothing in the DLT"; exit 1; }
echo $((n + 1)) > "$STATE/next_step"
py poison_check
rm -rf "$STATE"
echo "Explore: requests.http · orders http://localhost:8081 · payments :8082 · inventory :8084 · stop → make down"
