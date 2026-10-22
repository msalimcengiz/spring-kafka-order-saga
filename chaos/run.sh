#!/usr/bin/env bash
# Chaos test for the order saga.
#
# Places orders for DURATION seconds while SIGKILLing service instances every few
# seconds and the Kafka broker once, with a share of handler attempts failing on
# purpose. Then waits for every saga to finish and checks, across the three databases:
#   - no order is left in a pending state
#   - every acknowledged order exists
#   - each order's payment matches its outcome (charged / refunded / declined)
#   - money is conserved: balance = 5000 - sum(completed orders) for every customer
#   - stock is conserved: available = 100 - sum(completed quantities) for every product
#   - nothing is in a dead-letter topic
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-90}
RESULTS="$PWD/chaos/results"
mkdir -p "$RESULTS"
export FAULT_RATE=${FAULT_RATE:-0.05} SAGA_STUCK_AFTER=PT30S

sql() { docker compose exec -T postgres psql -U saga -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait >/dev/null
echo "stack up; load for ${DURATION}s with FAULT_RATE=$FAULT_RATE"

docker run --rm --network saga_default -v "$PWD/chaos:/scripts:ro" \
  -e DURATION="${DURATION}s" grafana/k6:1.2.3 run --quiet /scripts/load.js \
  2> >(grep -oE '(ACK|GAVEUP) [0-9a-f-]+' > "$RESULTS/acks.txt") &
K6=$!

kills=0
broker_killed=no
end=$((SECONDS + DURATION - 10))
while [ $SECONDS -lt $end ]; do
  sleep 5
  for svc in order-service payment-service inventory-service; do
    if [ $((RANDOM % 2)) -eq 0 ]; then
      ids=($(docker compose ps -q "$svc"))
      victim=${ids[$((RANDOM % ${#ids[@]}))]}
      docker kill -s KILL "$victim" >/dev/null && kills=$((kills + 1))
      ( sleep 2; docker start "$victim" >/dev/null 2>&1 || true ) &
    fi
  done
  if [ "$broker_killed" = no ] && [ $SECONDS -gt $((end - DURATION / 2)) ]; then
    echo "killing the Kafka broker"
    docker compose kill -s KILL kafka >/dev/null
    sleep 5
    docker compose start kafka >/dev/null
    broker_killed=yes
  fi
done
wait $K6 || true
wait
echo "load finished; SIGKILLed $kills service instances and the broker once"

# Make sure everything is running again, then wait for every saga to reach a final state.
docker compose up -d --wait >/dev/null
for _ in $(seq 1 180); do
  pending=$(sql orders "SELECT count(*) FROM orders WHERE status NOT IN ('COMPLETED', 'CANCELLED')")
  [ "$pending" = "0" ] && break
  sleep 2
done

# Copy the order outcomes into the payments and inventory databases to check them there.
sql orders "COPY (SELECT id, customer_id, product_id, quantity, amount, status, coalesce(cancel_reason, '') FROM orders) TO STDOUT" > "$RESULTS/orders.tsv"
for db in payments inventory; do
  sql "$db" "DROP TABLE IF EXISTS o; CREATE TABLE o (id uuid, customer_id text, product_id text, quantity int, amount numeric, status text, reason text)" >/dev/null
  docker compose exec -T postgres psql -U saga -d "$db" -qc "\\copy o FROM STDIN" < "$RESULTS/orders.tsv"
done
grep '^ACK ' "$RESULTS/acks.txt" | cut -d' ' -f2 | sort -u > "$RESULTS/acked.txt"
sql orders "DROP TABLE IF EXISTS acked; CREATE TABLE acked (id uuid PRIMARY KEY)" >/dev/null
docker compose exec -T postgres psql -U saga -d orders -qc "\\copy acked FROM STDIN" < "$RESULTS/acked.txt"

acked=$(wc -l < "$RESULTS/acked.txt" | tr -d ' ')
gave_up=$(grep -c '^GAVEUP ' "$RESULTS/acks.txt" || true)
total=$(sql orders "SELECT count(*) FROM orders")
completed=$(sql orders "SELECT count(*) FROM orders WHERE status = 'COMPLETED'")
declined=$(sql orders "SELECT count(*) FROM orders WHERE cancel_reason LIKE 'payment_declined%'")
out_of_stock=$(sql orders "SELECT count(*) FROM orders WHERE cancel_reason LIKE 'out_of_stock%'")
pending=$(sql orders "SELECT count(*) FROM orders WHERE status NOT IN ('COMPLETED', 'CANCELLED')")
missing=$(sql orders "SELECT count(*) FROM acked a LEFT JOIN orders o USING (id) WHERE o.id IS NULL")

payment_mismatch=$(sql payments "
  SELECT count(*) FROM o LEFT JOIN payments p ON p.order_id = o.id
  WHERE NOT (
       (o.status = 'COMPLETED' AND p.status = 'CHARGED' AND p.amount = o.amount)
    OR (o.reason LIKE 'out_of_stock%' AND p.status = 'REFUNDED')
    OR (o.reason LIKE 'payment_declined%' AND p.status = 'DECLINED'))")
money_drift=$(sql payments "
  SELECT count(*) FROM accounts a
  WHERE a.balance <> 5000 - coalesce((SELECT sum(amount) FROM o WHERE o.customer_id = a.customer_id AND o.status = 'COMPLETED'), 0)")
stock_drift=$(sql inventory "
  SELECT count(*) FROM products p
  WHERE p.available <> 100 - coalesce((SELECT sum(quantity) FROM o WHERE o.product_id = p.id AND o.status = 'COMPLETED'), 0)")
reservation_mismatch=$(sql inventory "
  SELECT count(*) FROM reservations r JOIN o ON o.id = r.order_id
  WHERE r.status = 'RESERVED' AND o.status <> 'COMPLETED'")

dlt=0
for t in payments.commands payments.events inventory.commands inventory.events orders.events; do
  n=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t.DLT" 2>/dev/null \
      | awk -F: '{s += $3} END {print s + 0}')
  dlt=$((dlt + n))
done

ok=yes
for v in "$pending" "$missing" "$payment_mismatch" "$money_drift" "$stock_drift" "$reservation_mismatch" "$dlt"; do
  [ "$v" = "0" ] || ok=no
done

{
  echo "Load: 20 VUs for ${DURATION}s. SIGKILLed $kills service instances (order, 2× payment, 2× inventory) and the Kafka broker once; ${FAULT_RATE} of handler attempts failed on purpose."
  echo
  echo "| orders | completed | declined (no money) | out of stock (refunded) | acknowledged | client gave up |"
  echo "|---:|---:|---:|---:|---:|---:|"
  echo "| $total | $completed | $declined | $out_of_stock | $acked | $gave_up |"
  echo
  echo "| check | violations |"
  echo "|---|---:|"
  echo "| sagas not finished | **$pending** |"
  echo "| acknowledged orders missing | **$missing** |"
  echo "| payment does not match the order's outcome | **$payment_mismatch** |"
  echo "| customers whose balance ≠ 5000 − completed orders | **$money_drift** |"
  echo "| products whose stock ≠ 100 − completed quantities | **$stock_drift** |"
  echo "| stock reserved for an order that did not complete | **$reservation_mismatch** |"
  echo "| records in dead-letter topics | **$dlt** |"
  echo
  [ "$ok" = yes ] && echo "**PASS**" || echo "**FAIL**"
} | tee "$RESULTS/chaos.md"
[ "$ok" = yes ]
