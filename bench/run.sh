#!/usr/bin/env bash
# Throughput and latency benchmark for the order saga (no faults injected).
#
#   1. max:    VUS clients post orders back to back for DURATION; measures how many orders
#              the API accepts and how many complete sagas per second the system finishes.
#   2. steady: each of RATES orders per second for DURATION; measures end-to-end saga latency
#              (POST accepted -> COMPLETED, through Kafka, payment and inventory) below saturation.
#
# End-to-end latency is updated_at - created_at of each COMPLETED order, both written by
# order-service's Postgres clock, so no cross-host clock skew is involved.
set -euo pipefail
cd "$(dirname "$0")/.."

VUS=${VUS:-64}
RATES=${RATES:-"100 500"}
DURATION=${DURATION:-60}
RESULTS="$PWD/bench/results"
mkdir -p "$RESULTS"
export FAULT_RATE=0

sql() { docker compose exec -T postgres psql -U saga -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
k6() {
  docker run --rm --network saga_default -v "$PWD/bench:/scripts:ro" -v "$RESULTS:/results" \
    -e MODE="$1" -e VUS="$VUS" -e RATE="${RATE:-100}" -e DURATION="${DURATION}s" \
    grafana/k6:1.2.3 run --quiet /scripts/load.js >/dev/null
}
drain() {
  for _ in $(seq 1 300); do
    [ "$(sql orders "SELECT count(*) FROM orders WHERE status NOT IN ('COMPLETED', 'CANCELLED')")" = 0 ] && return
    sleep 1
  done
  echo "sagas did not finish within 300 s" >&2
  exit 1
}
# Saga stats for orders created after $1 (a Postgres timestamp).
stats() {
  sql orders "
    SELECT count(*) FILTER (WHERE status = 'COMPLETED'),
           count(*) FILTER (WHERE status <> 'COMPLETED'),
           round((count(*) FILTER (WHERE status = 'COMPLETED')
                  / extract(epoch FROM max(updated_at) - min(created_at)))::numeric, 1),
           round(1000 * percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at))::numeric, 1),
           round(1000 * percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at))::numeric, 1),
           round(1000 * percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at))::numeric, 1)
    FROM orders WHERE created_at >= '$1'" | tr '|' ' '
}
http_stats() {  # accepted/s, p50, p95, p99 of POST /orders from the k6 summary
  python3 - "$RESULTS/$1.json" <<'EOF'
import json, sys
m = json.load(open(sys.argv[1]))["metrics"]
d = m["http_req_duration"]["values"]
ok = m["checks"]["values"]["passes"]
rate = m["http_reqs"]["values"]["rate"] * ok / m["http_reqs"]["values"]["count"]
print(f'{rate:.0f} {d["p(50)"]:.1f} {d["p(95)"]:.1f} {d["p(99)"]:.1f} {ok} {m["checks"]["values"]["fails"]}')
EOF
}

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait >/dev/null
sql payments "UPDATE accounts SET balance = 9999999" >/dev/null
sql inventory "UPDATE products SET available = 1000000000" >/dev/null
echo "stack up; warming up for 15 s"
DURATION=15 k6 max
drain

echo "max: $VUS clients for ${DURATION}s"
t0=$(sql orders "SELECT now()")
k6 max
drain
read -r max_done max_other max_tput max_p50 max_p95 max_p99 <<<"$(stats "$t0")"
read -r max_acc max_h50 max_h95 max_h99 max_ok max_fail <<<"$(http_stats max)"

steady_md=""
for RATE in $RATES; do
  echo "steady: $RATE orders/s for ${DURATION}s"
  t1=$(sql orders "SELECT now()")
  k6 steady
  cp "$RESULTS/steady.json" "$RESULTS/steady-$RATE.json"
  drain
  read -r st_done st_other st_tput st_p50 st_p95 st_p99 <<<"$(stats "$t1")"
  read -r st_acc st_h50 st_h95 st_h99 st_ok st_fail <<<"$(http_stats steady)"
  steady_md+="| steady ($RATE orders/s) | $st_ok | $st_acc | $st_h50 | $st_h95 | $st_h99 | $st_done | $st_other | $st_tput | **$st_p50** | **$st_p95** | $st_p99 |"$'\n'
done

{
  echo "No faults injected. order-service ×1, payment-service ×2, inventory-service ×2, Kafka ×1 (6 partitions), Postgres ×1."
  echo "Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), Docker VM with $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' | awk '{printf "%s %s %.0f GiB", $1, $2, $3/1073741824}')."
  echo
  echo "| run | orders accepted | accepted/s | POST p50 ms | POST p95 ms | POST p99 ms | sagas completed | not completed | completed sagas/s | saga p50 ms | saga p95 ms | saga p99 ms |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
  echo "| max ($VUS clients, closed loop) | $max_ok | $max_acc | $max_h50 | $max_h95 | $max_h99 | $max_done | $max_other | **$max_tput** | – | – | – |"
  printf '%s' "$steady_md"
  echo
  echo "Rejected POSTs in the max run: $max_fail. In the max run the API accepts orders faster than the sagas finish, so a backlog builds in the outbox and Kafka and is worked off afterwards (no order lost or left pending); saga latency there is mostly queueing time (p50 ${max_p50} ms) and is not shown as a latency figure. Saga latency = order created -> COMPLETED: outbox relay -> payment -> reply -> outbox relay -> inventory -> reply."
} | tee "$RESULTS/summary.md"
