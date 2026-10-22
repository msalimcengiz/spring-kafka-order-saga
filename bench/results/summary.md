No faults injected. order-service ×1, payment-service ×2, inventory-service ×2, Kafka ×1 (6 partitions), Postgres ×1.
Machine: Apple M4, Docker VM with 4 CPUs, 8 GiB.

| run | orders accepted | accepted/s | POST p50 ms | POST p95 ms | POST p99 ms | sagas completed | not completed | completed sagas/s | saga p50 ms | saga p95 ms | saga p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| max (64 clients, closed loop) | 258686 | 4310 | 13.0 | 29.2 | 43.2 | 258686 | 0 | **833.3** | – | – | – |
| steady (100 orders/s) | 6001 | 100 | 0.8 | 3.2 | 7.5 | 6001 | 0 | 99.6 | **253.2** | **379.9** | 429.2 |
| steady (500 orders/s) | 29978 | 500 | 0.7 | 4.9 | 16.3 | 29978 | 0 | 497.6 | **320.1** | **487.7** | 827.3 |

Rejected POSTs in the max run: 0. In the max run the API accepts orders faster than the sagas finish, so a backlog builds in the outbox and Kafka and is worked off afterwards (no order lost or left pending); saga latency there is mostly queueing time (p50 168312.6 ms) and is not shown as a latency figure. Saga latency = order created -> COMPLETED: outbox relay -> payment -> reply -> outbox relay -> inventory -> reply.
