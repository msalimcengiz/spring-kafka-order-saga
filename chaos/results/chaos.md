Load: 20 VUs for 90s. SIGKILLed 28 service instances (order, 2× payment, 2× inventory) and the Kafka broker once; 0.05 of handler attempts failed on purpose.

| orders | completed | declined (no money) | out of stock (refunded) | acknowledged | client gave up |
|---:|---:|---:|---:|---:|---:|
| 15516 | 664 | 13328 | 1524 | 15506 | 40 |

| check | violations |
|---|---:|
| sagas not finished | **0** |
| acknowledged orders missing | **0** |
| payment does not match the order's outcome | **0** |
| customers whose balance ≠ 5000 − completed orders | **0** |
| products whose stock ≠ 100 − completed quantities | **0** |
| stock reserved for an order that did not complete | **0** |
| records in dead-letter topics | **0** |

**PASS**
