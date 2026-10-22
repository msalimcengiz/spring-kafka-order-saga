// Benchmark load for the order saga. MODE=max runs a closed loop (VUS clients posting
// back to back) to find capacity; MODE=steady posts at a fixed RATE per second to measure
// latency below saturation. Customers and products are topped up before the run, so every
// order takes the full path: charge -> reserve -> completed.
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TARGET = __ENV.TARGET || 'http://order-service:8080';
const MODE = __ENV.MODE || 'max';
const DURATION = __ENV.DURATION || '60s';

export const options = {
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios:
    MODE === 'max'
      ? { max: { executor: 'constant-vus', vus: parseInt(__ENV.VUS || '64', 10), duration: DURATION } }
      : {
          steady: {
            executor: 'constant-arrival-rate',
            rate: parseInt(__ENV.RATE || '100', 10),
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: 50,
            maxVUs: 200,
          },
        },
};

const pad = (n) => String(n).padStart(3, '0');

export default function () {
  const body = JSON.stringify({
    customerId: `c${pad(1 + Math.floor(Math.random() * 100))}`,
    productId: `p${pad(1 + Math.floor(Math.random() * 20))}`,
    quantity: 1 + Math.floor(Math.random() * 5),
    amount: (50 + Math.floor(Math.random() * 750)).toFixed(2),
  });
  const res = http.post(`${TARGET}/orders`, body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': uuidv4() },
    timeout: '10s',
  });
  check(res, { accepted: (r) => r.status === 202 });
}

export function handleSummary(data) {
  return { [`/results/${MODE}.json`]: JSON.stringify(data) };
}
