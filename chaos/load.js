// Places orders with an Idempotency-Key and retries with the same key until the API
// answers, so a crash between "stored" and "responded" never creates a second order.
// Logs "ACK <orderId>" for every acknowledged order; the chaos script checks them all.
import http from 'k6/http';
import { sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TARGET = __ENV.TARGET || 'http://order-service:8080';

export const options = {
  vus: parseInt(__ENV.VUS || '20', 10),
  duration: __ENV.DURATION || '90s',
  gracefulStop: '60s',
};

const pad = (n) => String(n).padStart(3, '0');

export default function () {
  const key = uuidv4();
  const body = JSON.stringify({
    customerId: `c${pad(1 + Math.floor(Math.random() * 100))}`,
    productId: `p${pad(1 + Math.floor(Math.random() * 20))}`,
    quantity: 1 + Math.floor(Math.random() * 5),
    // Large enough that some customers run out of money during the run.
    amount: (50 + Math.floor(Math.random() * 750)).toFixed(2),
  });
  const params = { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, timeout: '5s' };

  const deadline = Date.now() + 50_000;
  for (let attempt = 0; Date.now() < deadline; attempt++) {
    const res = http.post(`${TARGET}/orders`, body, params);
    if (res.status === 202 || res.status === 200) {
      console.log(`ACK ${res.json('id')}`);
      return;
    }
    sleep(Math.min(0.1 * 2 ** attempt, 2));
  }
  console.log(`GAVEUP ${key}`);
}
