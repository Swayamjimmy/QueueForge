const TOTAL_ORDERS = parseInt(process.env.TOTAL_ORDERS) || 200;
const DURATION_SECONDS = parseInt(process.env.DURATION_SECONDS) || 30;
const API_URL = process.env.API_URL || 'http://localhost:3000';
const API_KEY = process.env.API_KEY || 'dev-api-key-12345';

const customers = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'];
const products = ['Laptop', 'Phone', 'Headphones', 'Keyboard', 'Monitor', 'Mouse'];

async function sendOrder(index) {
  const order = {
    customer: customers[index % customers.length],
    items: [
      { product: products[Math.floor(Math.random() * products.length)], quantity: Math.floor(Math.random() * 5) + 1 },
    ],
    idempotencyKey: 'load-test-' + Date.now() + '-' + index,
  };

  try {
    const res = await fetch(API_URL + '/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
      body: JSON.stringify(order),
    });
    const data = await res.json();
    if (res.ok) return { success: true, orderId: data.orderId };
    return { success: false, error: data.error || res.statusText };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

async function run() {
  console.log('Starting load test: ' + TOTAL_ORDERS + ' orders over ' + DURATION_SECONDS + 's');
  const interval = (DURATION_SECONDS * 1000) / TOTAL_ORDERS;
  let sent = 0;
  let succeeded = 0;
  let failed = 0;

  for (let i = 0; i < TOTAL_ORDERS; i++) {
    sendOrder(i).then(result => {
      if (result.success) succeeded++;
      else failed++;
    });
    sent++;
    if (sent % 20 === 0) console.log('Sent: ' + sent + '/' + TOTAL_ORDERS);
    await new Promise(r => setTimeout(r, interval));
  }

  await new Promise(r => setTimeout(r, 3000));
  console.log('Load test complete. Sent: ' + sent + ' Succeeded: ' + succeeded + ' Failed: ' + failed);
}

run();