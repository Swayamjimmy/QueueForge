const { Worker, Queue } = require('bullmq');
const IORedis = require('ioredis');
const express = require('express');
const client = require('prom-client');

const connection = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null,
});

const dlqQueue = new Queue('dead-letter-queue', { connection });

// Prometheus metrics
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const ordersProcessed = new client.Counter({
  name: 'orders_processed_total',
  help: 'Total orders processed',
  labelNames: ['status', 'failed_step'],
  registers: [register],
});

const orderDuration = new client.Histogram({
  name: 'order_processing_duration_seconds',
  help: 'Order processing duration in seconds',
  buckets: [0.1, 0.25, 0.5, 1, 2, 5, 10],
  registers: [register],
});

const dlqSize = new client.Gauge({
  name: 'orders_dlq_size',
  help: 'Number of orders in the dead-letter queue',
  registers: [register],
  async collect() {
    const counts = await dlqQueue.getJobCounts('waiting', 'completed');
    this.set(counts.waiting + counts.completed);
  },
});

// Simulated processing steps
async function simulateStep(name, delayMs, failChance) {
  await new Promise(resolve => setTimeout(resolve, delayMs));
  if (Math.random() < failChance) {
    throw new Error('Step "' + name + '" failed: simulated transient error');
  }
}

// Order processor
const worker = new Worker(
  'order-queue',
  async (job) => {
    const end = orderDuration.startTimer();
    const { orderId, customer, items } = job.data;
    console.log('Processing order ' + orderId + ' for ' + customer + ' (attempt ' + job.attemptsMade + ')');

    await job.updateProgress(10);
    await simulateStep('validate-payment', 200, 0.05);
    await job.updateProgress(40);

    await simulateStep('check-inventory', 150, 0.03);
    await job.updateProgress(70);

    await simulateStep('generate-shipping-label', 100, 0.02);
    await job.updateProgress(100);

    end();
    ordersProcessed.inc({ status: 'completed', failed_step: 'none' });
    console.log('Order ' + orderId + ' completed successfully');

    return {
      orderId,
      status: 'shipped',
      trackingNumber: 'TRK-' + Date.now(),
      processedAt: new Date().toISOString(),
    };
  },
  { connection, concurrency: 3 }
);

worker.on('failed', async (job, err) => {
  const step = err.message.includes('validate-payment') ? 'payment'
    : err.message.includes('check-inventory') ? 'inventory'
    : err.message.includes('generate-shipping') ? 'shipping'
    : 'unknown';

  if (job && job.attemptsMade >= job.opts.attempts) {
    console.error('Order ' + job.data.orderId + ' permanently failed after ' + job.attemptsMade + ' attempts. Moving to DLQ.');
    await dlqQueue.add('failed-order', {
      ...job.data,
      failedReason: err.message,
      failedAt: new Date().toISOString(),
      totalAttempts: job.attemptsMade,
    });
    ordersProcessed.inc({ status: 'dead_letter', failed_step: step });
  } else {
    console.warn('Order ' + (job ? job.data.orderId : 'unknown') + ' failed (attempt ' + (job ? job.attemptsMade : '?') + '): ' + err.message);
    ordersProcessed.inc({ status: 'retrying', failed_step: step });
  }
});

worker.on('error', (err) => {
  console.error('Worker error:', err);
});

// Metrics and health server
const metricsApp = express();

metricsApp.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

metricsApp.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'order-worker' });
});

metricsApp.get('/ready', async (req, res) => {
  try {
    await connection.ping();
    res.json({ status: 'ready', redis: 'connected' });
  } catch (err) {
    res.status(503).json({ status: 'not ready', redis: 'disconnected' });
  }
});

const METRICS_PORT = process.env.METRICS_PORT || 3001;
metricsApp.listen(METRICS_PORT, () => {
  console.log('Worker metrics on port ' + METRICS_PORT);
  console.log('Order Worker ready. Processing with concurrency: 3');
});