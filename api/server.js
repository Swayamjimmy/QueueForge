const express = require('express');
const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const client = require('prom-client');
const { rateLimit } = require('express-rate-limit');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const API_KEY = process.env.API_KEY || 'dev-api-key-12345';

// Redis connection
const connection = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null,
});

// BullMQ queues
const orderQueue = new Queue('order-queue', { connection });
const dlqQueue = new Queue('dead-letter-queue', { connection });

// Prometheus metrics
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const ordersCreated = new client.Counter({
  name: 'orders_created_total',
  help: 'Total number of orders created',
  labelNames: ['status'],
  registers: [register],
});

const httpDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5],
  registers: [register],
});

const queueDepth = new client.Gauge({
  name: 'orders_queue_depth',
  help: 'Current number of orders waiting in the queue',
  registers: [register],
  async collect() {
    const counts = await orderQueue.getJobCounts('waiting', 'active', 'delayed');
    this.set(counts.waiting + counts.active + counts.delayed);
  },
});

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 100,
  standardHeaders: 'draft-8',
  legacyHeaders: false,
  message: { error: 'Too many requests. Please try again later.' },
});

// HTTP duration tracking middleware
app.use((req, res, next) => {
  const end = httpDuration.startTimer();
  res.on('finish', () => {
    end({ method: req.method, route: req.route ? req.route.path : req.path, status_code: res.statusCode });
  });
  next();
});

// Health check (liveness) - no auth required
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'order-api' });
});

// Readiness check - verifies Redis connectivity
app.get('/ready', async (req, res) => {
  try {
    await connection.ping();
    res.json({ status: 'ready', redis: 'connected' });
  } catch (err) {
    res.status(503).json({ status: 'not ready', redis: 'disconnected' });
  }
});

// Metrics endpoint - no auth required
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// API key authentication middleware
function authenticate(req, res, next) {
  const key = req.headers['x-api-key'];
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: 'Invalid or missing API key' });
  }
  next();
}

// Apply rate limiting and auth to order routes
app.use('/orders', limiter, authenticate);

// Create a new order
app.post('/orders', async (req, res) => {
  const { customer, items, idempotencyKey } = req.body;

  if (!customer || !items || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Fields "customer" (string) and "items" (non-empty array) are required' });
  }

  // Idempotency check
  if (idempotencyKey) {
    const existing = await connection.get('idempotency:' + idempotencyKey);
    if (existing) {
      ordersCreated.inc({ status: 'duplicate' });
      return res.status(200).json({ orderId: existing, status: 'already_created', message: 'Duplicate request detected' });
    }
  }

  const orderId = uuidv4();

  const job = await orderQueue.add(
    'process-order',
    { orderId, customer, items, createdAt: new Date().toISOString() },
    {
      jobId: orderId,
      attempts: 5,
      backoff: { type: 'exponential', delay: 2000 },
      removeOnComplete: { count: 1000 },
      removeOnFail: false,
    }
  );

  // Store idempotency key with 1 hour TTL
  if (idempotencyKey) {
    await connection.set('idempotency:' + idempotencyKey, orderId, 'EX', 3600);
  }

  ordersCreated.inc({ status: 'queued' });
  res.status(201).json({ orderId: job.id, status: 'queued', message: 'Order accepted for processing' });
});

// Check order status
app.get('/orders/:id', async (req, res) => {
  const job = await orderQueue.getJob(req.params.id);
  if (!job) {
    return res.status(404).json({ error: 'Order not found' });
  }
  const state = await job.getState();
  const response = { orderId: job.id, status: state, data: job.data, attemptsMade: job.attemptsMade };
  if (state === 'completed') response.result = job.returnvalue;
  if (state === 'failed') response.error = job.failedReason;
  res.json(response);
});

// Get dead-letter queue contents
app.get('/orders/failed/dlq', async (req, res) => {
  const jobs = await dlqQueue.getJobs(['waiting', 'completed'], 0, 20);
  const results = jobs.map(j => ({ id: j.id, data: j.data, failedReason: j.data.failedReason }));
  res.json({ count: results.length, orders: results });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('Order API running on port ' + PORT);
});