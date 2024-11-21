const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const Redis = require('ioredis');
const Queue = require('bull');

const redisClient = new Redis();
const app = express();
app.use(bodyParser.json());

// Task queue
const taskQueue = new Queue('taskQueue', { redis: { port: 6379, host: '127.0.0.1' } });

// Rate limiting
const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'rateLimiter',
  points: 20, // 20 requests
  duration: 60, // per minute
  blockDuration: 1, // block for 1 second
});

// Task function
async function task(user_id) {
  const logEntry = `${user_id}-task completed at-${Date.now()}\n`;
  fs.appendFileSync('task.log', logEntry);
  console.log(logEntry.trim());
}

// Queue processing
taskQueue.process(async (job) => {
  const { user_id } = job.data;
  await rateLimiter.consume(user_id);
  await task(user_id);
});

// API endpoint
app.post('/task', async (req, res) => {
  const { user_id } = req.body;
  if (!user_id) return res.status(400).send('user_id is required');

  try {
    await rateLimiter.consume(user_id);
    await task(user_id);
    res.send('Task completed.');
  } catch (rateLimiterRes) {
    taskQueue.add({ user_id }, { delay: 1000 }); // Queue task with delay
    res.send('Task queued due to rate limit.');
  }
});

module.exports = app;
