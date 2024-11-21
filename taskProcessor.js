const Queue = require('bull');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const Redis = require('ioredis');
const fs = require('fs');

// Redis client setup
const redisClient = new Redis();

// Task queue
const taskQueue = new Queue('taskQueue', { redis: { port: 6379, host: '127.0.0.1' } });

// Rate limiting (1 task per second, 20 tasks per minute)
const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'rateLimiter',
  points: 20, // 20 tasks
  duration: 60, // per minute
});

// Task function
async function task(user_id) {
  const logEntry = `${user_id}-task completed at-${Date.now()}\n`;
  fs.appendFileSync('task.log', logEntry);
  console.log(logEntry.trim());
}

// Task processing logic
taskQueue.process(async (job) => {
  const { user_id } = job.data;

  try {
    // Consume 1 point from the rate limiter
    await rateLimiter.consume(user_id);

    // Execute the task
    await task(user_id);

    console.log(`Task processed successfully for user: ${user_id}`);
  } catch (err) {
    if (err instanceof RateLimiterRedis.RateLimiterRes) {
      // If rate limit exceeded, requeue the task with a delay
      console.log(`Rate limit exceeded for user: ${user_id}, requeuing...`);
      taskQueue.add({ user_id }, { delay: 1000 }); // 1-second delay
    } else {
      // Log unexpected errors
      console.error(`Error processing task for user: ${user_id}`, err);
    }
  }
});

// Export for reuse (if needed)
module.exports = { taskQueue };
