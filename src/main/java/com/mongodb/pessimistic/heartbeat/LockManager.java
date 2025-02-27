package com.mongodb.pessimistic.heartbeat;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import org.bson.Document;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class LockManager {

    private static final Logger LOGGER = Logger.getLogger(LockManager.class.getName());
    public static final String EXPIRE_AT = "expireAt";

    private final MongoClient mongoClient;
    private final MongoCollection<Document> lockCollection;
    private final ConcurrentMap<String, LockInfo> lockMap;
    private final Thread extender;
    private volatile boolean isRunning = true;

    private final long ttlSeconds;
    private final int maxRetries;
    private final int baseBackoffMillis;
    private final int maxJitter;
    private final int maxDelay;

    /**
     * Holds information about a lock and its owner thread.
     */
    private static class LockInfo {
        final String lockId;
        final Thread owner;

        LockInfo(String lockId, Thread owner) {
            this.lockId = lockId;
            this.owner = owner;
        }
    }

    /**
     * Periodically extends the expiry of active locks.
     */
    private class LockExtender implements Runnable {
        @Override
        public void run() {
            while (isRunning) {
                try {
                    extendLockExpiry();
                    cleanupExpiredLocks();
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private void extendLockExpiry() {
            List<String> activeLockIds = new ArrayList<>();

            // Collect lock IDs that are still owned by alive threads
            for (LockInfo lockInfo : lockMap.values()) {
                if (lockInfo.owner.isAlive()) {
                    activeLockIds.add(lockInfo.lockId);
                }
            }

            if (!activeLockIds.isEmpty()) {
                Document query = new Document("_id", new Document("$in", activeLockIds));
                LocalDateTime newExpireAt = LocalDateTime.now().plusSeconds(ttlSeconds);
                Document updates = new Document("$set", new Document(EXPIRE_AT, newExpireAt));
                try {
                    lockCollection.updateMany(query, updates);
                    LOGGER.info("Extended life of " + activeLockIds.size() + " locks");
                } catch (MongoException e) {
                    LOGGER.log(Level.SEVERE, "Error updating locks: {0}", e.getMessage());
                }
            }
        }
    }

    /**
     * Constructs a LockManager instance.
     *
     * @param mongoClient the MongoDB client instance.
     * @param database the database name.
     * @param collection the collection name.
     * @param ttlSeconds time-to-live (TTL) for locks in seconds and the time to extend the locks ideally it should be more than
     *                  3 seconds as the thread to extend the locks runs every 3 seconds.
     * @param maxRetries max retry attempts for acquiring a lock.
     * @param baseBackoffMillis base backoff delay in milliseconds.
     * @param maxJitter max jitter to be added to the backoff delay.
     * @param maxDelay maximum backoff delay.
     */
    public LockManager(MongoClient mongoClient, String database, String collection, long ttlSeconds,
                       int maxRetries, int baseBackoffMillis, int maxJitter, int maxDelay) {
        this.mongoClient = mongoClient;
        this.ttlSeconds = ttlSeconds;
        this.maxRetries = maxRetries;
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxJitter = maxJitter;
        this.maxDelay = maxDelay;

        this.lockMap = new ConcurrentHashMap<>();
        this.lockCollection = mongoClient.getDatabase(database).getCollection(collection);

        //MongoDB TTL can be used for clearing up the locks as well if client and server timings are aligned

        /*lockCollection.createIndex(
                Indexes.ascending(EXPIRE_AT),
                new IndexOptions().expireAfter(ttlSeconds, TimeUnit.SECONDS)
        );*/
        LOGGER.info("TTL index created with expiry: " + ttlSeconds + " seconds");

        extender = new Thread(new LockExtender());
        extender.start();
    }

    /**
     * Attempts to acquire a distributed lock for the current thread.
     * If the lock is already held by another thread, retries are made with exponential backoff and jitter.
     *
     * @param lockId the unique identifier for the lock
     * @return true if the lock is successfully acquired, false otherwise
     */
    public boolean getLock(String lockId) {
        Thread currentThread = Thread.currentThread();

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {

                LockInfo existingLockInfo = lockMap.get(lockId);
                if (existingLockInfo != null && existingLockInfo.owner.equals(currentThread)) {
                    LOGGER.info("Lock already held by this thread: " + lockId);
                    return true;
                }

                Document lockData = new Document("_id", lockId)
                        .append(EXPIRE_AT, LocalDateTime.now().plusSeconds(ttlSeconds));
                lockCollection.insertOne(lockData);

                lockMap.put(lockId, new LockInfo(lockId, currentThread));
                LOGGER.info("Lock acquired: " + lockId);
                return true;

            } catch (MongoWriteException e) {
                if (e.getError().getCategory().equals(com.mongodb.ErrorCategory.DUPLICATE_KEY)) {
                    LOGGER.warning("Lock already taken for: " + lockId);

                    int backoffMillis = calculateBackoffWithJitter(attempt);
                    try {
                        Thread.sleep(backoffMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                } else {
                    LOGGER.severe("Unexpected MongoDB write error: " + e.getMessage());
                    return false;
                }
            }
        }

        LOGGER.warning("Failed to acquire lock after maximum retries: " + lockId);
        return false;
    }

    /**
     * Calculates the exponential backoff delay with random jitter.
     *
     * @param attempt the current retry attempt
     * @return the computed backoff delay in milliseconds
     */
    private int calculateBackoffWithJitter(int attempt) {
        int backoffMillis = baseBackoffMillis * (1 << attempt); // Exponential backoff
        int jitter = ThreadLocalRandom.current().nextInt(maxJitter); // Random jitter
        int delay = Math.min(backoffMillis + jitter, maxDelay); // Max delay cap
        LOGGER.warning("Adding Backoff delay of " + delay + " ms ");
        return delay;
    }

    /**
     * Releases the lock with the given lock ID.
     *
     * @param lockId the unique lock ID.
     * @return true if the lock is released successfully, false otherwise.
     */
    public boolean releaseLock(String lockId) {
        lockMap.remove(lockId);
        try {
            lockCollection.deleteOne(new Document("_id", lockId));
            LOGGER.info("Lock released: " + lockId);
            return true;
        } catch (MongoException e) {
            LOGGER.log(Level.SEVERE, "Error releasing lock: {0}", e.getMessage());
            return false;
        }
    }

    /**
     * Stops the lock extender thread and cleans up resources.
     */
    public void stopExtender() {
        isRunning = false;
        extender.interrupt();
        try {
            extender.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Lock extender stopped.");
    }

    /**
     * Cleans up expired locks from the database.
     */
    private void cleanupExpiredLocks() {

        List<String> activeLockIds = lockMap.values()
                .stream()
                .map(lockInfo -> lockInfo.lockId)
                .collect(Collectors.toList());

        if (activeLockIds.isEmpty()) {
            return;
        }

        Document query = new Document("$and", Arrays.asList(
                new Document("_id", new Document("$in", activeLockIds)),
                new Document(EXPIRE_AT, new Document("$lte", LocalDateTime.now()))
        ));

        lockCollection.deleteMany(query);
    }
}

