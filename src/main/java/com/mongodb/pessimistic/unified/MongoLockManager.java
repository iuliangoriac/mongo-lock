package com.mongodb.pessimistic.unified;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Updates.set;
import static java.lang.String.format;

/**
 * A MongoDB-based distributed lock manager that provides pessimistic locking functionality.
 * <p>
 * This class allows the creation and management of locks stored in a MongoDB collection.
 * It supports both basic (manually extended) and managed (automatically extended) locks,
 * ensuring thread-safe and distributed locking.
 * </p>
 * <p>
 * The managed locks are periodically extended (heartbeat mechanism) to prevent expiration while in use.
 * </p>
 * <p>
 * This implementation strictly relies on the MongoDB server time for lock lifecycle management.
 * </p>
 */
public final class MongoLockManager {
    static final Logger LOGGER = LoggerFactory.getLogger(MongoLockManager.class);

    public static final String DEFAULT_CONNECTION_ID = "default";
    public static final String DEFAULT_LOCK_DATABASE_NAME = "PessimisticLocks";
    public static final String DEFAULT_LOCK_COLLECTION_NAME = "mongoLocks";

    public static final String DEFAULT_LOCK_TTL_MILLIS_KEY = "DEFAULT_LOCK_TTL_MILLIS";
    public static final long DEFAULT_LOCK_TTL_MILLIS = 5000;
    static final long MIN_LOCK_TTL_MILLIS = 500;

    static final String EXPIRES_AT = "expiresAt";
    static final String OWNER_ID = "ownerId";
    static final String TTL_MS = "ttlMs";

    private static final ConcurrentMap<String, MongoLockManager> LOCK_MANAGER_INSTANCES = new ConcurrentHashMap<>();


    /**
     * Retrieves an instance of {@link MongoLockManager} for the given MongoDB client.
     * Uses default connection ID, database name, and collection name.
     *
     * @param syncMongoClient the MongoDB client instance.
     * @return a singleton instance of {@link MongoLockManager}.
     */
    public static MongoLockManager getInstance(MongoClient syncMongoClient) {
        return getInstance(syncMongoClient,
                DEFAULT_CONNECTION_ID,
                DEFAULT_LOCK_DATABASE_NAME,
                DEFAULT_LOCK_COLLECTION_NAME);
    }

    /**
     * Retrieves an instance of {@link MongoLockManager} for the given MongoDB client and parameters.
     *
     * @param syncMongoClient    the MongoDB client instance.
     * @param mongoConnectionId  the unique identifier for the MongoDB connection.
     * @param lockDatabaseName   the name of the database to store locks.
     * @param lockCollectionName the name of the collection to store locks.
     * @return a singleton instance of {@link MongoLockManager}.
     */
    public static MongoLockManager getInstance(MongoClient syncMongoClient
            , String mongoConnectionId
            , String lockDatabaseName
            , String lockCollectionName
    ) {
        var lockManagerKey = format("%s.%s.%s", mongoConnectionId, lockDatabaseName, lockCollectionName);
        return LOCK_MANAGER_INSTANCES.computeIfAbsent(lockManagerKey, lockManagerId -> {
            var lockDatabase = syncMongoClient.getDatabase(lockDatabaseName);
            var lockCollection = lockDatabase.getCollection(lockCollectionName);
            lockCollection.createIndex(new Document(EXPIRES_AT, 1), new IndexOptions().unique(false));
            lockCollection.createIndex(new Document(OWNER_ID, 1), new IndexOptions().unique(false));

            long defaultLockTtlMillis = getEffectiveLockTtlMillis();
            long beatIntervalMillis = defaultLockTtlMillis / 3;
            long baseBackoffMillis = beatIntervalMillis / 10;
            long maxJitter = baseBackoffMillis / 2;
            long maxDelay = 3 * beatIntervalMillis;

            return new MongoLockManager(lockCollection, defaultLockTtlMillis, beatIntervalMillis, baseBackoffMillis, maxJitter, maxDelay);
        });
    }

    static long getEffectiveLockTtlMillis() {
        long defaultLockTtlMillis = DEFAULT_LOCK_TTL_MILLIS;
        try {
            var envTtl = System.getenv(DEFAULT_LOCK_TTL_MILLIS_KEY);
            if (envTtl != null) {
                defaultLockTtlMillis = Math.max(Long.parseLong(envTtl), MIN_LOCK_TTL_MILLIS);
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Could not parse `" + DEFAULT_LOCK_TTL_MILLIS_KEY + "' environment variable content", ex);
        }
        return defaultLockTtlMillis;
    }

    static void startHeartbeat(AtomicReference<ScheduledExecutorService> heartbeat, Runnable command, long beatIntervalMillis) {
        var newHeartbeat = Executors.newScheduledThreadPool(1);
        if (heartbeat.compareAndSet(null, newHeartbeat)) {
            newHeartbeat.scheduleAtFixedRate(command, 0, beatIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            newHeartbeat.shutdown(); // Avoid resource leakage if another thread initialized it first
        }
    }

    private final MongoCollection<Document> lockCollection;
    private final long defaultLockTtlMillis;
    private final long beatIntervalMillis;
    private final long baseBackoffMillis;
    private final long maxJitter;
    private final long maxDelay;
    private final Set<MongoLock> registeredLocks = Collections.synchronizedSet(new HashSet<>());
    private final AtomicReference<ScheduledExecutorService> heartbeat = new AtomicReference<>();
    private final AtomicBoolean isHeartbeatStarted = new AtomicBoolean(false);
    private final AtomicLong beatCounter = new AtomicLong();

    private MongoLockManager(MongoCollection<Document> lockCollection
            , long defaultLockTtlMillis
            , long beatIntervalMillis
            , long baseBackoffMillis
            , long maxJitter
            , long maxDelay) {
        this.lockCollection = lockCollection;
        this.defaultLockTtlMillis = defaultLockTtlMillis;
        this.beatIntervalMillis = beatIntervalMillis;
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxJitter = maxJitter;
        this.maxDelay = maxDelay;
    }

    /**
     * Creates a new managed lock with the default TTL (time-to-live).
     * <p>
     * Managed locks are automatically extended (heartbeat mechanism) while they are in use.
     * </p>
     *
     * @param lockId the unique identifier for the lock.
     * @return a new {@link Lock} instance.
     */
    public Lock newLock(String lockId) {
        return newLock(lockId, defaultLockTtlMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new managed lock with a custom TTL (time-to-live).
     * <p>
     * Managed locks are automatically extended (heartbeat mechanism) while they are in use.
     * </p>
     *
     * @param lockId the unique identifier for the lock.
     * @param ttl    the time-to-live for the lock.
     * @param unit   the time unit for the TTL.
     * @return a new {@link Lock} instance.
     */
    public Lock newLock(String lockId, long ttl, TimeUnit unit) {
        var mongoLock = new MongoLock(this, lockId, unit.toMillis(ttl));
        registeredLocks.add(mongoLock);
        startHeartbeat();
        return mongoLock;
    }

    /**
     * Creates a new basic lock with a custom TTL (time-to-live).
     * <p>
     * Used for test purposes only.
     * </p>
     *
     * @param lockId the unique identifier for the lock.
     * @param ttl    the time-to-live for the lock.
     * @param unit   the time unit for the TTL.
     * @return a new {@link Lock} instance.
     */
    Lock newUnmanagedLock(String lockId, long ttl, TimeUnit unit) {
        return new MongoLock(this, lockId, unit.toMillis(ttl));
    }

    MongoCollection<Document> getLockCollection() {
        return lockCollection;
    }

    /**
     * Calculates the delay for the next lock acquisition attempt using exponential backoff.
     *
     * @param attempt the number of attempts made so far.
     * @return the calculated delay in milliseconds.
     */
    long calculateDelay(int attempt) {
        // if attempt no. is high simply return the maxDelay without extra calculations
        if (attempt > 21) {
            return maxDelay;
        }
        var backoffMillis = baseBackoffMillis * (1L << attempt); // Exponential backoff
        var jitter = ThreadLocalRandom.current().nextLong(maxJitter << 1) - maxJitter; // Random jitter
        return Math.min(backoffMillis + jitter, maxDelay); // Max delay cap
    }

    /**
     * Starts the heartbeat thread to periodically extend managed locks and clean up expired locks.
     * <p>
     * The heartbeat thread is intended to be unique in each JVM for every triplet
     * (mongoConnectionId, lockDatabaseName, lockCollectionName)
     * </p>
     */
    private void startHeartbeat() {
        if (isHeartbeatStarted.compareAndSet(false, true)) {
            startHeartbeat(heartbeat, this::onBeat, beatIntervalMillis);
        }
    }

    /**
     * Performs a periodic lock maintenance procedure.
     * <p>
     * This involves:
     * - releasing the resources of the locks whose owner thread is no longer alive
     * - removing the previously mentioned locks from the manager's lockRegistry
     * - automatically extending the expiration time of the active locks
     * - deleting expired lock documents from the lockCollection
     * </p>
     */
    private void onBeat() {
        // Increment the heartbeat counter to track the number of beats.
        var beatNo = beatCounter.incrementAndGet();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Beat #{}", beatNo);
        }

        // Identify locks inactive owner thread and release their resources
        var inactiveLocks = registeredLocks.stream()
                .filter(lock -> !lock.isOwnerAlive())
                .collect(Collectors.toSet());
        inactiveLocks.forEach(MongoLock::unlock);
        registeredLocks.removeAll(inactiveLocks);

        // Query to find expired locks (locks whose expiration time is less than the current time)
        // and remove expired locks from the MongoDB collection.
        // This operation has a housekeeping role only since tryLock can already handle expired locks.
        // It's only called occasionally to reduce the burden on the database server
        if (beatNo % 10 == 0) {
            var deleteQuery = expr(new Document("$lt", List.of("$" + EXPIRES_AT, "$$NOW")));
            var deleteResult = lockCollection.deleteMany(deleteQuery);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Beat #{}: deleteCount: {}", beatNo, deleteResult.getDeletedCount());
            }
        }

        // Retrieve IDs of locks owned by threads that are still alive
        var myLockIds = registeredLocks.stream()
                .filter(MongoLock::isAcquired)
                .map(MongoLock::getLockId)
                .collect(Collectors.toSet());
        if (!myLockIds.isEmpty()) {
            // Query to update expiration times for active locks owned by this manager
            var updateQuery = in("_id", myLockIds);

            // The new expiration time will be the current time provided by
            // the MongoDB server current time plus the TTL registered
            var updates = List.of(
                    set(EXPIRES_AT, new Document("$add", List.of("$$NOW", "$" + TTL_MS)))
            );

            var updateResult = lockCollection.updateMany(updateQuery, updates);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Beat #{}: updateCount: {}", beatNo, updateResult.getModifiedCount());
            }
        }
    }

}
