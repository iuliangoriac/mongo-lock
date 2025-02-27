package com.mongodb.pessimistic.heartbeat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.mongodb.pessimistic.heartbeat.MongoLockUtils.LOGGER;
import static com.mongodb.pessimistic.heartbeat.MongoLockRepository.*;
import static java.lang.String.format;

/**
 * Manages distributed pessimistic locks stored in a MongoDB collection.
 * <p>
 * The {@code MongoLockManager} class is responsible for creating, managing, and maintaining locks
 * in a distributed system. It provides functionality to create locks with a time-to-live (TTL),
 * extend lock expiration via heartbeats, and clean up expired or orphaned locks.
 * </p>
 * <p>
 * Key Features:
 * <ul>
 *     <li>Creates and manages distributed locks with TTL-based expiration.</li>
 *     <li>Uses a heartbeat mechanism to periodically extend active locks' expiration time.</li>
 *     <li>Automatically cleans up expired locks and locks held by terminated threads.</li>
 *     <li>Supports exponential backoff with jitter for lock acquisition retries.</li>
 *     <li>Implements a singleton pattern to ensure one instance per MongoDB connection and lock collection.</li>
 * </ul>
 * </p>
 * <p>
 * Usage:
 * <ol>
 *     <li>Obtain an instance of {@code MongoLockManager} using {@link #getInstance(MongoClient)}.</li>
 *     <li>Create locks using {@link #createLock(String)} or {@link #createLease(String, long, TimeUnit)}.</li>
 *     <li>Use the lock's methods (e.g., {@code lockInterruptibly()}, {@code tryLock()}, {@code unlock()}) to manage lock state.</li>
 * </ol>
 * </p>
 * <p>
 * The {@code MongoLockManager} uses a MongoDB collection to persist locks. It ensures that locks
 * are released when no longer needed, either explicitly by the owning thread or automatically
 * through the heartbeat mechanism.
 * </p>
 */
public final class MongoLockManager {

    // Default connection ID used to uniquely identify the MongoDB connection for the lock manager.
    public static final String DEFAULT_CONNECTION_ID = "default";

    // Default database name where the lock collection is stored.
    public static final String DEFAULT_LOCK_DATABASE_NAME = "MongoLocks";

    // Default collection name for storing locks.
    public static final String DEFAULT_LOCK_COLLECTION_NAME = "pessimisticLocks";

    // Environment variable key for overriding the default lock TTL (time-to-live) in milliseconds.
    public static final String DEFAULT_LOCK_TTL_MILLIS_KEY = "DEFAULT_LOCK_TTL_MILLIS";

    // Default lock TTL in milliseconds if no environment variable is provided.
    public static final long DEFAULT_LOCK_TTL_MILLIS = 5000;

    // A thread-safe map to ensure a single instance of MongoLockManager per MongoDB connection and lock collection.
    private static final ConcurrentMap<String, MongoLockManager> LOCK_MANAGER_INSTANCES = new ConcurrentHashMap<>();

    /**
     * Retrieves a singleton instance of {@code MongoLockManager} for the default MongoDB connection,
     * database, and collection.
     *
     * @param syncMongoClient the MongoDB client to use for lock management.
     * @return the singleton instance of {@code MongoLockManager}.
     */
    public static MongoLockManager getInstance(MongoClient syncMongoClient) {
        return getInstance(syncMongoClient,
                DEFAULT_CONNECTION_ID,
                DEFAULT_LOCK_DATABASE_NAME,
                DEFAULT_LOCK_COLLECTION_NAME);
    }

    /**
     * Retrieves a singleton instance of {@code MongoLockManager} for a specific MongoDB connection,
     * database, and collection.
     * <p>
     * This method implements a "multi-instance singleton" pattern, ensuring that only one instance
     * of {@code MongoLockManager} exists for each unique combination of MongoDB connection,
     * database, and collection.
     * </p>
     *
     * @param syncMongoClient    the MongoDB client to use for lock management.
     * @param mongoConnectionId  a unique identifier for the MongoDB connection.
     * @param lockDatabaseName   the name of the database containing the lock collection.
     * @param lockCollectionName the name of the collection where locks are stored.
     * @return the singleton instance of {@code MongoLockManager}.
     */
    public static MongoLockManager getInstance(MongoClient syncMongoClient
            , String mongoConnectionId
            , String lockDatabaseName
            , String lockCollectionName
    ) {
        // Construct a unique key for the lock manager instance based on connection ID, database, and collection.
        var lockManagerKey = format("%s.%s.%s", mongoConnectionId, lockDatabaseName, lockCollectionName);

        // Use a thread-safe map to ensure a single instance per key.
        return LOCK_MANAGER_INSTANCES.computeIfAbsent(lockManagerKey,
                lockManagerId -> new MongoLockManager(
                        initializeLockCollection(syncMongoClient, lockDatabaseName, lockCollectionName),
                        TemporalAlignmentStrategy.getDefault(MongoLockUtils.getEffectiveLockTtlMillis())));
    }

    private final MongoCollection<Document> lockCollection;
    private final TemporalAlignmentStrategy temporalStrategy;
    private final Set<MongoLockImpl> registeredLocks = new ConcurrentSkipListSet<>();
    private final AtomicLong serverTimeDelta = new AtomicLong();
    private final AtomicReference<ScheduledExecutorService> heartbeat = new AtomicReference<>();
    private final AtomicBoolean isHeartbeatStarted = new AtomicBoolean(false);
    private final AtomicLong beatCounter = new AtomicLong();

    /**
     * Private constructor to initialize a new {@code MongoLockManager}.
     *
     * @param lockCollection   the MongoDB collection used to store locks.
     * @param temporalStrategy the strategy for managing lock TTL and heartbeat intervals.
     */
    private MongoLockManager(MongoCollection<Document> lockCollection, TemporalAlignmentStrategy temporalStrategy) {
        this.lockCollection = lockCollection;
        this.temporalStrategy = temporalStrategy;
    }

    /**
     * Creates a new distributed lock with the default TTL.
     *
     * @param lockId the unique identifier for the lock.
     * @return the created {@code MongoLock} instance.
     */
    public MongoLock createLock(String lockId) {
        return createLock(lockId, temporalStrategy.getLockTtlMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new distributed lock with a specified TTL.
     *
     * @param lockId the unique identifier for the lock.
     * @param ttl    the time-to-live for the lock.
     * @param unit   the time unit for the TTL.
     * @return the created {@code MongoLock} instance.
     */
    public MongoLock createLock(String lockId, long ttl, TimeUnit unit) {
        // Calculate a buffered TTL to account for the clock drift between the client and the server.
        var bufferedTtl = temporalStrategy.getBufferedLockTtlMillis(unit.toMillis(ttl));

        // Create a new lock instance and register it with the manager.
        var mongoLock = new MongoLockImpl(this, lockId, bufferedTtl);
        registeredLocks.add(mongoLock);

        // Start the heartbeat mechanism if it is not already running.
        startHeartbeat();
        return mongoLock;
    }

    /**
     * Creates a lease (temporary lock) with a specified TTL.
     *
     * @param lockId the unique identifier for the lock.
     * @param ttl    the time-to-live for the lease.
     * @param unit   the time unit for the TTL.
     * @return the created {@code MongoLock} instance.
     */
    MongoLock createLease(String lockId, long ttl, TimeUnit unit) {
        return new MongoLockImpl(this, lockId, unit.toMillis(ttl));
    }

    /**
     * Retrieves the current heartbeat counter value.
     *
     * @return the current heartbeat count.
     */
    long getBeatNo() {
        return beatCounter.get();
    }

    /**
     * Retrieves the MongoDB collection used for storing locks.
     *
     * @return the MongoDB lock collection.
     */
    MongoCollection<Document> getLockCollection() {
        return lockCollection;
    }

    /**
     * Retrieves the temporal alignment strategy used for managing lock TTL and heartbeat intervals.
     *
     * @return the temporal alignment strategy.
     */
    TemporalAlignmentStrategy getTemporalStrategy() {
        return temporalStrategy;
    }

    /**
     * Updates the server-client time delta by querying the MongoDB server for the current time.
     * <p>
     * This method ensures accurate synchronization between the client and server clocks,
     * which is critical for managing lock expiration times.
     * If the server time cannot be retrieved via aggregation, a dummy document is inserted
     * to enable the retrieval.
     * </p>
     */
    void updateServerTimeDelta() {
        var serverTimeDoc = readServerTime(lockCollection);
        if (serverTimeDoc == null) {
            ensureLockCollectionIsAggregable(lockCollection);
            serverTimeDoc = readServerTime(lockCollection);
        }
        var serverTimeMillis = Objects.requireNonNull(serverTimeDoc).getDate("serverTime").getTime();
        serverTimeDelta.set(serverTimeMillis - System.currentTimeMillis());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Server - Client time delta was updated to {} milliseconds", serverTimeDelta.get());
        }
    }

    /**
     * Retrieves the current server time in milliseconds, adjusted for the server-client time delta.
     *
     * @return the adjusted server time in milliseconds.
     */
    long serverTimeMillis() {
        return serverTimeDelta.get() + System.currentTimeMillis();
    }

    /**
     * Starts the heartbeat mechanism to maintain active locks and clean up expired locks.
     */
    private void startHeartbeat() {
        // Update the server-client time delta to ensure accurate expiration calculations.
        updateServerTimeDelta();

        // Start the heartbeat thread if it is not already running.
        if (isHeartbeatStarted.compareAndSet(false, true)) {
            MongoLockUtils.startHeartbeat(heartbeat,
                    new HeartbeatUncaughtExceptionHandler(this, this::onBeat),
                    temporalStrategy.getBeatIntervalMillis());
        }
    }

    /**
     * Executes the heartbeat logic to maintain and clean up locks.
     * <p>
     * This method is periodically invoked by the heartbeat thread to:
     * <ul>
     *     <li>Releases locks held by terminated threads and removes them from the {@link #registeredLocks}.</li>
     *     <li>Extends the expiration time of active locks.</li>
     *     <li>Updates the server-client time delta (executed every 5th heartbeat). </li>
     *     <li>Deletes expired locks from the MongoDB collection (executed every 10th heartbeat).</li>
     * </ul>
     * </p>
     */
    private void onBeat() {
        // Increment the heartbeat counter for monitoring purposes.
        var beatNo = beatCounter.incrementAndGet();
        if (MongoLockUtils.LOGGER.isTraceEnabled()) {
            MongoLockUtils.LOGGER.trace("Beat: {}", beatNo);
        }

        // Release locks whose owner threads are no longer alive.
        var inactiveLocks = registeredLocks.stream()
                .filter(lock -> !lock.isOwnerAlive())
                .collect(Collectors.toSet());
        inactiveLocks.forEach(lock -> lock.unlock("heartbeat"));
        registeredLocks.removeAll(inactiveLocks);

        // Occasionally update the server-client time delta.
        if (beatNo % 5 == 1) {
            updateServerTimeDelta();
        }

        // Occasionally delete expired locks from the MongoDB collection.
        if (beatNo % 10 == 0) {
            // This operation has a housekeeping role only since tryLock can already handle expired locks.
            var deleteResult = bulkDeleteExpiredLocks(this);
            if (MongoLockUtils.LOGGER.isDebugEnabled()) {
                MongoLockUtils.LOGGER.debug("Beat: {}, deleteCount: {}", beatNo, deleteResult.getDeletedCount());
            }
        }

        // Extend the expiration time of active locks owned by this manager.
        var myLockIds = registeredLocks.stream()
                .filter(MongoLockImpl::isActive)
                .map(MongoLockImpl::getLockId)
                .collect(Collectors.toSet());
        if (!myLockIds.isEmpty()) {
            var updateResult = bulkExtendActiveLocks(this, myLockIds);
            if (MongoLockUtils.LOGGER.isDebugEnabled()) {
                MongoLockUtils.LOGGER.debug("Beat: {}, updateCount: {}", beatNo, updateResult.getModifiedCount());
            }
        }
    }

}
