package com.mongodb.pessimistic.heartbeat;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoWriteException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.pessimistic.heartbeat.MongoLockRepository.*;
import static com.mongodb.pessimistic.heartbeat.MongoLockUtils.LOGGER;
import static java.lang.String.format;

/**
 * Represents an individual lock managed by the {@link MongoLockManager}.
 * <p>
 * This class implements the {@link MongoLock} interface and provides thread-safe operations
 * for distributed pessimistic locking using MongoDB as the backend. Each lock instance is tied
 * to a specific lock ID and maintains metadata such as the lock owner, TTL (time-to-live),
 * and thread ownership.
 * </p>
 * <p>
 * The lock lifecycle includes:
 * <ul>
 *     <li>Acquisition (with blocking, timeout, or immediate attempts).</li>
 *     <li>Renewal (via heartbeat or explicit attempts).</li>
 *     <li>Release (explicit or automatic cleanup).</li>
 * </ul>
 * </p>
 * <p>
 * Key features:
 * <ul>
 *     <li>Thread-safe locking with {@link ReentrantLock} for local thread coordination.</li>
 *     <li>Distributed lock persistence in MongoDB with TTL-based expiration.</li>
 *     <li>Automatic cleanup of locks held by terminated threads.</li>
 *     <li>Exponential backoff with jitter for retrying lock acquisition.</li>
 * </ul>
 * </p>
 */
final class MongoLockImpl implements MongoLock, Comparable<MongoLockImpl> {

    private final MongoLockManager mongoLockManager;
    private final String lockId;
    private final long ttlMillis;
    private final String lockOwnerId;
    private final Thread ownerThread;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final AtomicBoolean active = new AtomicBoolean();

    /**
     * Constructs a new lock instance.
     *
     * @param mongoLockManager the lock manager responsible for managing this lock.
     * @param lockId           the unique identifier for the lock.
     * @param ttlMillis        the time-to-live for the lock in milliseconds.
     */
    MongoLockImpl(MongoLockManager mongoLockManager, String lockId, long ttlMillis) {
        this.mongoLockManager = mongoLockManager;
        this.lockId = lockId;
        this.ttlMillis = ttlMillis;
        this.lockOwnerId = UUID.randomUUID().toString();
        this.ownerThread = Thread.currentThread();
        // Set a custom uncaught exception handler for the owner thread to release the lock if the thread terminates unexpectedly.
        var exceptionHandler = ownerThread.getUncaughtExceptionHandler();
        ownerThread.setUncaughtExceptionHandler(new ThreadUncaughtExceptionHandler(this, exceptionHandler));
    }

    /**
     * Retrieves the unique identifier for this lock.
     *
     * @return the lock ID.
     */
    String getLockId() {
        return lockId;
    }

    /**
     * Checks if the owner thread of this lock is still alive.
     *
     * @return {@code true} if the owner thread is alive, {@code false} otherwise.
     */
    boolean isOwnerAlive() {
        return ownerThread.isAlive();
    }

    /**
     * Checks if the lock is currently active.
     * <p>
     * A lock is considered "active" if it has been successfully acquired by the current
     * owner and has not been explicitly released or expired. This status is maintained
     * by the {@link #active} flag, which is set to {@code true} when the lock is acquired
     * and reset to {@code false} when the lock is released or fails to renew.
     * </p>
     *
     * @return {@code true} if the lock is active, {@code false} otherwise.
     */
    boolean isActive() {
        return active.get();
    }

    /**
     * Acquires the lock, blocking the owner thread until it becomes available.
     * <p>
     * This method blocks indefinitely unless interrupted. If the lock is already
     * held by another owner, it will wait until the lock is released.
     * </p>
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for the lock.
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        tryLock(0, TimeUnit.MILLISECONDS);
    }

    /**
     * Attempts to acquire the lock within the specified timeout.
     * <p>
     * This method uses an exponential backoff strategy with jitter for retrying
     * lock acquisition attempts. It respects the provided timeout and terminates
     * if the deadline is reached.
     * </p>
     *
     * @param time the maximum time to wait for the lock. A value of 0 means no timeout (wait indefinitely).
     * @param unit the time unit of the {@code time} argument.
     * @return {@code true} if the lock was acquired, {@code false} otherwise.
     * @throws InterruptedException if the current thread is interrupted while waiting for the lock.
     */
    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (time > 0 && unit == null) {
            throw new MongoLockWrappingException(format("Lock '%s' acquisition of '%s' failed due to null TimeUnit", lockId, lockOwnerId), new NullPointerException("TimeUnit must not be null when time > 0"));
        }
        try {
            var deadline = System.currentTimeMillis() + unit.toMillis(time);
            var attemptNo = 0;
            reentrantLock.lockInterruptibly();
            try {
                while (!tryLock()) {
                    long delay = mongoLockManager.getTemporalStrategy().calculateDelay(attemptNo++);
                    if (time > 0) {
                        long remainingTime = deadline - System.currentTimeMillis();
                        if (remainingTime <= 0) {
                            return false; // Timeout reached.
                        }
                        delay = Math.min(delay, remainingTime);
                    }
                    condition.await(delay, TimeUnit.MILLISECONDS);
                }
                return true;
            } finally {
                reentrantLock.unlock();
            }
        } catch (Exception ex) {
            throw handleMongoLockWrappingException(ex);
        }
    }

    InterruptedException handleMongoLockWrappingException(Exception ex) {
        if (ex instanceof InterruptedException || ex.getCause() instanceof MongoInterruptedException) {
            return new InterruptedException(format("Lock '%s' acquisition of '%s' was interrupted", lockId, lockOwnerId));
        }
        if (ex instanceof MongoLockWrappingException) {
            throw (MongoLockWrappingException) ex;
        }
        throw new MongoLockWrappingException(format("Lock '%s' acquisition of '%s' failed", lockId, lockOwnerId), ex);
    }

    /**
     * Attempts to acquire the lock immediately without blocking.
     * <p>
     * This method is designed for distributed locking scenarios and interacts with a MongoDB-based lock system.
     * It provides the following behavior:
     * <ul>
     *   <li>If the lock is not currently held, it will be acquired, and its TTL will be set.</li>
     *   <li>If the lock is already held by the current thread, the TTL will be extended.</li>
     *   <li>If the lock is held by another owner and has not expired, the method will return {@code false}.</li>
     * </ul>
     * The acquisition is materialized either by inserting a new document in the MongoDB collection or updating
     * the document if its previous ownership is expired.
     * </p>
     * <p>
     * This method may throw a {@link MongoLockWrappingException} in the following scenarios:
     * <ul>
     *   <li>If an unexpected runtime exception occurs during the lock acquisition process.</li>
     *   <li>If a MongoDB write conflict or other database-related issue prevents the lock from being acquired.</li>
     * </ul>
     * The {@code MongoLockWrappingException} is used to encapsulate low-level exceptions (e.g., {@link com.mongodb.MongoWriteException})
     * and provide a higher-level abstraction relevant to the distributed lock management domain.
     * This ensures that the caller receives a meaningful exception with sufficient context for debugging.
     * </p>
     *
     * @return {@code true} if the lock was acquired or renewed successfully, {@code false} otherwise.
     * @throws MongoLockWrappingException if the lock acquisition fails due to an unexpected runtime exception or database conflict.
     */
    @Override
    public boolean tryLock() {
        try {
            insertLock(mongoLockManager, lockId, lockOwnerId, ttlMillis);
            LOGGER.info("Lock for '{}' was acquired by '{}'", lockId, lockOwnerId);
            active.set(true);
            return true; // Lock acquisition successful.
        } catch (MongoWriteException mongoEx) {
            // Handle duplicate key errors or lock expiration scenarios.
            if (handleMongoWriteException(mongoEx, lockId, lockOwnerId, ttlMillis, mongoLockManager)) {
                active.set(true);
                return true;
            }
        } catch (Exception ex) {
            // Wrap and propagate unexpected exceptions.
            throw new MongoLockWrappingException(format("Lock '%s' acquisition of '%s' failed", lockId, lockOwnerId), ex);
        }
        active.set(false);
        return false;
    }

    /**
     * Handles {@link MongoWriteException} during lock acquisition.
     * <p>
     * This method attempts to recover from specific MongoDB write errors (e.g., duplicate key errors)
     * by checking if the lock can be extended or transferred.
     * For other error categories, it throws a wrapping {@link MongoLockWrappingException}.
     * If the lock can be extended or acquired, it updates the lock document in MongoDB
     * and logs whether the lock was extended or transferred to the current owner.
     * </p>
     *
     * @param mongoEx          the {@link MongoWriteException} to handle.
     * @param lockId           the unique identifier of the lock.
     * @param lockOwnerId      the identifier of the lock owner attempting acquisition.
     * @param ttlMillis        the time-to-live (TTL) for the lock in milliseconds.
     * @param mongoLockManager the {@link MongoLockManager} managing the lock.
     * @return {@code true} if the lock was successfully extended or acquired, {@code false} otherwise.
     * @throws MongoLockWrappingException if the exception cannot be handled.
     */
    static boolean handleMongoWriteException(MongoWriteException mongoEx
            , String lockId
            , String lockOwnerId
            , long ttlMillis
            , MongoLockManager mongoLockManager) {
        var category = mongoEx.getError().getCategory();
        if (ErrorCategory.DUPLICATE_KEY.equals(category)) {
            var result = updateLock(mongoLockManager, lockId, lockOwnerId, ttlMillis);
            if (result != null) {
                // If the update succeeds, the lock has been extended or taken from another worker.
                if (lockOwnerId.equals(result.getString(MongoLockRepository.OWNER_ID))) {
                    LOGGER.info("Lock for '{}' was extended for '{}'", lockId, lockOwnerId);
                } else {
                    LOGGER.info("Lock for '{}' was transferred to '{}'", lockId, lockOwnerId);
                }
                return true; // Lock acquisition successful.
            }
        } else {
            throw new MongoLockWrappingException(format("Lock '%s' acquisition attempt by '%s' failed (%s)", lockId, lockOwnerId, category.name()), mongoEx);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Lock '{}' acquisition attempt by '{}' failed ({})", lockId, lockOwnerId, category.name());
        }
        return false; // Lock acquisition failed.
    }

    /**
     * Releases the lock.
     * <p>
     * This method is idempotent, meaning multiple calls to this method will not cause errors.
     * If the lock document is already deleted (e.g., due to a previous unlock or expiration),
     * the method safely handles this scenario without throwing an error.
     * </p>
     */
    @Override
    public void unlock() {
        unlock(null);
    }

    /**
     * Releases the lock with an optional reason for the release.
     *
     * @param by the reason or context for the release (e.g., "heartbeat").
     */
    void unlock(String by) {
        try {
            if (deleteLock(mongoLockManager, lockId, lockOwnerId) != null) {
                if (by == null) {
                    LOGGER.info("Lock '{}' released by '{}'", lockId, lockOwnerId);
                } else {
                    LOGGER.info("Lock '{}' released from '{}' by {}", lockId, lockOwnerId, by);
                }
            }
        } finally {
            active.set(false);
            reentrantLock.lock();
            try {
                condition.signal(); // Notify a waiting thread
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    /**
     * Compares this lock with another lock based on their IDs and owner IDs.
     * <p>
     * This method is added to support the {@link java.util.concurrent.ConcurrentSkipListSet}  based registry in MongoLockManager
     * </p>
     *
     * @param other the other lock to compare with.
     * @return a negative integer, zero, or a positive integer as this lock is less than,
     * equal to, or greater than the specified lock.
     */
    @Override
    public int compareTo(MongoLockImpl other) {
        if (other == null) {
            return -1;
        }
        var cmp = this.lockId.compareTo(other.lockId);
        return cmp == 0 ? this.lockOwnerId.compareTo(other.lockOwnerId) : cmp;
    }

}
