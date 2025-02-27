package com.mongodb.pessimistic.heartbeat;

import java.util.concurrent.TimeUnit;

/**
 * A distributed lock interface for MongoDB-based pessimistic locking.
 * <p>
 * This interface is conceptually similar to {@link java.util.concurrent.locks.Lock},
 * but it is designed for distributed environments where locks are persisted in a MongoDB collection.
 * It provides methods for acquiring and releasing locks in a thread-safe and distributed manner.
 * </p>
 * <p>
 * Key differences from {@link java.util.concurrent.locks.Lock}:
 * <ul>
 *   <li>Locks are persisted in MongoDB and can survive application crashes or restarts.</li>
 *   <li>Lock expiration is managed using a TTL (time-to-live) mechanism to prevent deadlocks.</li>
 *   <li>Heartbeat mechanisms are used to automatically extend the TTL of active locks.</li>
 *   <li>Lock ownership is tracked by thread and process identifiers.</li>
 * </ul>
 * </p>
 */
public interface MongoLock {

    /**
     * Acquires the lock, blocking the calling thread until the lock becomes available
     * or the thread is interrupted.
     * <p>
     * This method is similar to {@link java.util.concurrent.locks.Lock#lockInterruptibly()},
     * but it interacts with a distributed MongoDB-based lock system. If the lock is already
     * held by another owner, the calling thread will block until the lock is released.
     * </p>
     * <p>
     * Note: The lock acquisition respects the TTL mechanism. If the TTL expires and
     * the lock is not renewed, other threads or processes may acquire the lock.
     * </p>
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for the lock.
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * Attempts to acquire the lock within the specified timeout.
     * <p>
     * This method is similar to {@link java.util.concurrent.locks.Lock#tryLock(long, TimeUnit)},
     * but it incorporates additional considerations for distributed locks:
     * <ul>
     *   <li>This method uses an exponential backoff strategy with jitter for retrying
     *       lock acquisition attempts.
     *       If the lock is already held by another owner, the method will wait for the lock
     *       to become available or until the timeout expires.</li>
     *   <li>The timeout is enforced using the system clock of the JVM running this method.</li>
     *   <li>Heartbeat mechanisms may extend the lock's expiration while waiting.</li>
     * </ul>
     * </p>
     *
     * @param time the maximum time to wait for the lock. A value of 0 means no timeout (wait indefinitely).
     * @param unit the time unit of the {@code time} argument.
     * @return {@code true} if the lock was acquired successfully, {@code false} if the timeout expired.
     * @throws InterruptedException if the current thread is interrupted while waiting for the lock.
     */
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to acquire the lock immediately without blocking.
     * <p>
     * This method is similar to {@link java.util.concurrent.locks.Lock#tryLock()},
     * but it has additional behavior for distributed locks:
     * <ul>
     *   <li>If the lock is not currently held, it will be acquired and its TTL will be set.</li>
     *   <li>If the lock is already held by the current owner, the TTL will be extended.</li>
     *   <li>If the lock is held by another owner and has not expired, the method will return {@code false}.</li>
     * </ul>
     * </p>
     *
     * @return {@code true} if the lock was acquired or renewed successfully, {@code false} otherwise.
     */
    boolean tryLock();

    /**
     * Releases the lock if it is currently held by the calling thread.
     * <p>
     * This method is similar to {@link java.util.concurrent.locks.Lock#unlock()},
     * but it is designed for distributed locks:
     * <ul>
     *   <li>The lock is removed from the MongoDB collection, making it available for other threads or processes.</li>
     *   <li>Releasing a lock is idempotent, meaning multiple calls to this method will not cause errors.</li>
     *   <li>If the lock is not held by the calling thread, this method has no effect.</li>
     * </ul>
     * </p>
     * <p>
     * Note: It is important to release locks explicitly to avoid unnecessary contention
     * and to ensure time effective cleanup of lock resources.
     * </p>
     */
    void unlock();

}
