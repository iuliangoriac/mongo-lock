package com.mongodb.pessimistic.unified;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.MinKey;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;
import static java.lang.String.format;

/**
 * Represents an individual lock managed by the {@link MongoLockManager}.
 * Implements the {@link Lock} interface for thread-safe operations.
 */
class MongoLock implements Lock {

    private final MongoLockManager mongoLockManager;
    private final String lockId;
    private final long ttlMillis;
    private final String lockOwnerId;
    private final Thread ownerThread;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final AtomicBoolean acquired = new AtomicBoolean();

    MongoLock(MongoLockManager mongoLockManager, String lockId, long ttlMillis) {
        this.mongoLockManager = mongoLockManager;
        this.lockId = lockId;
        this.ttlMillis = ttlMillis;
        this.lockOwnerId = UUID.randomUUID().toString();
        this.ownerThread = Thread.currentThread();

        var prevUeh = ownerThread.getUncaughtExceptionHandler();
        ownerThread.setUncaughtExceptionHandler(new MongoLockUncaughtExceptionHandler(this, prevUeh));
    }

    String getLockId() {
        return lockId;
    }

    boolean isOwnerAlive() {
        return ownerThread.isAlive();
    }

    boolean isAcquired() {
        return acquired.get();
    }

    /**
     * Acquires the lock, blocking the owner until it becomes available.
     */
    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void lock() {
        var attemptNo = 0;
        reentrantLock.lock();
        try {
            while (!tryLock()) {
                long delay = mongoLockManager.calculateDelay(attemptNo++);
                try {
                    condition.await(delay, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    MongoLockManager.LOGGER.error("Lock `{}' acquisition of `{}' was interrupted", lockId, lockOwnerId, ex);
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Acquires the lock, blocking until it becomes available or the thread is interrupted.
     *
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void lockInterruptibly() throws InterruptedException {
        var attemptNo = 0;
        reentrantLock.lockInterruptibly();
        try {
            while (!tryLock()) {
                long delay = mongoLockManager.calculateDelay(attemptNo++);
                try {
                    condition.await(delay, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new InterruptedException(format("Lock `%s' acquisition of `%s' was interrupted", lockId, lockOwnerId));
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Attempts to acquire the lock within the specified timeout.
     *
     * @param time the maximum time to wait for the lock.
     * @param unit the time unit of the {@code time} argument.
     * @return {@code true} if the lock was acquired, {@code false} otherwise.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        var deadline = System.currentTimeMillis() + unit.toMillis(time);
        var attemptNo = 0;
        reentrantLock.lockInterruptibly();
        try {
            while (!tryLock()) {
                long remainingTime = deadline - System.currentTimeMillis();
                if (remainingTime <= 0) {
                    return false;
                }
                long delay = Math.min(mongoLockManager.calculateDelay(attemptNo++), remainingTime);
                try {
                    condition.await(delay, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new InterruptedException(format("Lock `%s' acquisition of `%s' was interrupted", lockId, lockOwnerId));
                }
            }
            return true;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Attempts to acquire the lock immediately.
     * <p>
     * If the owner already has the lock then the TTL is extended.
     * If the requesting thread does not have the lock then is can acquire it if the lock's deadline expired.
     * </p>
     *
     * @return {@code true} if the lock was acquired, {@code false} otherwise.
     */
    @Override
    public boolean tryLock() {
        // Define the update operations to set the lock's expiration time, owner ID, and TTL (time-to-live).
        var update = List.of(
                set(MongoLockManager.EXPIRES_AT, new Document("$add", List.of("$$NOW", ttlMillis))),
                set(MongoLockManager.OWNER_ID, lockOwnerId),
                set(MongoLockManager.TTL_MS, ttlMillis)
        );
        try {
            // Define the query to attempt acquiring the lock.
            // This query ensures the lock does not already exist (using MinKey as a placeholder for non-existent locks).
            var query = and(eq("_id", lockId), lt("_id", new MinKey()));

            // Specify options for the update operation, enabling upsert (insert if not found).
            var options = new UpdateOptions().upsert(true);

            // Attempt to acquire the lock by updating or inserting the lock document in the collection.
            mongoLockManager.getLockCollection().updateOne(query, update, options);
            MongoLockManager.LOGGER.info("Lock for `{}' was acquired by `{}'", lockId, lockOwnerId);

            acquired.set(true);
            return true;// Lock acquisition successful.
        } catch (MongoWriteException mongoEx) {
            if (handleMongoWriteException(mongoEx, update, lockId, lockOwnerId, mongoLockManager)) {
                acquired.set(true);
                return true;
            }
        } catch (Exception ex) {
            MongoLockManager.LOGGER.error("Error acquiring lock for `{}' by `{}'", lockId, lockOwnerId, ex);
        }

        acquired.set(false);
        return false;
    }

    static boolean handleMongoWriteException(MongoWriteException mongoEx
            , List<Bson> update
            , String lockId
            , String lockOwnerId
            , MongoLockManager mongoLockManager) {
        var category = mongoEx.getError().getCategory();

        if (ErrorCategory.DUPLICATE_KEY.equals(category)) {
            // Define a query to check if the lock can be "extended" or re-acquired.
            // This happens if:
            // - The lock is already owned by the current owner.
            // - The lock has expired (its expiration time is less than the current time).
            var query = or(
                    and(eq("_id", lockId), eq(MongoLockManager.OWNER_ID, lockOwnerId)),
                    and(eq("_id", lockId), expr(new Document("$lt", List.of("$" + MongoLockManager.EXPIRES_AT, "$$NOW"))))
            );

            // Specify options to return the updated document after the operation.
            var options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE);

            // Attempt to extend or "steal" the lock by updating the document.
            var result = mongoLockManager.getLockCollection().findOneAndUpdate(query, update, options);

            if (result != null) {
                // If the update succeeds, the lock has been extended or taken from another worker.
                if (lockOwnerId.equals(result.getString(MongoLockManager.OWNER_ID))) {
                    MongoLockManager.LOGGER.info("Lock for `{}' was extended for `{}'", lockId, lockOwnerId);
                } else {
                    MongoLockManager.LOGGER.info("Lock for `{}' was transferred to `{}'", lockId, lockOwnerId);
                }
                return true; // Lock acquisition successful.
            }
        }

        if (MongoLockManager.LOGGER.isTraceEnabled()) {
            MongoLockManager.LOGGER.trace("Lock `{}' acquisition attempt by `{}' failed ({})",
                    lockId, lockOwnerId, category.name());
        }
        return false; // Lock acquisition failed.
    }

    /**
     * Releases the lock.
     * <p>
     * Once the lock is released the owner thread can try to acquire it again.
     * </p>
     * Since this method can be called from different places multiple times it is paramount for it to be idempotent.
     */
    @Override
    public void unlock() {
        try {
            var lockData = new Document("_id", lockId).append(MongoLockManager.OWNER_ID, lockOwnerId);
            var result = mongoLockManager.getLockCollection().findOneAndDelete(lockData);
            if (result != null) {
                MongoLockManager.LOGGER.info("Lock `{}' released by `{}'", lockId, lockOwnerId);
            }
        } finally {
            acquired.set(false);
            reentrantLock.lock();
            try {
                condition.signal(); // Notify a waiting thread
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    /**
     * Unsupported operation for this lock implementation.
     *
     * @return nothing, as this operation is unsupported.
     * @throws UnsupportedOperationException always thrown.
     */
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported by " + this.getClass().getName());
    }

}
