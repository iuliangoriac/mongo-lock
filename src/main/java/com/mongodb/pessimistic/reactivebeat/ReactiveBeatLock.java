package com.mongodb.pessimistic.reactivebeat;

import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static com.mongodb.pessimistic.reactivebeat.ReactiveBeatLocker.*;

public class ReactiveBeatLock implements Lock, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveBeatLocker.class);

    private static final long ATTEMPT_INTERVAL_IN_MILLIS = BEAT_INTERVAL_IN_MILLIS / 10;
    private static final long MAX_ATTEMPT_INTERVAL_IN_MILLIS = BEAT_INTERVAL_IN_MILLIS * 2;

    private final ReactiveBeatLocker locker;
    private final String lockId;
    private final Thread owner;
    private boolean active;

    public ReactiveBeatLock(ReactiveBeatLocker locker, String lockId) {
        this.locker = locker;
        this.lockId = lockId;
        this.owner = Thread.currentThread();
    }

    public String getLockId() {
        return lockId;
    }

    public synchronized boolean isActive() {
        return active && owner.isAlive();
    }

    @Override
    @SuppressWarnings("BusyWait")
    public void lock() {
        var waitTime = ATTEMPT_INTERVAL_IN_MILLIS;
        while (!tryLock()) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException ex) {
                logger.error("Waiting for lock on `{}'", lockId, ex);
            }
            waitTime = Math.min(waitTime * 2, MAX_ATTEMPT_INTERVAL_IN_MILLIS);
        }
    }

    @Override
    @SuppressWarnings("BusyWait")
    public void lockInterruptibly() throws InterruptedException {
        var waitTime = ATTEMPT_INTERVAL_IN_MILLIS;
        while (!tryLock()) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            waitTime = Math.min(waitTime * 2, MAX_ATTEMPT_INTERVAL_IN_MILLIS);
        }
    }

    /*
     * This should be done in a transaction
     * var lockData = new Document("_id", lockId);
     * lockData.append(EXPIRE_AT_ATTR, System.currentTimeMillis() + 24 * 60 * 60 * 1000);
     * Mono.from(lockCollection.insertOne(lockData)).block();
     * Mono.from(lockCollection.updateOne(
     * new Document("_id", lockId),
     * List.of(set(EXPIRE_AT_ATTR, new Document("$add", List.of("$$NOW", EXTENSION_INTERVAL_IN_MILLIS)))))).block();
     *
     * @return true if lock was acquired false otherwise
     */
    @Override
    public synchronized boolean tryLock() {
        if (active) {
            return true;
        }
        try {
            var serverTimeMillis = Objects.requireNonNull(Mono.from(locker.getLockCollection().aggregate(List.of(
                    Aggregates.project(new Document("currentDate", new Document("$toLong", "$$NOW")))
            )).first()).block()).getLong("currentDate");
            var lockData = new Document("_id", lockId);
            lockData.append(EXPIRE_AT_ATTR, serverTimeMillis + EXTENSION_INTERVAL_IN_MILLIS);
            Mono.from(locker.getLockCollection().insertOne(lockData)).block();
            active = true;
            logger.info("Lock `{}' acquired", lockId);
            return true;
        } catch (MongoWriteException mongoEx) {
            if (mongoEx.getError().getCategory().equals(com.mongodb.ErrorCategory.DUPLICATE_KEY)) {
                logger.info("Lock `{}' already taken", lockId);
                return false;
            }
            logger.error("Error acquiring lock for `{}'", lockId, mongoEx);
        } catch (Exception ex) {
            logger.error("Error acquiring lock for `{}'", lockId, ex);
        }
        return false;
    }

    @Override
    @SuppressWarnings("BusyWait")
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        var deadline = System.currentTimeMillis() + unit.toMillis(time);
        var waitTime = ATTEMPT_INTERVAL_IN_MILLIS;
        do {
            if (tryLock()) {
                return true;
            }
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            waitTime = Math.min(waitTime * 2, MAX_ATTEMPT_INTERVAL_IN_MILLIS);
        } while (System.currentTimeMillis() < deadline);
        return false;
    }

    @Override
    public synchronized void unlock() {
        try {
            var lockData = new Document("_id", lockId);
            Mono.from(locker.getLockCollection().findOneAndDelete(lockData)).block();
            logger.info("Lock `{}' released", lockId);
        } catch (Exception ex) {
            logger.error("Error realising lock for `{}'", lockId, ex);
        } finally {
            active = false;
        }

    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not supported by " + this.getClass().getName());
    }

    @Override
    public void close() {
        unlock();
    }

}
