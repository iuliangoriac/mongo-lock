package com.mongodb.pessimistic.heartbeat;

import ch.qos.logback.classic.Level;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.heartbeat.MongoLockManager.*;
import static org.junit.jupiter.api.Assertions.*;

public class MongoLockImplTest {

    private static final AtomicInteger lockCounter = new AtomicInteger(1000);
    private static final MongoCollection<Document> LOCK_COLLECTION;

    private final TestAppender testAppender = TestAppender.getInstance();
    private final MongoLockManager lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());

    static {
        LOCK_COLLECTION = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME);
    }

    @AfterAll
    static void cleanupAll() {
        LOCK_COLLECTION.deleteMany(new Document());
    }

    @BeforeEach
    void cleanup() {
        LOCK_COLLECTION.deleteMany(new Document());

        testAppender.clearLogs();
        testAppender.setlogLevel(Level.INFO);
    }

    @Test
    void checkLockIdentity() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLock(lockId);
        var lock2 = lockManager.createLock(lockId);
        assertNotEquals(lock1, lock2);
    }

    @Test
    void checkHandleMongoLockWrappingException() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock = (MongoLockImpl) lockManager.createLock(lockId);

        assertTrue(lock.handleMongoLockWrappingException(new InterruptedException()).getMessage().contains(lockId));

        var mlwEx = new MongoLockWrappingException("", new MongoInterruptedException("", null));
        assertTrue(lock.handleMongoLockWrappingException(mlwEx).getMessage().contains(lockId));

        var mlwEx1 = new MongoLockWrappingException("Outer", new InterruptedException("Inner"));
        try {
            throw lock.handleMongoLockWrappingException(mlwEx1);
        } catch (MongoLockWrappingException e) {
            assertTrue(e.getMessage().contains("Outer"));
        } catch (Exception e) {
            fail("Should not have thrown " + e);
        }

        try {
            throw lock.handleMongoLockWrappingException(new NullPointerException());
        } catch (MongoLockWrappingException e) {
            assertTrue(e.getMessage().contains(lockId));
        } catch (Exception e) {
            fail("Should not have thrown " + e);
        }
    }

    @Test
    void checkLockLifeCycle() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);

        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var lock = lockManager.createLock(lockId);
        var lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNull(lockDoc);

        // lock acquisition
        lock.lockInterruptibly();
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        assertTrue(DEFAULT_LOCK_TTL_MILLIS <= lockDoc.getLong(MongoLockRepository.TTL_MS));
        var expireAt0 = lockDoc.getLong(MongoLockRepository.EXPIRES_AT);

        // wait for at least 2 heartbeats
        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);
        var beatCount = new AtomicInteger();
        testAppender.getLogs().forEach(entry -> {
            if (entry.getFormattedMessage().contains("Beat")) {
                beatCount.incrementAndGet();
            }
        });
        assertTrue(2 <= beatCount.intValue());

        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt1 = lockDoc.getLong(MongoLockRepository.EXPIRES_AT);
        assertTrue(expireAt0 < expireAt1);

        // lock release
        lock.unlock();
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLockAcquisitionWithExplicitRelease() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLock(lockId);
        var lock2 = lockManager.createLock(lockId);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());
        lock1.unlock();
        assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void checkLockAcquisitionWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLock(lockId);
        var lock2 = lockManager.createLock(lockId);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);

        assertFalse(lock2.tryLock());
    }

    @Test
    void checkLockAcquisitionWithoutExplicitReleaseInASeparateThread() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.createLock(lockId, 1, TimeUnit.HOURS);
            try {
                assertTrue(lock.tryLock());
            } catch (MongoLockWrappingException e) {
                throw new RuntimeException(e);
            }
        });

        separateThread.start();
        separateThread.join();

        assertFalse(lockManager.createLock(lockId, 3500, TimeUnit.MILLISECONDS).tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);

        assertTrue(lockManager.createLock(lockId, 3500, TimeUnit.MILLISECONDS).tryLock());
    }

}
