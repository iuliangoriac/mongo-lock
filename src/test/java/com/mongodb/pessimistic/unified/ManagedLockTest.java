package com.mongodb.pessimistic.unified;

import ch.qos.logback.classic.Level;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.unified.MongoLockManager.*;
import static org.junit.jupiter.api.Assertions.*;

public class ManagedLockTest {

    private static final AtomicInteger lockCounter = new AtomicInteger(1000);

    private final TestAppender testAppender = TestAppender.getInstance();
    private final MongoLockManager lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());
    private final MongoCollection<Document> lockCollection;

    public ManagedLockTest() {
        this.lockCollection = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME);
    }

    @BeforeEach
    void cleanup() {
        lockCollection.deleteMany(new Document());

        testAppender.clearLogs();
        testAppender.setlogLevel(Level.INFO);
    }

    @Test
    void checkLockIdentity() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newLock(lockId);
        var lock2 = lockManager.newLock(lockId);
        assertNotEquals(lock1, lock2);
    }

    @Test
    void checkLockLifeCycle() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);

        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var lock = lockManager.newLock(lockId);
        var lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNull(lockDoc);

        // lock acquisition
        lock.lock();
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        assertEquals(DEFAULT_LOCK_TTL_MILLIS, lockDoc.getLong(TTL_MS));
        var expireAt0 = lockDoc.getDate(EXPIRES_AT);

        // wait for at least 2 heartbeats
        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);
        var beatCount = new AtomicInteger();
        testAppender.getLogs().forEach(entry -> {
            if (entry.getFormattedMessage().contains("Beat")) {
                beatCount.incrementAndGet();
            }
        });
        assertTrue(2 <= beatCount.intValue());

        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt1 = lockDoc.getDate(EXPIRES_AT);
        assertTrue(expireAt0.before(expireAt1));

        // lock release
        lock.unlock();
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLockAcquisitionWithExplicitRelease() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newLock(lockId);
        var lock2 = lockManager.newLock(lockId);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());
        lock1.unlock();
        assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void checkLockAcquisitionWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newLock(lockId);
        var lock2 = lockManager.newLock(lockId);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);

        assertFalse(lock2.tryLock());
    }


    @Test
    void checkLockAcquisitionWithoutExplicitReleaseInASeparateThread() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.newLock(lockId, 1, TimeUnit.HOURS);
            assertTrue(lock.tryLock());
        });

        separateThread.start();
        separateThread.join();

        assertFalse(lockManager.newLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);

        assertTrue(lockManager.newLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }


}
