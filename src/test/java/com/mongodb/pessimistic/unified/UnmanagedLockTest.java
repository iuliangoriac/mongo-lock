package com.mongodb.pessimistic.unified;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.unified.MongoLockManager.*;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(SystemStubsExtension.class)
public class UnmanagedLockTest {

    private static final AtomicInteger lockCounter = new AtomicInteger();

    private final TestAppender testAppender = TestAppender.getInstance();
    private final MongoLockManager lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());
    private final MongoCollection<Document> lockCollection;

    public UnmanagedLockTest() {
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
    void checkManagerSingleton() {
        assertEquals(lockManager, MongoLockManager.getInstance(MongoDB.getSyncClient()));
    }

    @Test
    void checkLockIdentity() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.SECONDS);
        var lock2 = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.SECONDS);
        assertNotEquals(lock1, lock2);
    }

    @Test
    void checkCalculateDelay_MaxDelayEnforced() {
        int highAttempt = 22; // Any value > 21
        long delay = lockManager.calculateDelay(highAttempt);
        assertTrue(delay < 1 << highAttempt, "Delay should be capped at maxDelay for high attempts");
    }

    @Test
    void checkUncaughtException() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS);
        var handler = new MongoLockUncaughtExceptionHandler(lock, null);
        handler.uncaughtException(null, null);
    }

    @Test
    void checkHandleMongoWriteException() {
        var error = new WriteError(0, "", new BsonDocument());
        var exception = new MongoWriteException(error, new ServerAddress(), new ArrayList<>());
        assertFalse(MongoLock.handleMongoWriteException(exception, null, null, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGenericExceptionHandler() {
        // Arrange
        var mongoLockManager = mock(MongoLockManager.class);
        var lockCollection = mock(MongoCollection.class);
        var lockId = "testLockId";
        var ttlMillis = 1000L;

        when(mongoLockManager.getLockCollection()).thenReturn(lockCollection);

        // Simulate a generic exception being thrown by updateOne
        when(lockCollection.updateOne(any(), (List<? extends Bson>) any(), any()))
                .thenThrow(new RuntimeException("Simulated exception"));

        var tryLockInstance = new MongoLock(mongoLockManager, lockId, ttlMillis);

        // Act
        boolean result = tryLockInstance.tryLock();

        // Assert
        assertFalse(result, "tryLock should return false when an exception is thrown");
    }


    @Test
    void checkLockLifeCycle() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var lock = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
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
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt1 = lockDoc.getDate(EXPIRES_AT);
        assertEquals(expireAt0, expireAt1);

        // lock extension
        assertTrue(lock.tryLock());
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt2 = lockDoc.getDate(EXPIRES_AT);
        assertTrue(expireAt1.before(expireAt2));

        // lock release
        lock.unlock();
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLockAcquisitionWithExplicitRelease() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        var lock2 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());
        lock1.unlock();
        assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void checkLockAcquisitionWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        var lock2 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS + 1);
        assertTrue(lock2.tryLock());

        lock1.unlock();
        var lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);

        lock2.unlock();
        lockDoc = lockCollection.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLock() throws InterruptedException {
        testAppender.setlogLevel(Level.ERROR);

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(lock1.tryLock());

        var separateThread = new Thread(() ->
                lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS).lock());
        separateThread.start();
        separateThread.join(1000);
        separateThread.interrupt();
        separateThread.join();

        for (var entry : testAppender.getLogs()) {
            var message = entry.getFormattedMessage();
            if (message.contains(format("Lock `%s' acquisition", lockId))
                    && message.contains("was interrupted")) {
                return;
            }
        }
        fail("Thread interruption was not recorded");
    }

    @Test
    void checkLockInterruptibly_noInterruption() throws InterruptedException {
        var exceptions = new ArrayList<Exception>();

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.HOURS);
        lock1.lockInterruptibly();

        var separateThread = new Thread(() -> {
            var lock2 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
            assertFalse(lock2.tryLock());
            try {
                lock2.lockInterruptibly();
            } catch (InterruptedException ex) {
                exceptions.add(ex);
            }
        });
        separateThread.start();
        separateThread.join(1000);
        lock1.unlock();
        separateThread.join();

        assertEquals(0, exceptions.size());
    }

    @Test
    void checkLockInterruptibly_withInterruption() throws InterruptedException {
        var exceptions = new ArrayList<Exception>();

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.HOURS);
        lock1.lockInterruptibly();

        var separateThread = new Thread(() -> {
            var lock2 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
            assertFalse(lock2.tryLock());
            try {
                lock2.lockInterruptibly();
            } catch (InterruptedException ex) {
                exceptions.add(ex);
            }
        });
        separateThread.start();
        separateThread.join(1000);
        separateThread.interrupt();
        separateThread.join();

        assertEquals(1, exceptions.size());
        assertTrue(exceptions.get(0).getMessage().startsWith(format("Lock `%s' acquisition", lockId)));
    }

    @Test
    void checkTryLockWithDeadline() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);
        var exceptions = new ArrayList<Exception>();


        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(lock1.tryLock(1, TimeUnit.SECONDS));

        var lock2 = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        assertFalse(lock2.tryLock(1, TimeUnit.MILLISECONDS));

        var separateThread = new Thread(() -> {
            try {
                assertFalse(lock2.tryLock(10, TimeUnit.SECONDS));
            } catch (InterruptedException ex) {
                exceptions.add(ex);
            }
        });
        separateThread.start();
        separateThread.join(1000);
        separateThread.interrupt();
        separateThread.join();

        assertEquals(1, exceptions.size());
        assertTrue(exceptions.get(0).getMessage().startsWith(format("Lock `%s' acquisition", lockId)));
    }

    @Test
    void checkNewCondition() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock = lockManager.newUnmanagedLock(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        assertThrows(UnsupportedOperationException.class, lock::newCondition);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.HOURS);
            assertTrue(lock.tryLock());
        });

        separateThread.start();
        separateThread.join();

        assertFalse(lockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.newUnmanagedLock(lockId, 1, TimeUnit.HOURS);
            assertTrue(lock.tryLock());
            lock.unlock();
        });

        separateThread.start();
        separateThread.join();

        assertTrue(lockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithReleaseOnException() throws InterruptedException {
        var lockId1 = String.valueOf(lockCounter.incrementAndGet());
        var lockId2 = String.valueOf(lockCounter.incrementAndGet());
        var lockId3 = String.valueOf(lockCounter.incrementAndGet());

        var separateThread1 = new Thread(() -> {
            assertTrue(lockManager.newUnmanagedLock(lockId1, 1, TimeUnit.HOURS).tryLock());
            throw new RuntimeException("expected1");
        });
        separateThread1.start();
        separateThread1.join();

        var separateThread2 = new Thread(() -> {
            assertTrue(lockManager.newUnmanagedLock(lockId2, 1, TimeUnit.HOURS).tryLock());
            assertTrue(lockManager.newUnmanagedLock(lockId3, 1, TimeUnit.HOURS).tryLock());
            throw new RuntimeException("expected2");
        });
        separateThread2.start();
        separateThread2.join();

        assertTrue(lockManager.newUnmanagedLock(lockId1, 1, TimeUnit.MILLISECONDS).tryLock());
        assertTrue(lockManager.newUnmanagedLock(lockId2, 1, TimeUnit.MILLISECONDS).tryLock());
        assertTrue(lockManager.newUnmanagedLock(lockId3, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    void checkLockDocumentDeletedByHeartbeat(EnvironmentVariables environmentVariables) throws InterruptedException {
        testAppender.setlogLevel(Level.TRACE);
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, String.valueOf(MIN_LOCK_TTL_MILLIS));
        var altLockManager = MongoLockManager.getInstance(MongoDB.getSyncClient(),
                DEFAULT_CONNECTION_ID,
                DEFAULT_LOCK_DATABASE_NAME,
                DEFAULT_LOCK_COLLECTION_NAME + "Test");
        altLockManager.getLockCollection().deleteMany(new Document());

        altLockManager.newLock(lockId, 1, TimeUnit.MILLISECONDS); // to start the heartbeat
        assertTrue(altLockManager.newUnmanagedLock(lockId, 1, TimeUnit.SECONDS).tryLock());
        assertFalse(altLockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());

        Thread.sleep((long) (3.5 * MIN_LOCK_TTL_MILLIS));

        var record = testAppender.getLogs().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("deleteCount: 1"))
                .findAny();
        assertTrue(record.isPresent(), "Heartbeat did not delete the lock document");

        testAppender.setlogLevel(Level.INFO);
        assertTrue(altLockManager.newUnmanagedLock(lockId, 1, TimeUnit.SECONDS).tryLock());
        assertFalse(altLockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
        Thread.sleep((long) (3.5 * MIN_LOCK_TTL_MILLIS));
        assertTrue(altLockManager.newUnmanagedLock(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

}
