package com.mongodb.pessimistic.heartbeat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import one.util.streamex.StreamEx;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.heartbeat.MongoLockImpl.handleMongoWriteException;
import static com.mongodb.pessimistic.heartbeat.MongoLockManager.*;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(SystemStubsExtension.class)
public class MongoLeaseTest {

    private static final AtomicInteger lockCounter = new AtomicInteger();

    private static final MongoCollection<Document> LOCK_COLLECTION;
    private static final MongoCollection<Document> ALT_LOCK_COLLECTION;

    private final TestAppender testAppender = TestAppender.getInstance();
    private final MongoLockManager lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());

    static {
        LOCK_COLLECTION = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME);
        ALT_LOCK_COLLECTION = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME + "Test");
    }

    @AfterAll
    static void cleanupAll() {
        LOCK_COLLECTION.deleteMany(new Document());
        ALT_LOCK_COLLECTION.drop();
    }

    @BeforeEach
    void cleanup() {
        LOCK_COLLECTION.deleteMany(new Document());

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
        var lock1 = lockManager.createLease(lockId, 1, TimeUnit.SECONDS);
        var lock2 = lockManager.createLease(lockId, 1, TimeUnit.SECONDS);
        assertNotEquals(lock1, lock2);
    }

    @Test
    void checkHandleMongoWriteException() {
        var error = new WriteError(0, "", new BsonDocument());
        var exception = new MongoWriteException(error, new ServerAddress(), new ArrayList<>());
        assertThrows(MongoLockWrappingException.class,
                () -> handleMongoWriteException(exception, "lockId", "lockOwnerId", 0, null));
    }

    @Test
    void checkCompareToNull() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock = (MongoLockImpl) lockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS);
        assertTrue(lock.compareTo(null) < 0);
    }

    @Test
    void checkNullTimeUnit() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock = (MongoLockImpl) lockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS);
        assertThrows(MongoLockWrappingException.class,
                () -> lock.tryLock(1, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void checkGenericExceptionHandler() {
        // Arrange
        var mongoLockManager = mock(MongoLockManager.class);
        var lockCollection = mock(MongoCollection.class);
        var lockId = "testLockId";
        var ttlMillis = 1000L;

        when(mongoLockManager.getLockCollection()).thenReturn(lockCollection);

        // Simulate a generic exception being thrown by updateOne
        when(lockCollection.insertOne(any()))
                .thenThrow(new RuntimeException("Simulated exception"));

        var tryLockInstance = new MongoLockImpl(mongoLockManager, lockId, ttlMillis);

        // Act
        assertThrows(MongoLockWrappingException.class, tryLockInstance::tryLock);
    }


    @Test
    void checkLockLifeCycle() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var lock = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        var lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNull(lockDoc);

        // lock acquisition
        lock.lockInterruptibly();
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        assertEquals(DEFAULT_LOCK_TTL_MILLIS, lockDoc.getLong(MongoLockRepository.TTL_MS));
        var expireAt0 = lockDoc.getLong(MongoLockRepository.EXPIRES_AT);

        // wait for at least 2 heartbeats
        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS);
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt1 = lockDoc.getLong(MongoLockRepository.EXPIRES_AT);
        assertEquals(expireAt0, expireAt1);

        // lock extension
        assertTrue(lock.tryLock());
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);
        var expireAt2 = lockDoc.getLong(MongoLockRepository.EXPIRES_AT);
        assertTrue(expireAt1 < expireAt2);

        // lock release
        lock.unlock();
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLockAcquisitionWithExplicitRelease() {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());
        lock1.unlock();
        assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void checkLockAcquisitionWithExponentialBackoffAndJitter() throws InterruptedException {
        testAppender.setlogLevel(Level.TRACE);

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, 1, TimeUnit.MINUTES);
        var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock(5, TimeUnit.SECONDS));

        var deltas = StreamEx.of(testAppender.getLogs())
                .filter(entry -> entry.getMessage().contains("acquisition attempt"))
                .map(ILoggingEvent::getInstant)
                .map(Instant::toEpochMilli)
                .pairMap((a, b) -> b - a)
                .pairMap((a, b) -> (double) b / a)
                .toList();

        deltas = new ArrayList<>(deltas);
        deltas.remove(deltas.size() - 1);
        assertEquals(deltas.size(), deltas.stream().filter(v -> v > 1).count());
    }

    @Test
    void checkLockAcquisitionWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);

        assertTrue(lock1.tryLock());
        assertFalse(lock2.tryLock());

        Thread.sleep(DEFAULT_LOCK_TTL_MILLIS + 1);
        assertTrue(lock2.tryLock());

        lock1.unlock();
        var lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNotNull(lockDoc);

        lock2.unlock();
        lockDoc = LOCK_COLLECTION.find(eq("_id", lockId)).first();
        assertNull(lockDoc);
    }

    @Test
    void checkLock_noInterruption() throws InterruptedException {
        var exceptions = new ArrayList<Exception>();

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, 1, TimeUnit.HOURS);
        lock1.lockInterruptibly();

        var separateThread = new Thread(() -> {
            var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
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
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void checkLock_withInterruption() throws InterruptedException {
        var exceptions = new ArrayList<Exception>();

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, 1, TimeUnit.HOURS);
        lock1.lockInterruptibly();

        var separateThread = new Thread(() -> {
            var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
            assertFalse(lock2.tryLock());
            try {
                lock2.lockInterruptibly();
            } catch (InterruptedException ex) {
                exceptions.add(ex);
            }
        });
        separateThread.start();

        for (int i = 0; i < 50; ++i) {
            if (separateThread.isAlive()) {
                separateThread.join(400);
                separateThread.interrupt();
                if (!exceptions.isEmpty()) {
                    break;
                }
            }
        }

        assertEquals(1, exceptions.size());
        assertTrue(exceptions.get(0).getMessage().startsWith(format("Lock '%s' acquisition", lockId)));
    }

    @Test
    void checkTryLockWithDeadline() throws InterruptedException {
        testAppender.setlogLevel(Level.DEBUG);
        var exceptions = new ArrayList<Exception>();

        var lockId = String.valueOf(lockCounter.incrementAndGet());
        var lock1 = lockManager.createLease(lockId, 1, TimeUnit.MINUTES);
        assertTrue(lock1.tryLock(1, TimeUnit.MINUTES));

        var lock2 = lockManager.createLease(lockId, DEFAULT_LOCK_TTL_MILLIS, TimeUnit.MILLISECONDS);
        assertFalse(lock2.tryLock(1, TimeUnit.MILLISECONDS));

        var separateThread = new Thread(() -> {
            try {
                assertFalse(lock2.tryLock(1, TimeUnit.MINUTES));
            } catch (InterruptedException ex) {
                exceptions.add(ex);
            }
        });
        separateThread.start();

        for (int i = 0; i < 50; ++i) {
            if (separateThread.isAlive()) {
                separateThread.interrupt();
                if (!exceptions.isEmpty()) {
                    break;
                }
                separateThread.join(1000);
            }
        }

        assertFalse(exceptions.isEmpty());
        assertTrue(exceptions.get(0).getMessage().startsWith(format("Lock '%s' acquisition", lockId)));
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithoutExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.createLease(lockId, 1, TimeUnit.HOURS);
            try {
                assertTrue(lock.tryLock());
            } catch (MongoLockWrappingException e) {
                throw new RuntimeException(e);
            }
        });

        separateThread.start();
        separateThread.join();

        assertFalse(lockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithExplicitRelease() throws InterruptedException {
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        var separateThread = new Thread(() -> {
            var lock = lockManager.createLease(lockId, 1, TimeUnit.HOURS);
            try {
                assertTrue(lock.tryLock());
            } catch (MongoLockWrappingException e) {
                throw new RuntimeException(e);
            }
            lock.unlock();
        });

        separateThread.start();
        separateThread.join();

        assertTrue(lockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void checkLockAcquisitionInASeparateThreadWithReleaseOnException() throws InterruptedException {
        var lockId1 = String.valueOf(lockCounter.incrementAndGet());
        var lockId2 = String.valueOf(lockCounter.incrementAndGet());
        var lockId3 = String.valueOf(lockCounter.incrementAndGet());

        var separateThread1 = new Thread(() -> {
            try {
                assertTrue(lockManager.createLease(lockId1, 1, TimeUnit.HOURS).tryLock());
            } catch (MongoLockWrappingException e) {
                throw new RuntimeException(e);
            }
            throw new RuntimeException("expected1");
        });
        separateThread1.start();
        separateThread1.join();

        var separateThread2 = new Thread(() -> {
            try {
                assertTrue(lockManager.createLease(lockId2, 1, TimeUnit.HOURS).tryLock());
                assertTrue(lockManager.createLease(lockId3, 1, TimeUnit.HOURS).tryLock());
            } catch (MongoLockWrappingException e) {
                throw new RuntimeException(e);
            }
            throw new RuntimeException("expected2");
        });
        separateThread2.start();
        separateThread2.join();

        assertTrue(lockManager.createLease(lockId1, 1, TimeUnit.MILLISECONDS).tryLock());
        assertTrue(lockManager.createLease(lockId2, 1, TimeUnit.MILLISECONDS).tryLock());
        assertTrue(lockManager.createLease(lockId3, 1, TimeUnit.MILLISECONDS).tryLock());
    }

    @Test
    void checkLockDocumentDeletedByHeartbeat(EnvironmentVariables environmentVariables) throws InterruptedException {
        testAppender.setlogLevel(Level.TRACE);
        var lockId = String.valueOf(lockCounter.incrementAndGet());

        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, String.valueOf(MongoLockUtils.MIN_LOCK_TTL_MILLIS));
        var altLockManager = MongoLockManager.getInstance(MongoDB.getSyncClient(),
                DEFAULT_CONNECTION_ID,
                DEFAULT_LOCK_DATABASE_NAME,
                DEFAULT_LOCK_COLLECTION_NAME + "Test");
        altLockManager.getLockCollection().deleteMany(new Document());

        altLockManager.createLock(lockId, 310, TimeUnit.MILLISECONDS); // to start the heartbeat
        assertTrue(altLockManager.createLease(lockId, 1, TimeUnit.SECONDS).tryLock());
        assertFalse(altLockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS).tryLock());

        Thread.sleep((long) (3.5 * MongoLockUtils.MIN_LOCK_TTL_MILLIS));

        var record = testAppender.getLogs().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("deleteCount: 1"))
                .findAny();
        assertTrue(record.isPresent(), "Heartbeat did not delete the lock document");

        testAppender.setlogLevel(Level.INFO);
        assertTrue(altLockManager.createLease(lockId, 1, TimeUnit.SECONDS).tryLock());
        assertFalse(altLockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
        Thread.sleep((long) (3.5 * MongoLockUtils.MIN_LOCK_TTL_MILLIS));
        assertTrue(altLockManager.createLease(lockId, 1, TimeUnit.MILLISECONDS).tryLock());
    }

}
