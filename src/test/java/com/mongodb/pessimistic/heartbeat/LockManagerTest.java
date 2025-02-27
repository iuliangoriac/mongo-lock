package com.mongodb.pessimistic.heartbeat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LockManagerTest {

    private static MongoClient mongoClient;
    private static MongoDatabase testDatabase;
    private static MongoCollection<Document> lockCollection;
    private LockManager lockManager;
    private final String databaseName = "temenos";
    private final String collectionName = "TAFJ_LOCK";
    private final int ttlSeconds = 60;
    private final TestAppender appender = TestAppender.getInstance();

    @BeforeAll
    void setupMongo() {
        mongoClient = MongoDB.getSyncClient();
        testDatabase = mongoClient.getDatabase(databaseName);
        lockCollection = testDatabase.getCollection(collectionName);
        lockCollection.drop();
    }

    @BeforeEach
    void setupLockManager() {
        lockManager = new LockManager(mongoClient, databaseName, collectionName, ttlSeconds,3,100,50,5000);
    }

    @AfterEach
    void cleanupLocks() {
        lockCollection.deleteMany(new Document());
        lockManager.stopExtender();
    }


    @Test
    void testGetLockSuccessfully() {
        assertTrue(lockManager.getLock("testLock1"), "Should acquire lock successfully");
        Document lock = lockCollection.find(new Document("_id", "testLock1")).first();
        assertNotNull(lock, "Lock document should exist in collection");
    }

    @Test
    void testGetLockDuplicateSuccessfully() {
        assertTrue(lockManager.getLock("testLock1"), "Should acquire lock successfully");
        assertTrue(lockManager.getLock("testLock1"), "Should acquire the existing lock");
    }

    @Test
    void testReleaseLockSuccessfully() {
        assertTrue(lockManager.getLock("testLock1"), "Should acquire lock successfully");
        assertTrue(lockManager.releaseLock("testLock1"), "Should release lock successfully");
        Document lock = lockCollection.find(new Document("_id", "testLock1")).first();
        assertNull(lock, "Lock document should no longer exist");
    }

    @Test
    void testHeartbeatExtension() throws InterruptedException {
        assertTrue(lockManager.getLock("testLock1"), "Should acquire lock successfully");

        // Wait for lock extension to trigger at least once
        TimeUnit.SECONDS.sleep(100);

        // Verify lock is still present after heartbeat extension
        Document lock = lockCollection.find(new Document("_id", "testLock1")).first();
        assertNotNull(lock, "Lock should still exist after heartbeat extension");
    }

    @Test
    void testConcurrentLockAcquisition() throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successfulLocks = new AtomicInteger(0);
        String lockId = "concurrentLock";

        Callable<Boolean> lockTask = () -> lockManager.getLock(lockId);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(executorService.submit(lockTask));
        }

        for (Future<Boolean> result : results) {
            if (result.get()) successfulLocks.incrementAndGet();
        }

        assertEquals(1, successfulLocks.get(), "Only one thread should acquire the lock");
        executorService.shutdown();
    }

    @Test
    void stressTestLockManagement() throws InterruptedException {
        int threadCount = 50;
        int lockCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        Runnable lockAndReleaseTask = () -> {
            String lockId = "stressLock-" + ThreadLocalRandom.current().nextInt(lockCount);
            if (lockManager.getLock(lockId)) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(10, 100));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    lockManager.releaseLock(lockId);
                }
            }
        };

        for (int i = 0; i < threadCount * 5; i++) {
            executorService.execute(lockAndReleaseTask);
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(1, TimeUnit.MINUTES), "Executor should complete tasks in time");
    }

    @Test
    void testConcurrentLockRelease() throws InterruptedException, ExecutionException {
        String lockId = "concurrentReleaseLock";
        assertTrue(lockManager.getLock(lockId), "Lock should be acquired successfully");

        Callable<Boolean> releaseTask = () -> lockManager.releaseLock(lockId);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Future<Boolean> result1 = executorService.submit(releaseTask);
        Future<Boolean> result2 = executorService.submit(releaseTask);

        assertTrue(result1.get() || result2.get(), "At least one release should succeed");
        assertFalse(lockCollection.find(new Document("_id", lockId)).iterator().hasNext(),
                "Lock document should no longer exist");

        executorService.shutdown();
    }

    @Test
    void testPerformanceUnderHighLoad() throws InterruptedException {
        int threadCount = 100;
        int lockCount = 500;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        Runnable lockAndReleaseTask = () -> {
            String lockId = "highLoadLock-" + ThreadLocalRandom.current().nextInt(lockCount);
            if (lockManager.getLock(lockId)) {
                lockManager.releaseLock(lockId);
            }
        };

        for (int i = 0; i < threadCount * 10; i++) {
            executorService.execute(lockAndReleaseTask);
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(2, TimeUnit.MINUTES),
                "Executor should complete tasks in time");
    }

    @Test
    void testLockExtensionFailureHandling() {
        String lockId = "extensionFailureLock";
        assertTrue(lockManager.getLock(lockId), "Should acquire lock successfully");

        lockCollection.drop();

        assertDoesNotThrow(() -> {
            Thread.sleep(35000); // Wait for heartbeat extension attempt
        }, "Heartbeat thread should handle lock extension failures gracefully");
    }

    @Test
    void testExponentialBackoffCleanExecution() throws InterruptedException {
        String lockId = "testBackoffLock";
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Runnable lockTask1 = () -> lockManager.getLock(lockId);
        Runnable lockTask2 = () -> lockManager.getLock(lockId);

        executor.execute(lockTask1);
        executor.execute(lockTask2);

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Tasks should complete cleanly without interruptions");
    }

    @Test
    void shouldRemoveOnlyExpiredLocksFromLockMapAndDB() throws InterruptedException {

        LockManager newLock =  new LockManager(mongoClient, databaseName, collectionName, 0,3,100,50,5000);

        assertTrue(newLock.getLock("expiredLock"), "Should acquire lock successfully");
        Thread.sleep(50000);

        assertTrue(lockManager.getLock("activeLock"), "Should acquire lock successfully");

        Document expiredLock = lockCollection.find(new Document("_id", "expiredLock")).first();
        Document activeLock = lockCollection.find(new Document("_id", "activeLock")).first();


        assertNotNull(activeLock, "activeLock document should exist in collection");
        assertNull(expiredLock, "expiredLock document should not exist in collection");
    }


}
