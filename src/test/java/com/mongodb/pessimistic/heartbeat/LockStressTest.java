package com.mongodb.pessimistic.heartbeat;

import ch.qos.logback.classic.Level;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.heartbeat.MongoLockManager.DEFAULT_LOCK_COLLECTION_NAME;
import static com.mongodb.pessimistic.heartbeat.MongoLockManager.DEFAULT_LOCK_DATABASE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class LockStressTest {

    private static final int THREAD_LOAD_COUNT = 128;

    private static final MongoCollection<Document> LOCK_COLLECTION;
    private static final MongoCollection<Document> STRESS_COLLECTION;

    private final TestAppender testAppender = TestAppender.getInstance();

    static {
        LOCK_COLLECTION = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME);
        STRESS_COLLECTION = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME + "Test");
    }

    @AfterAll
    static void cleanupAll() {
        LOCK_COLLECTION.deleteMany(new Document());
        STRESS_COLLECTION.drop();
    }

    void incrementCount(String id) {
        var count = readCount(id);
        STRESS_COLLECTION.updateOne(eq("_id", id), new Document("$set", new Document("counter", count + 1)));
    }

    int readCount(String id) {
        return Objects.requireNonNull(STRESS_COLLECTION.find(eq("_id", id)).first()).getInteger("counter");
    }

    @BeforeEach
    void cleanup() {
        LOCK_COLLECTION.deleteMany(new Document());
        STRESS_COLLECTION.deleteMany(new Document());
        STRESS_COLLECTION.insertOne(new Document("_id", "guarded").append("counter", 0));
        STRESS_COLLECTION.insertOne(new Document("_id", "unguarded").append("counter", 0));
        testAppender.clearLogs();
        testAppender.setlogLevel(Level.WARN);
    }

    @Test
    void pessimisticLock() {
        testAppender.setlogLevel(Level.INFO);


        var threads = new ArrayList<Thread>();

        IntStream.range(0, THREAD_LOAD_COUNT).forEach(threadNo -> {
            var worker = new Thread(() -> performUpdates(threadNo + 1));
            threads.add(worker);
            worker.start();
        });

        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(THREAD_LOAD_COUNT, readCount("guarded"));
        assertTrue(THREAD_LOAD_COUNT > readCount("unguarded"),
                "Try increasing the number of parallel threads to make this test more relevant");
    }

    private void performUpdates(int threadNo) {
        System.err.printf("In thread #%d%n", threadNo);

        incrementCount("unguarded");

        var locker = MongoLockManager.getInstance(MongoDB.getSyncClient());
        var lock = locker.createLock("guarded");
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        var countBefore = readCount("guarded");
        incrementCount("guarded");
        var countAfter = readCount("guarded");

        System.err.printf("Thread #%d updated count from %d to %d%n", threadNo, countBefore, countAfter);

        var path = new Random().nextInt(3);
        switch (path) {
            case 0:
                break;
            case 1:
                throw new RuntimeException("Testing ReactiveBeatLocker");
            default:
                lock.unlock();
        }
    }

}