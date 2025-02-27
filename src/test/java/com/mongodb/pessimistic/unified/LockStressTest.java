package com.mongodb.pessimistic.unified;

import ch.qos.logback.classic.Level;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.unified.MongoLockManager.DEFAULT_LOCK_COLLECTION_NAME;
import static com.mongodb.pessimistic.unified.MongoLockManager.DEFAULT_LOCK_DATABASE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class LockStressTest {

    protected final TestAppender testAppender = TestAppender.getInstance();
    private final MongoCollection<Document> lockCollection;
    protected final MongoCollection<Document> stressCollection;

    public LockStressTest() {
        this.stressCollection = MongoDB.getSyncClient()
                .getDatabase("test")
                .getCollection("tailableLockStressTest");
        this.lockCollection = MongoDB.getSyncClient()
                .getDatabase(DEFAULT_LOCK_DATABASE_NAME)
                .getCollection(DEFAULT_LOCK_COLLECTION_NAME);
    }

    void incrementCount(String id) {
        var count = readCount(id);
        stressCollection.updateOne(eq("_id", id), new Document("$set", new Document("counter", count + 1)));
    }

    int readCount(String id) {
        return Objects.requireNonNull(stressCollection.find(eq("_id", id)).first()).getInteger("counter");
    }

    @BeforeEach
    void cleanup() {
        lockCollection.deleteMany(new Document());
        stressCollection.deleteMany(new Document());
        stressCollection.insertOne(new Document("_id", "guarded").append("counter", 0));
        stressCollection.insertOne(new Document("_id", "unguarded").append("counter", 0));
        testAppender.clearLogs();
        testAppender.setlogLevel(Level.INFO);
    }

    @Test
    void pessimisticLock() {
        var loadCount = 64;
        var threads = new ArrayList<Thread>();

        IntStream.range(0, loadCount).forEach(threadNo -> {
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

        assertEquals(loadCount, readCount("guarded"));
        assertTrue(loadCount > readCount("unguarded"),
                "Try increasing the number of parallel threads to make this test more relevant");
    }

    private void performUpdates(int threadNo) {
        System.err.printf("In thread #%d%n", threadNo);

        incrementCount("unguarded");
        var random = new Random();
        var locker = MongoLockManager.getInstance(MongoDB.getSyncClient());
        var lock = locker.newLock("guarded");
        lock.lock();
        var countBefore = readCount("guarded");
        incrementCount("guarded");
        var countAfter = readCount("guarded");

        System.err.printf("Thread #%d updated count from %d to %d%n", threadNo, countBefore, countAfter);

        switch (random.nextInt(3)) {
            case 0:
                lock.unlock();
                break;
            case 1:
                throw new RuntimeException("Testing ReactiveBeatLocker");
        }
    }

}