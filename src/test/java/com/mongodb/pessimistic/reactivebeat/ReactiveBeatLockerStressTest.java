package com.mongodb.pessimistic.reactivebeat;

import ch.qos.logback.classic.Level;
import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReactiveBeatLockerStressTest {

    private final TestAppender testAppender = TestAppender.getInstance();
    private MongoCollection<Document> stressCollection;

    @BeforeEach
    void cleanup() {
        testAppender.clearLogs();
        stressCollection = MongoDB.getSyncClient().getDatabase("test").getCollection("tailableLockStressTest");
        stressCollection.deleteMany(new Document());
        stressCollection.insertOne(new Document("_id", "guarded").append("counter", 0));
        stressCollection.insertOne(new Document("_id", "unguarded").append("counter", 0));
    }

    @Test
    void dbOperations() {
        testAppender.setlogLevel(Level.DEBUG);
        assertEquals(0, readCount("guarded"));
        assertEquals(0, readCount("unguarded"));
        incrementCount("unguarded");
        assertEquals(1, readCount("unguarded"));
    }

    @Test
    void pessimisticLock() {
        testAppender.setlogLevel(Level.INFO);

        var loadCount = 64;
        var threads = new ArrayList<Thread>();

        IntStream.range(0, loadCount).forEach(ignore -> {
            var worker = new Thread(this::performUpdates);
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

    void incrementCount(String id) {
        var count = readCount(id);
        stressCollection.updateOne(eq("_id", id), new Document("$set", new Document("counter", count + 1)));
    }

    int readCount(String id) {
        return Objects.requireNonNull(stressCollection.find(eq("_id", id)).first()).getInteger("counter");
    }

    void performUpdates() {
        incrementCount("unguarded");
        var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
        var lock = locker.getLockFor("guarded");
        lock.lock();
        incrementCount("guarded");
        switch (new Random().nextInt(5)) {
            case 0:
                return;
            case 1:
                throw new RuntimeException("Testing ReactiveBeatLocker");
            default:
                lock.unlock();
        }
    }

}
