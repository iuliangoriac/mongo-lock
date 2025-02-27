package com.mongodb.pessimistic.tailable;

import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.pessimistic.tailable.SyncLockManager.LOCK_COLLECTION_NAME;
import static com.mongodb.pessimistic.tailable.SyncLockManager.TAILABLE_DB_NAME;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated
public class SyncLockManagerTest {

    private final TestAppender appender = TestAppender.getInstance();

    private final SyncLockManager syncLockManager = SyncLockManager.of(MongoDB.getSyncClient());

    @BeforeEach
    void cleanup() {
        MongoDB.getSyncClient()
                .getDatabase(TAILABLE_DB_NAME)
                .getCollection(LOCK_COLLECTION_NAME)
                .deleteMany(new Document());
        appender.clearLogs();
    }

    @Test
    void checkReentrantLock() {
        assertTrue(syncLockManager.getLock("5", "USER1"));
        assertTrue(syncLockManager.getLock("5", "USER1"));
    }

    @Test
    void checkLockAndRelease() {
        assertTrue(syncLockManager.getLock("6", "USER1"));
        assertFalse(syncLockManager.getLock("6", "USER2"));
        assertTrue(syncLockManager.releaseLock("6", "USER1"));
        assertTrue(syncLockManager.getLock("6", "USER2"));
    }

}
