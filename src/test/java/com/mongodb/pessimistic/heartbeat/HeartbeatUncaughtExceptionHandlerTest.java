package com.mongodb.pessimistic.heartbeat;

import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatUncaughtExceptionHandlerTest {

    private final TestAppender testAppender = TestAppender.getInstance();

    @Test
    void checkUncaughtException() {
        var lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());
        var handler = new HeartbeatUncaughtExceptionHandler(lockManager, () -> {
            throw new RuntimeException("expected beat exception");
        });

        testAppender.clearLogs();
        handler.run();
        assertTrue(testAppender.getLogs().get(0).getFormattedMessage().contains("Unexpected error in heartbeat"));
    }

}