package com.mongodb.pessimistic.heartbeat;

import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class ThreadUncaughtExceptionHandlerTest {

    public ThreadUncaughtExceptionHandlerTest() {
        TestAppender.getInstance();
    }

    @Test
    void checkUncaughtException() {
        var lockManager = MongoLockManager.getInstance(MongoDB.getSyncClient());
        var lock = lockManager.createLease("0", 1, TimeUnit.MILLISECONDS);
        var handler = new ThreadUncaughtExceptionHandler((MongoLockImpl) lock, null);
        handler.uncaughtException(null, null);
    }

}