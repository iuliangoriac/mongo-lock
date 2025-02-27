package com.mongodb.pessimistic.reactivebeat;

import ch.qos.logback.classic.Level;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.mongodb.pessimistic.reactivebeat.ReactiveBeatLocker.BEAT_INTERVAL_IN_MILLIS;
import static org.junit.jupiter.api.Assertions.*;

class ReactiveBeatLockerTest {

    private final TestAppender testAppender = TestAppender.getInstance();

    @BeforeEach
    void cleanup() {
        testAppender.clearLogs();
        testAppender.setlogLevel(Level.INFO);
    }

    @Test
    void getInstance() throws InterruptedException {
        testAppender.setlogLevel(Level.TRACE);

        IntStream.range(0, 10).forEach(ignore -> {
            var beatInitializer = new Thread(() -> ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient()));
            beatInitializer.start();
            try {
                beatInitializer.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertFalse(beatInitializer.isAlive());
        });

        Thread.sleep(5 * BEAT_INTERVAL_IN_MILLIS);

        var logEntry = testAppender.getLogs().stream()
                .filter(entry -> entry.getFormattedMessage().contains("Beat #3"))
                .toList();
        assertEquals(1, logEntry.size());
    }

    @Test
    void checkLockAndRelease() {
        var lockId = "1";
        var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
        var lock1 = locker.getLockFor(lockId);
        var lock2 = locker.getLockFor(lockId);
        lock1.lock();
        assertFalse(lock2.tryLock());
        lock1.unlock();
        assertTrue(lock2.tryLock());
        lock2.unlock();
    }

    @Test
    void checkLockAcquisitionTimeout() throws InterruptedException {
        var lockId = "2";
        var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
        var lock1 = locker.getLockFor(lockId);
        var lock2 = locker.getLockFor(lockId);
        lock1.lock();
        assertFalse(lock2.tryLock(2 * BEAT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS));
        lock1.unlock();
    }

    @Test
    @Timeout(BEAT_INTERVAL_IN_MILLIS)
    void checkExplicitLockRelease() throws InterruptedException {
        var lockId = "3";

        var worker = new Thread(() -> {
            var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
            var lock = locker.getLockFor(lockId);
            lock.lock();
            lock.unlock();
        });

        worker.start();
        worker.join();

        var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
        var lock = locker.getLockFor(lockId);
        lock.lock();
        lock.unlock();
    }

    @Test
    @Timeout(2 * BEAT_INTERVAL_IN_MILLIS)
    void checkAutomaticLockReleaseOnNormalThreadEnd() throws InterruptedException {
        var lockId = "4";

        var worker = new Thread(() -> {
            var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
            locker.getLockFor(lockId).lock();
        });

        worker.start();
        worker.join();

        var locker = ReactiveBeatLocker.getInstance(MongoDB.getReactiveClient());
        var lock = locker.getLockFor(lockId);
        lock.lock();
        lock.unlock();
    }

}