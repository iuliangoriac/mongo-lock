package com.mongodb.pessimistic.tailable;

import ch.qos.logback.classic.Level;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.mongodb.pessimistic.tailable.TailableUtils.TAILABLE_CURSOR_MAX_AWAIT_TIME_MILLIS;
import static org.junit.jupiter.api.Assertions.*;

class TailableManagerTest {

    private final TestAppender testAppender = TestAppender.getInstance();
    private final TailableManager lockManager = TailableManager.of(MongoDB.getSyncClient());

    @BeforeEach
    void cleanup() {
        testAppender.clearLogs();
    }

    @Test
    void checkAccessRights() {
        testAppender.setlogLevel(Level.DEBUG);
        var adminDatabase = MongoDB.getSyncClient().getDatabase("admin");
        assertTrue(TailableUtils.snapshotTailableLocksFor(adminDatabase, "0").isEmpty());
    }

    @Test
    void checkLockAndRelease() throws TailableLockException {
        var tailableId = "1";
        testAppender.setlogLevel(Level.DEBUG);
        var adminDatabase = MongoDB.getSyncClient().getDatabase("admin");
        assertTrue(TailableUtils.snapshotTailableLocksFor(adminDatabase, tailableId).isEmpty());
        TailableLock outerLock;
        try (var lock = lockManager.acquireLock(tailableId)) {
            assertTrue(lock.isActive());
            outerLock = lock;
        }
        assertFalse(outerLock.isActive());

        try (var lock = lockManager.acquireLock(tailableId, 100)) {
            assertTrue(lock.isActive());
        }
    }

    @Test
    void checkLockAcquisitionTimeout() throws TailableLockException {
        var tailableId = "2";
        testAppender.setlogLevel(Level.TRACE);
        try (var lock1 = lockManager.acquireLock(tailableId)) {
            assertTrue(lock1.isActive());
            try (var ignore = lockManager.acquireLock(tailableId, 1000)) {
                fail("A second lock should not have been acquired");
            } catch (TailableLockException timeout) {
                assertEquals(TailableLockException.TIMEOUT, timeout.getErrorCode());
            }
        }
    }

    @Test
    void checkTailableContinuity() throws TailableLockException {
        var tailableId = "3";
        testAppender.setlogLevel(Level.INFO);
        var adminDatabase = MongoDB.getSyncClient().getDatabase("admin");
        try (var lock = lockManager.acquireLock(tailableId)) {
            var failedTailableChecks = 0;
            for (int i = 0; i < TAILABLE_CURSOR_MAX_AWAIT_TIME_MILLIS * 10; ++i) {
                if (TailableUtils.snapshotTailableLocksFor(adminDatabase, tailableId).isEmpty()) {
                    ++failedTailableChecks;
                }
                assertTrue(lock.isActive(), "After " + i + " iterations");
                if (failedTailableChecks > 1) {
                    break;
                }
            }
            assertTrue(failedTailableChecks > 0); // checks that single query failed at least once
        }
    }

    @Test
    void checkExplicitLockRelease() throws InterruptedException, TailableLockException {
        var tailableId = "4";
        testAppender.setlogLevel(Level.DEBUG);

        var worker = new Thread(() -> {
            try (var lock = lockManager.acquireLock(tailableId)) {
                assertTrue(lock.isActive());
            } catch (TailableLockException e) {
                e.printStackTrace(System.err);
            }
        });

        worker.start();
        worker.join();

        try (var lock = lockManager.acquireLock(tailableId, 1000)) {
            assertTrue(lock.isActive());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void checkAutomaticLockReleaseOnUnexpectedException() throws InterruptedException, TailableLockException {
        var tailableId = "5";
        testAppender.setlogLevel(Level.INFO);

        var worker = new Thread(() -> {
            try {
                assertTrue(lockManager.acquireLock(tailableId).isActive());
                assertTrue(lockManager.acquireLock(tailableId + "a").isActive());
                assertTrue(lockManager.acquireLock(tailableId + "b").isActive());
                throw new RuntimeException("Simulated uncaught exception");
            } catch (TailableLockException e) {
                e.printStackTrace(System.err);
            }
        });

        worker.start();
        worker.join();

        for (var i = 0; i < 10; ++i) { // make sure the lock handle threads had a chance to complete
            var closeCount = 0;
            for (var entry : testAppender.getLogs()) {
                var message = entry.getFormattedMessage();
                if (message.contains("was released explicitly")) {
                    if (message.contains(tailableId)
                            || message.contains(tailableId + "a")
                            || message.contains(tailableId + "b")) {
                        ++closeCount;
                    }
                }
            }
            if (closeCount >= 3) {
                return;
            }
            Thread.sleep(100);
        }

        try (var lock = lockManager.acquireLock(tailableId, 1000)) {
            assertTrue(lock.isActive());
        }
        try (var lock = lockManager.acquireLock(tailableId + "a", 1000)) {
            assertTrue(lock.isActive());
        }
        try (var lock = lockManager.acquireLock(tailableId + "b", 1000)) {
            assertTrue(lock.isActive());
        }
    }

    @Test
    @SuppressWarnings("resource")
    @Disabled
    void checkAutomaticLockReleaseOnNormalThreadEnd() throws InterruptedException, TailableLockException {
        var tailableId = "6";
        testAppender.setlogLevel(Level.INFO);

        var worker = new Thread(() -> {
            try {
                var lock = lockManager.acquireLock(tailableId);
                assertTrue(lock.isActive());
            } catch (TailableLockException e) {
                e.printStackTrace(System.err);
            }
        });

        worker.start();
        worker.join();

        try (var lock = lockManager.acquireLock(tailableId, 1000)) {
            assertTrue(lock.isActive());
        }
    }

}
