package com.mongodb.pessimistic.heartbeat;

/**
 * A thread-specific uncaught exception handler for releasing distributed locks.
 *
 * <p>
 * This handler ensures that if a thread holding a distributed lock terminates unexpectedly
 * due to an uncaught exception, the lock is released automatically. This is critical for
 * preventing resource contention or deadlocks in a distributed system.
 * </p>
 *
 * <p>
 * The {@code ThreadUncaughtExceptionHandler} wraps the default uncaught exception handler
 * for the thread. It first attempts to release the lock owned by the thread, and then
 * delegates the exception handling to the original handler (if any).
 * </p>
 *
 * <p>
 * This mechanism ensures that the distributed lock system remains robust and does not
 * leave locks in an unreleased state due to unexpected thread termination.
 * </p>
 *
 * @see Thread#setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler)
 * @see MongoLockImpl
 */
final class ThreadUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final MongoLockImpl lock;
    private final Thread.UncaughtExceptionHandler prevUeh;

    /**
     * Constructs a new {@code ThreadUncaughtExceptionHandler}.
     *
     * @param lock   the {@link MongoLockImpl} instance associated with the thread.
     *               This lock will be released if an uncaught exception occurs.
     * @param prevUeh the previous uncaught exception handler for the thread.
     *                If non-null, the exception will be delegated to this handler
     *                after the lock is released. If null, a no-op handler is used.
     */
    ThreadUncaughtExceptionHandler(MongoLockImpl lock, Thread.UncaughtExceptionHandler prevUeh) {
        this.lock = lock;
        // If no previous handler exists, use a no-op handler to avoid null checks later.
        this.prevUeh = prevUeh == null ? (t, e) -> {
        } : prevUeh;
    }

    /**
     * Handles uncaught exceptions in the thread and ensures the associated lock is released.
     *
     * <p>
     * This method is invoked automatically by the JVM when an uncaught exception
     * occurs in the thread. It performs the following steps:
     * <ul>
     *   <li>Releases the lock held by the thread (if any).</li>
     *   <li>Delegates the exception handling to the previous uncaught exception handler.</li>
     * </ul>
     * </p>
     *
     * @param t the thread in which the exception occurred.
     * @param e the uncaught exception.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            // Attempt to release the lock to prevent resource contention or deadlocks.
            lock.unlock("uncaught exception handler");
        } finally {
            // Delegate the exception to the previous uncaught exception handler, if any.
            prevUeh.uncaughtException(t, e);
        }
    }

}
