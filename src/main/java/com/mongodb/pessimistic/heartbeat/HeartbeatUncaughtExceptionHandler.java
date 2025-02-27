package com.mongodb.pessimistic.heartbeat;

import static com.mongodb.pessimistic.heartbeat.MongoLockUtils.LOGGER;

/**
 * A wrapper for the heartbeat mechanism that ensures exceptions occurring
 * during heartbeat execution are caught and logged without disrupting the
 * heartbeat thread.
 *
 * <p>
 * This class acts as an exception handler for the heartbeat logic in the
 * {@link MongoLockManager}. If any unexpected exception occurs during the
 * execution of the heartbeat, it logs the exception and allows the heartbeat
 * thread to continue operating without termination.
 * </p>
 *
 * <p>
 * By wrapping the heartbeat logic with this handler, the system ensures that
 * transient or unexpected errors do not cause the heartbeat mechanism to stop,
 * which is critical for maintaining the health and consistency of managed locks.
 * </p>
 */
public final class HeartbeatUncaughtExceptionHandler implements Runnable {

    private final MongoLockManager lockManager;
    private final Runnable wrapped;

    /**
     * Constructs a new {@code HeartbeatUncaughtExceptionHandler}.
     *
     * @param lockManager the {@link MongoLockManager} instance responsible for managing locks.
     * @param wrapped     the heartbeat logic to be executed, wrapped with exception handling.
     */
    HeartbeatUncaughtExceptionHandler(MongoLockManager lockManager, Runnable wrapped) {
        this.lockManager = lockManager;
        this.wrapped = wrapped;
    }

    /**
     * Executes the wrapped heartbeat logic, catching and logging any unexpected exceptions.
     *
     * <p>
     * This method ensures that any exceptions thrown during the execution of the
     * heartbeat logic do not terminate the heartbeat thread. Instead, it logs the
     * exception and allows the thread to continue running.
     * </p>
     *
     * <p>
     * The log message includes the current heartbeat number (retrieved from the
     * {@link MongoLockManager}) to aid in debugging and monitoring.
     * </p>
     */
    @Override
    public void run() {
        try {
            // Execute the wrapped heartbeat logic.
            wrapped.run();
        } catch (Exception trapped) {
            LOGGER.error("Unexpected error in heartbeat #{}:", lockManager.getBeatNo(), trapped);
        }
    }

}
