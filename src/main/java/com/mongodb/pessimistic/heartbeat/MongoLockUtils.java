package com.mongodb.pessimistic.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class for handling MongoDB-based pessimistic locking operations.
 * <p>
 * This class provides shared functionality for managing distributed locks, including:
 * <ul>
 *   <li>Constants used for lock metadata fields in MongoDB documents.</li>
 *   <li>Methods for managing lock acquisition and handling MongoDB write exceptions.</li>
 *   <li>Utility methods for starting and managing heartbeat threads.</li>
 * </ul>
 * </p>
 */
final class MongoLockUtils {

    /**
     * Logger instance for logging lock-related events and errors.
     */
    @SuppressWarnings("all")
    static final Logger LOGGER = LoggerFactory.getLogger(MongoLockManager.class.getName());


    // Minimum allowable TTL for a lock in milliseconds.
    static final long MIN_LOCK_TTL_MILLIS = 500;

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private MongoLockUtils() {
    }

    /**
     * Retrieves the effective lock TTL (time-to-live) in milliseconds.
     * <p>
     * The TTL is determined based on the value of the environment variable {@code DEFAULT_LOCK_TTL_MILLIS_KEY}.
     * If the environment variable is not set or contains an invalid value, the default TTL is used.
     * </p>
     *
     * @return the effective lock TTL in milliseconds.
     */
    static long getEffectiveLockTtlMillis() {
        long defaultLockTtlMillis = MongoLockManager.DEFAULT_LOCK_TTL_MILLIS;
        try {
            // Check if the environment variable is set and parse its value
            var envTtl = System.getenv(MongoLockManager.DEFAULT_LOCK_TTL_MILLIS_KEY);
            if (envTtl != null) {
                // Use the parsed value, ensuring it meets the minimum TTL requirement
                defaultLockTtlMillis = Math.max(Long.parseLong(envTtl), MIN_LOCK_TTL_MILLIS);
            }
        } catch (RuntimeException ex) {
            // Log an error if the environment variable contains an invalid value
            LOGGER.error("Could not parse '" + MongoLockManager.DEFAULT_LOCK_TTL_MILLIS_KEY + "' environment variable content", ex);
        }
        return defaultLockTtlMillis;
    }

    /**
     * Starts a heartbeat mechanism to periodically execute a command.
     * <p>
     * The heartbeat is implemented using a {@link ScheduledExecutorService} that repeatedly invokes the
     * specified command at a fixed interval. If a heartbeat is already running, this method does not start a new one.
     * </p>
     *
     * @param heartbeat          a reference to the heartbeat executor service.
     * @param command            the command to execute periodically.
     * @param beatIntervalMillis the interval between executions in milliseconds.
     */
    static void startHeartbeat(AtomicReference<ScheduledExecutorService> heartbeat, Runnable command, long beatIntervalMillis) {
        var newHeartbeat = Executors.newScheduledThreadPool(1);
        if (heartbeat.compareAndSet(null, newHeartbeat)) {
            // Schedule the heartbeat task at a fixed rate
            newHeartbeat.scheduleAtFixedRate(command, 0, beatIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            // If another thread initialized the heartbeat first, shut down the newly created executor to avoid leaks
            newHeartbeat.shutdown();
        }
    }

}
