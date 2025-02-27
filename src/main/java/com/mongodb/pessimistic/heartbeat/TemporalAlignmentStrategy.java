package com.mongodb.pessimistic.heartbeat;

import java.util.concurrent.ThreadLocalRandom;

import static java.lang.String.format;

/**
 * A strategy for managing temporal alignment in distributed lock systems.
 * <p>
 * This class is responsible for calculating various time-related parameters for lock management,
 * such as lock TTL (time-to-live), heartbeat intervals, and exponential backoff delays for retrying
 * lock acquisition. It ensures that the lock's expiration and heartbeat timings are aligned
 * to minimize contention and optimize resource usage in a distributed environment.
 * </p>
 * <p>
 * Key Features:
 * <ul>
 *     <li>Calculates buffered TTL to account for clock drift and ensure lock safety.</li>
 *     <li>Determines heartbeat intervals for periodic lock renewal.</li>
 *     <li>Implements exponential backoff with jitter for retrying lock acquisition attempts.</li>
 *     <li>Enforces a maximum delay to prevent excessive wait times during retries.</li>
 * </ul>
 * </p>
 */
public class TemporalAlignmentStrategy {

    /**
     * Creates a default temporal alignment strategy based on the provided lock TTL.
     * <p>
     * The default strategy divides the lock TTL into three equal parts for the heartbeat interval,
     * applies a buffer coefficient for safety, and calculates backoff and jitter parameters
     * to ensure efficient retries.
     * </p>
     *
     * @param ttlMillis the lock TTL (time-to-live) in milliseconds.
     * @return a new instance of {@code TemporalAlignmentStrategy}.
     */
    public static TemporalAlignmentStrategy getDefault(long ttlMillis) {
        long beatIntervalMillis = ttlMillis / 3;
        long baseBackoffMillis = beatIntervalMillis / 10;
        long maxJitter = baseBackoffMillis / 2;
        long maxDelay = 3 * beatIntervalMillis;
        return new TemporalAlignmentStrategy(ttlMillis, 1.1, beatIntervalMillis, baseBackoffMillis, maxJitter, maxDelay);
    }

    private final long lockTtlMillis;
    private final double lockTtlBufferCoefficient;
    private final long beatIntervalMillis;
    private final long baseBackoffMillis;
    private final long maxJitter;
    private final long maxDelay;
    private final int attemptCountThreshold;

    /**
     * Constructs a new {@code TemporalAlignmentStrategy} with the specified parameters.
     *
     * @param lockTtlMillis            the lock TTL (time-to-live) in milliseconds.
     * @param lockTtlBufferCoefficient the coefficient to calculate a buffered TTL.
     * @param beatIntervalMillis       the interval between heartbeats in milliseconds.
     * @param baseBackoffMillis        the base delay for exponential backoff in milliseconds.
     * @param maxJitter                the maximum random jitter added to the backoff delay.
     * @param maxDelay                 the maximum allowable delay for retries in milliseconds.
     */
    public TemporalAlignmentStrategy(long lockTtlMillis
            , double lockTtlBufferCoefficient
            , long beatIntervalMillis
            , long baseBackoffMillis
            , long maxJitter
            , long maxDelay) {
        this.lockTtlMillis = lockTtlMillis;
        this.lockTtlBufferCoefficient = lockTtlBufferCoefficient;
        this.beatIntervalMillis = beatIntervalMillis;
        this.baseBackoffMillis = baseBackoffMillis;
        this.maxJitter = maxJitter;
        this.maxDelay = maxDelay;

        // Calculate the threshold for the number of attempts before the delay reaches the maximum.
        this.attemptCountThreshold = (int) Math.ceil((Math.log(maxDelay / (double) baseBackoffMillis) / Math.log(2)));
    }

    /**
     * Retrieves the lock TTL (time-to-live) in milliseconds.
     *
     * @return the lock TTL in milliseconds.
     */
    public long getLockTtlMillis() {
        return lockTtlMillis;
    }

    /**
     * Calculates a buffered TTL to account for clock drift and ensure lock safety.
     * <p>
     * The buffered TTL is computed by applying a buffer coefficient to the provided TTL.
     * If the buffered TTL is too small to support at least two heartbeat intervals,
     * an exception is thrown to prevent unsafe lock configurations.
     * </p>
     *
     * @param ttlMillis the original TTL in milliseconds.
     * @return the buffered TTL in milliseconds.
     * @throws UnsupportedOperationException if the buffered TTL is too small for the heartbeat interval.
     */
    public long getBufferedLockTtlMillis(long ttlMillis) {
        var bufferedTtl = (long) (ttlMillis * lockTtlBufferCoefficient);
        if (bufferedTtl <= beatIntervalMillis * 2) {
            // Calculate the minimum recommended TTL to support at least two heartbeat intervals.
            var minRecommended = (long) Math.ceil(beatIntervalMillis * 2 / 1.1) + 1;
            throw new UnsupportedOperationException(
                    format("A lock TTL of %d millis is too small for a beat interval of %d millis. The minimum recommended is %d millis.",
                            ttlMillis, beatIntervalMillis, minRecommended));
        }
        return bufferedTtl;
    }

    /**
     * Retrieves the interval between heartbeats in milliseconds.
     *
     * @return the heartbeat interval in milliseconds.
     */
    public long getBeatIntervalMillis() {
        return beatIntervalMillis;
    }

    /**
     * Calculates the delay for the next lock acquisition attempt using exponential backoff.
     * <p>
     * The delay is calculated as follows:
     * <ul>
     *     <li>The base delay is multiplied by 2^attemptNo to implement exponential growth.</li>
     *     <li>A random jitter is added to the delay to prevent contention between competing threads.</li>
     *     <li>The delay is capped at the maximum allowable delay to prevent excessive wait times.</li>
     * </ul>
     * </p>
     *
     * @param attemptNo the number of attempts made so far.
     * @return the calculated delay in milliseconds.
     */
    public long calculateDelay(int attemptNo) {
        if (attemptNo > attemptCountThreshold) {
            // Cap the delay at the maximum allowable value if the attempt threshold is exceeded.
            return maxDelay;
        }
        // Calculate the exponential backoff delay.
        var backoffMillis = baseBackoffMillis * (1L << attemptNo); // 2^attemptNo scaling.
        // Add random jitter to the delay to avoid contention.
        var jitter = ThreadLocalRandom.current().nextLong(maxJitter << 1) - maxJitter; // [-maxJitter, +maxJitter].
        // Return the delay, capped at the maximum allowable value.
        return Math.min(backoffMillis + jitter, maxDelay);
    }

}
