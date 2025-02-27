package com.mongodb.pessimistic.heartbeat;

/**
 * A custom runtime exception used to wrap and propagate exceptions encountered during MongoDB-based
 * pessimistic lock operations.
 *
 * <p>
 * The {@code MongoLockWrappingException} provides additional context for exceptions that occur
 * during lock-related operations, such as lock acquisition, renewal, or release. It is used to
 * encapsulate low-level exceptions (e.g., {@link com.mongodb.MongoWriteException}) and present
 * them in a higher-level abstraction relevant to the lock management domain.
 * </p>
 *
 * <p>
 * This exception is typically thrown in scenarios where a lock operation fails due to reasons
 * such as:
 * <ul>
 *   <li>Duplicate key errors when attempting to acquire a lock that is already held.</li>
 *   <li>MongoDB write conflicts or other database-related issues.</li>
 *   <li>Unexpected runtime exceptions during lock management.</li>
 * </ul>
 * </p>
 *
 * <p>
 * By wrapping the original exception, the {@code MongoLockWrappingException} preserves the
 * root cause for debugging and troubleshooting while providing a meaningful message for
 * higher-level application logic.
 * </p>
 *
 * <p>
 * Example usage:
 * <pre>
 * try {
 *     // Attempt to acquire a lock
 *     lockManager.createLock("myLock").tryLock();
 * } catch (MongoWriteException ex) {
 *     throw new MongoLockWrappingException("Failed to acquire lock for 'myLock'", ex);
 * }
 * </pre>
 * </p>
 */
public class MongoLockWrappingException extends RuntimeException {

    /**
     * Constructs a new {@code MongoLockWrappingException} with the specified detail message and cause.
     *
     * @param message a descriptive message providing context about the exception.
     * @param cause   the underlying exception that caused this exception to be thrown.
     */
    public MongoLockWrappingException(String message, Throwable cause) {
        super(message, cause);
    }

}
