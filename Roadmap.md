### Lock Fairness
- The `lock()` and `lockInterruptibly()` methods use a loop to repeatedly attempt lock acquisition. If multiple threads are waiting for the same lock, this can lead to contention.
  Introduce a fairness mechanism to ensure that waiting threads acquire the lock in the order they requested it. For example, a queue can be used to manage waiting threads.
- Allow users to configure the backoff strategy (e.g., linear, exponential) and jitter parameters to suit their application's requirements. Maybe use specific strategy classes that encapsulate this configuration.
- ?? Introduce a MongoDB based notification system for locks (would tailable cursors (that poll the db) be more efficient than backoff based strategy).


### Client / connection management
- what should happen if the client gets closed and then recreated
- so far only the situation of the client singleton is being considered


### Use structured logging

- Use structured logging to include metadata (e.g., lock ID, owner ID, TTL) in log messages for easier debugging.


### Try-With-Resources for Locking

- Use a `try-with-resources` pattern to ensure the lock is always released, even if an exception occurs.
- Have the MongoLock expose an overloaded method asAutoClosable() that will call lockInterruptibly or tryLock that will throw MongoLockAcquisitionException in case of failure
- Example:
    ```java
    try (AutoCloseableLock ignored = new AutoCloseableLock(lock)) {
      // Critical section
    }
    ```