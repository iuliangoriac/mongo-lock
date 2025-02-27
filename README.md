# MongoDB Distributed Pessimistic Locks for Java

This article explores a reference implementation of a pessimistic locking mechanism backed by MongoDB,
tailored for distributed systems.
We will examine how this solution addresses key concerns such as
mutual exclusion, lock expiration, fault tolerance, and scalability,
providing a practical guide for developers seeking to integrate distributed locking into their applications.


## Why Pessimistic Locking?

Data integrity is the bedrock of any reliable application.
Starting with version 4.0, MongoDB introduced
[multi-document ACID transactions](https://www.mongodb.com/products/capabilities/transactions).
By default, MongoDB employs optimistic concurrency control to ensure isolation,
allowing transactions to operate on a snapshot of the data.
Changes are validated, and conflicts are detected at commit time.
While this approach scales well in scenarios with infrequent write conflicts,
high-contention environments with costly rollbacks and critical consistency requirements
may benefit from a more proactive approach,
such as custom-defined pessimistic locking mechanisms.
To address such needs,
we provide a reference Java (21+) implementation that not only secures multi-document transactions
but also serves as a versatile distributed locking solution backed by MongoDB.


## Basic Usage Example

The following example demonstrates how to use `MongoLock` to safeguard a critical section of code
against the classic Read-Update-Modify anti-pattern:

```java
// Initialize the MongoClient (MongoDB Java driver API)
MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

// Get a MongoLockManager instance
MongoLockManager lockManager = MongoLockManager.getInstance(mongoClient);

// Create a lock with a 4-second TTL
MongoLock lock = lockManager.createLock("L1", 4, TimeUnit.SECONDS);

try {
    // Attempt to acquire the lock, waiting up to 12 seconds
    if (lock.tryLock(12, TimeUnit.SECONDS)) { 
        
        // Access the target collection
        var collection = mongoClient.getDatabase("test").getCollection("collection");
        
        // Perform the Read-Update-Modify sequence safely
        var document = collection.find(eq("_id", id)).first();
        var nextCount = document.getInteger("counter") + 1;
        collection.updateOne(eq("_id", id), 
                   new Document("$set", new Document("counter", nextCount)));
    }
} catch (InterruptedException e) {
    Thread.currentThread().interrupt(); // Handle thread interruption
} finally {
    // Ensure the lock is released (idempotent operation)
    lock.unlock();
}
```
The `MongoLock` API adopts the familiar semantics of the `java.util.concurrent.locks.Lock` interface,
making it intuitive to use while abstracting away the underlying complexity of distributed locking.


## Implementation Details


### Lock attributes persistence

To ensure that the business logic collections remain isolated and unaffected,
the `MongoLock` mechanism operates on a dedicated collection (`pessimisticLocks`)
within a separate database (`MongoLocks`).

**Lock Document Structure**: Each lock is represented as a document with the following schema:
  ```json
  {
    "_id": "L1",   
    "ownerId": "88968b03-a40e-4f9f-8727-e16523feedb7",
    "ttlMs": 4400,
    "expiresAt": 1740398945776  
  }
  ```

**Field semantics**:
- **`_id`**: Unique identifier for the lock (or the set of resources used in the critical sections).
- **`ownerId`**: Identifies the unique pair (lock ID, owner thread)
- **`ttlMs`**: The lock’s time-to-live, including a 10% buffer for clock drift.
- **`expiresAt`**: The expiration timestamp, adjusted for server-client time differences.


### The lock manager

The `MongoLockManager` is responsible for managing lock persistence and
ensuring proper configuration of the underlying MongoDB collection.
It maintains an efficient, thread-safe Java collection of all active `MongoLock` instances.
A singleton **heartbeat scheduler** periodically updates the expiration time of active locks,
releases locks held by terminated threads, and cleans up expired lock documents.

Key methods include:
- **`updateServerTimeDelta()`**: Synchronizes the server-client time delta using
  MongoDB’s `$$NOW` aggregation variable and Java’s `System.currentTimeMillis()`.
- **`onBeat()`**: Implements the maintenance logic periodically called by the heartbeat.


#### The heartbeat

The heartbeat is a periodic background task that plays a vital role in maintaining the health and reliability
of distributed locks.
While the basic lock lifecycle ensures a minimal level of guarantees—such as leasing
a lock for a fixed duration—it has limitations.
For example, without additional safeguards,
it can be difficult to determine if a lock owner is still active or has failed,
or to dynamically adjust lock durations.

This is where the heartbeat comes in. It enhances the locking mechanism by periodically performing the following tasks:

- **Extending Active Locks**:
  Automatically updates the expiration time of active locks to prevent them from expiring prematurely.
  This is done in bulk for efficiency.
- **Releasing Locks from Terminated Threads**:
  Detects and releases locks held by threads that are no longer alive (using `.isAlive()`),
  even before their `expiresAt` timestamp.
- **Synchronizing Time**:
  Updates the server-client time delta to account for clock drift between the application and MongoDB server.
- **Cleaning Up Expired Locks**:
  Removes expired lock documents from the MongoDB collection to keep the database clean and efficient.

By performing these tasks, the heartbeat ensures that locks remain valid, stale locks are cleaned up,
and the system can reliably detect and handle failures.
This improves the overall robustness and performance of the distributed locking mechanism.


### The lock lifecycle

A `MongoLock` instance is created using `MongoLockManager.createLock(..)`,
but the lock is not active until `tryLock()` is called.
Lock acquisition and mutual exclusion are guaranteed by MongoDB’s atomic `insertOne` and `findOneAndUpdate`
operations, which use the **majority** write concern and **primary** read preference
to prevent deadlocks and split-brain scenarios.


#### Lock Acquisition Behavior
- If no lock document exists, a new document is created.
- If the lock is already held by the current thread, the expiration time is extended.
- If the lock is held by another thread and expired, ownership is transferred to the requesting thread.
- Retry attempts use exponential backoff with jitter to prevent livelocks.

Locks can be explicitly released using the `unlock()` method,
ensuring proper cleanup after the critical section is executed.

1. **Explicit Release**:
   The lock is manually released using the `.unlock()` method,
   ideally within a `finally` block to ensure proper cleanup.
2. **Unhandled Exception Handling**:
   If a thread terminates due to an unhandled exception,
   the lock is released via a custom `Thread.UncaughtExceptionHandler` using the `.unlock()` method.
3. **Heartbeat Monitoring**:
   A dedicated heartbeat thread monitors all registered locks.
   If the owner thread is no longer alive,
   the heartbeat explicitly releases the locks before they expire using the `.unlock()` method.
4. **Lock Reassignment**:
   If a lock expires, it can be automatically reassigned to another worker using the `.tryLock(..)` method.
5. **Database Cleanup**:
   The heartbeat thread can also delete expired lock entries directly from the database to prevent stale locks.


## Known limitations

The heartbeat logic is safeguarded by a dedicated exception handler,
but issues may arise if it stops or is starved.
Key risks include:

1. **Lock Expiration**:
   Lock TTLs may not be extended.
   This can be mitigated by using longer TTLs relative to the heartbeat interval
   or manually extending locks via `.tryLock()`.
2. **Time Delta Drift**:
   The server-client time delta may become outdated,
   which can be addressed by increasing the `lockTtlBufferCoefficient`.
3. **Delayed Lock Release**:
   While this may cause performance penalties,
   deadlocks are avoided since stopped heartbeats halt automatic extensions,
   allowing locks to be deleted or transferred during `.tryLock()`.

If a JVM crashes, the primary concern is performance.

Locks are reentrant, allowing repeated `.tryLock()` calls to extend TTLs.
However, creating multiple locks for the same thread and lock ID can lead to deadlocks
with `.lockInterruptibly()` or `.tryLock(0, ..)`.
To prevent this, `MongoLockManager` could throw an exception when attempting to
create multiple locks with the same ID for the same thread.

No fairness mechanism is implemented, but an `ownerId` queue with tailable cursors could address this.


## Advanced Configuration

When creating a `MongoLockManager`,
the API allows customization of the lock collection name and its associated database.

Given the unique nature of distributed systems, temporal synchronization strategies must be adaptable.
To address this, the `TemporalAlignmentStrategy` class encapsulates the configuration,
leveraging the following parameters:

- **`lockTtlMillis`**: Specifies the default time-to-live (TTL) for a lock in milliseconds.
- **`lockTtlBufferCoefficient`**: A multiplier to account for potential clock drift across replica set nodes.
- **`beatIntervalMillis`**: Defines the interval at which the `MongoLockManager.onBeat()` method is invoked.
- **`baseBackoffMillis`**: Sets the minimum delay between lock acquisition retries.
- **`maxJitter`**: Introduces a random jitter to the backoff delay, up to this maximum value.
- **`maxDelay`**: Specifies the upper limit for retry delays in milliseconds.

This design ensures flexibility and robustness,
enabling fine-tuned synchronization strategies tailored to the requirements of diverse distributed environments.


## Conclusion

This reference implementation of a distributed pessimistic locking mechanism using MongoDB
provides a robust solution for high-contention environments.
By leveraging MongoDB’s atomic operations and a carefully designed heartbeat mechanism,
the implementation ensures data integrity, fault tolerance, and scalability.
While there are some limitations, such as the lack of fairness and potential heartbeat failures,
these can be mitigated through configuration and additional enhancements.

In the end we would like to thank all our colleagues who made this initiative possible
and in particular to John Page and Carlos Castro for sharing their invaluable technical expertise.


## References
1. [How To SELECT ... FOR UPDATE inside MongoDB Transactions](https://www.mongodb.com/blog/post/how-to-select-for-update-inside-mongodb-transactions)
2. [MongoDB FAQ: Concurrency](https://www.mongodb.com/docs/manual/faq/concurrency/)
3. [Redis Lock](https://redis.io/glossary/redis-lock/)
4. [GitHub Repository](https://github.com/ronita-kuriakose-mongodb/pessimistic-lock)