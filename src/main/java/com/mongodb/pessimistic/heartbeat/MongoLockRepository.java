package com.mongodb.pessimistic.heartbeat;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;

/**
 * Utility class for interacting with the MongoDB collection that stores distributed locks.
 * <p>
 * This class provides methods for initializing the lock collection, performing CRUD operations
 * on lock documents, and handling server time synchronization.
 * </p>
 * <p>
 * The lock documents in the MongoDB collection are structured as follows:
 * <ul>
 *   <li>{@code _id}: The unique identifier for the lock.</li>
 *   <li>{@code ownerId}: The identifier of the lock owner (e.g., thread or process).</li>
 *   <li>{@code ttlMs}: The time-to-live (TTL) of the lock in milliseconds.</li>
 *   <li>{@code expiresAt}: The expiration timestamp of the lock.</li>
 * </ul>
 * </p>
 */
final class MongoLockRepository {

    // MongoDB document field names for lock metadata
    static final String EXPIRES_AT = "expiresAt"; // Field for the lock's expiration timestamp
    static final String OWNER_ID = "ownerId";     // Field for the lock's owner identifier
    static final String TTL_MS = "ttlMs";         // Field for the lock's time-to-live (TTL) in milliseconds

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private MongoLockRepository() {
    }

    /**
     * Initializes the MongoDB collection for storing locks.
     * <p>
     * This method ensures that the required indexes are created for efficient querying
     * and management of lock documents.
     * </p>
     *
     * @param syncMongoClient   the MongoDB client to use for initialization.
     * @param lockDatabaseName  the name of the database containing the lock collection.
     * @param lockCollectionName the name of the collection where locks are stored.
     * @return the initialized MongoDB collection.
     */
    static MongoCollection<Document> initializeLockCollection(MongoClient syncMongoClient
            , String lockDatabaseName
            , String lockCollectionName) {
        var lockDatabase = syncMongoClient.getDatabase(lockDatabaseName);
        var lockCollection = lockDatabase.getCollection(lockCollectionName);

        // Ensure the collection has the required indexes
        lockCollection.createIndex(new Document(EXPIRES_AT, 1), new IndexOptions().unique(false));
        lockCollection.createIndex(new Document(OWNER_ID, 1), new IndexOptions().unique(false));
        return lockCollection;
    }

    /**
     * Ensures that the lock collection is aggregable by inserting a dummy document if it is empty.
     * <p>
     * This is required for certain MongoDB aggregation pipelines (e.g., retrieving server time)
     * that require at least one document in the collection.
     * </p>
     *
     * @param lockCollection the MongoDB collection to check and populate if necessary.
     */
    static void ensureLockCollectionIsAggregable(MongoCollection<Document> lockCollection) {
        while (lockCollection.countDocuments() <= 0) {
            var query = eq("_id", 0);
            var update = List.of(
                    set(EXPIRES_AT, Long.MAX_VALUE),
                    set(OWNER_ID, "Used for $$NOW retrieving aggregation pipeline"),
                    set(TTL_MS, 0)
            );
            var options = new FindOneAndUpdateOptions().upsert(true);
            lockCollection
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .findOneAndUpdate(query, update, options);
        }
    }

    /**
     * Reads the current server time from MongoDB using an aggregation pipeline.
     * <p>
     * This method uses the {@code $$NOW} operator to retrieve the server's current time.
     * </p>
     *
     * @param lockCollection the MongoDB collection to query.
     * @return a document containing the server time, or {@code null} if the query fails.
     */
    static Document readServerTime(MongoCollection<Document> lockCollection) {
        var pipeline = List.of(
                project(new Document("serverTime", "$$NOW")), // Project the current server time
                limit(1) // Limit the result to one document
        );
        return lockCollection
                .withReadPreference(ReadPreference.primary())
                .aggregate(pipeline)
                .first();
    }

    /**
     * Inserts a new lock document into the MongoDB collection.
     *
     * @param lockManager the lock manager managing the lock.
     * @param lockId      the unique identifier for the lock.
     * @param lockOwnerId the identifier of the lock owner.
     * @param ttlMillis   the time-to-live (TTL) for the lock in milliseconds.
     */
    static void insertLock(MongoLockManager lockManager, String lockId, String lockOwnerId, long ttlMillis) {
        lockManager.getLockCollection()
                .withWriteConcern(WriteConcern.MAJORITY)
                .insertOne(new Document("_id", lockId)
                        .append(OWNER_ID, lockOwnerId)
                        .append(TTL_MS, ttlMillis)
                        .append(EXPIRES_AT, lockManager.serverTimeMillis() + ttlMillis));
    }

    /**
     * Updates an existing lock document to extend its TTL or transfer ownership.
     * <p>
     * The lock is updated if:
     * <ul>
     *   <li>It is already owned by the current owner.</li>
     *   <li>It has expired (i.e., its expiration time is in the past).</li>
     * </ul>
     * </p>
     *
     * @param lockManager the lock manager managing the lock.
     * @param lockId      the unique identifier for the lock.
     * @param lockOwnerId the identifier of the lock owner.
     * @param ttlMillis   the new TTL for the lock in milliseconds.
     * @return the updated lock document, or {@code null} if the update fails.
     */
    static Document updateLock(MongoLockManager lockManager, String lockId, String lockOwnerId, long ttlMillis) {
        var serverTimeMillis = lockManager.serverTimeMillis();
        // Define a query to check if the lock can be "extended" or re-acquired.
        // This happens if:
        // - The lock is already owned by the current owner.
        // - The lock has expired (its expiration time is less than the current time).
        var query = and(
                eq("_id", lockId),
                or(
                        eq(OWNER_ID, lockOwnerId),
                        expr(new Document("$lt", List.of("$" + EXPIRES_AT, serverTimeMillis)))
                )
        );

        var update = List.of(
                set(OWNER_ID, lockOwnerId),
                set(TTL_MS, ttlMillis),
                set(EXPIRES_AT, serverTimeMillis + ttlMillis)
        );

        // Specify options to return the updated document after the operation.
        var options = new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE);
        // Attempt to extend or transferred the lock ownership by updating the document.
        return lockManager.getLockCollection()
                .withWriteConcern(WriteConcern.MAJORITY)
                .findOneAndUpdate(query, update, options);
    }

    /**
     * Deletes a lock document from the MongoDB collection.
     *
     * @param lockManager the lock manager managing the lock.
     * @param lockId      the unique identifier for the lock.
     * @param lockOwnerId the identifier of the lock owner.
     * @return the deleted lock document, or {@code null} if no document was deleted.
     */
    static Document deleteLock(MongoLockManager lockManager, String lockId, String lockOwnerId) {
        var lockData = new Document("_id", lockId).append(OWNER_ID, lockOwnerId);
        return lockManager.getLockCollection()
                .withWriteConcern(WriteConcern.MAJORITY)
                .findOneAndDelete(lockData);
    }

    /**
     * Deletes all expired lock documents from the MongoDB collection.
     *
     * @param lockManager the lock manager managing the locks.
     * @return the result of the bulk delete operation.
     */
    static DeleteResult bulkDeleteExpiredLocks(MongoLockManager lockManager) {
        var deleteQuery = expr(new Document("$lt", List.of("$" + EXPIRES_AT, lockManager.serverTimeMillis())));
        return lockManager.getLockCollection()
                .withWriteConcern(WriteConcern.MAJORITY)
                .deleteMany(deleteQuery);
    }

    /**
     * Extends the expiration time of active locks in bulk.
     *
     * @param lockManager the lock manager managing the locks.
     * @param myLockIds   the set of lock IDs to extend.
     * @return the result of the bulk update operation.
     */
    static UpdateResult bulkExtendActiveLocks(MongoLockManager lockManager, Collection<String> myLockIds) {
        var updateQuery = in("_id", myLockIds);
        var updates = List.of(
                set(EXPIRES_AT, new Document("$add", List.of(lockManager.serverTimeMillis(), "$" + TTL_MS)))
        );
        return lockManager.getLockCollection()
                .withWriteConcern(WriteConcern.MAJORITY)
                .updateMany(updateQuery, updates);
    }
}
