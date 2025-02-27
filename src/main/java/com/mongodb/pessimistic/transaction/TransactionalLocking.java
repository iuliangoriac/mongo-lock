package com.mongodb.pessimistic.transaction;


import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Optional;

public class TransactionalLocking {

    private final MongoCollection<Document> collection;
    private final String userId;

    public TransactionalLocking(MongoClient mongoClient, String databaseName, String collectionName, String userId) {
        this.collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        this.userId = userId;
    }

    /**
     * Acquires a lock with a provided session.
     *
     * @param session     the MongoDB client session
     * @param documentId  the ID of the document to lock
     * @return Optional containing the locked document if successful
     */
    public Optional<Document> acquireLockWithSession(ClientSession session, ObjectId documentId) {
        Document lockInfo = new Document("userId", userId);

        int maxRetries = 3;
        int attempts = 0;

        while (attempts < maxRetries) {
            try {
                session.startTransaction();
                Document existingDoc = collection.find(Filters.eq("_id", documentId)).first();
                if (existingDoc != null && existingDoc.containsKey("lock")) {
                    session.abortTransaction();
                    return Optional.empty();
                }

                Document updatedDocument = collection.findOneAndUpdate(
                        session,
                        Filters.eq("_id", documentId),
                        Updates.set("lock", lockInfo),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
                );

                session.commitTransaction();
                return Optional.ofNullable(updatedDocument);

            } catch (MongoCommandException e) {session.abortTransaction();
                if (!e.hasErrorLabel("TransientTransactionError")) {
                    throw e;
                }
                attempts++;

                // Exponential Backoff
                try {
                    long backoffTime = (long) Math.pow(2, attempts) * 100;
                    System.out.println("Retrying transaction in " + backoffTime + "ms due to transient error: " + e.getMessage());
                    Thread.sleep(backoffTime);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread interrupted during backoff", interruptedException);
                }
            }
        }

        return Optional.empty();
    }



    public void releaseLockAfterCommit(ClientSession session, ObjectId documentId) {
        // Check if the lock exists and the userId matches
        Document document = collection.find(Filters.eq("_id", documentId)).first();
        if (document != null && document.containsKey("lock") &&
                userId.equals(document.get("lock", Document.class).getString("userId"))) {

            // Release the lock by unsetting the lock field
            collection.updateOne(
                    session,
                    Filters.eq("_id", documentId),
                    new Document("$unset", new Document("lock", ""))
            );
        }
    }



    /**
     * Handles potential lock scenarios:
     * 1. Lock already exists by another application.
     * 2. Lock expired or orphaned locks.
     */
    public boolean isLockHeld(ObjectId documentId) {
        Document document = collection.find(Filters.eq("_id", documentId)).first();
        if (document == null || !document.containsKey("lock")) {
            return false;
        }
        Document lockInfo = document.get("lock", Document.class);
        return lockInfo != null && userId.equals(lockInfo.getString("userId"));
    }




}
