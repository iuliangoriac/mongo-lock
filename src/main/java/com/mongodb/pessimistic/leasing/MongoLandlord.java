package com.mongodb.pessimistic.leasing;

import com.mongodb.client.MongoClient;
import org.bson.Document;

import java.util.Optional;

public final class MongoLandlord {

    public static Optional<DocumentLease> getLease(MongoClient mongoClient
            , String databaseName
            , String collectionName
            , Object objectId
            , long initialDurationMs) {
        var mongoDatabase = mongoClient.getDatabase(databaseName);
        var mongoCollection = mongoDatabase.getCollection(collectionName);
        var documentSelector = new Document("_id", objectId);
        var lease = new DocumentLease(mongoCollection, documentSelector);
        if (lease.extendFor(initialDurationMs)) {
            return Optional.of(lease);
        }
        return Optional.empty();
    }

}
