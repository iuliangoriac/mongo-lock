package com.mongodb.pessimistic.tailable;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Deprecated
public class SyncLockManager {
    private static final Logger logger = LoggerFactory.getLogger(SyncLockManager.class);

    public static final String TAILABLE_DB_NAME = "TailableWorkers";
    public static final String TAILABLE_COLLECTION_NAME = "tailingWorkers";
    public static final int TAILABLE_COLLECTION_SIZE = 32000;
    public static final String LOCK_COLLECTION_NAME = "locks";

    public static SyncLockManager of(MongoClient syncClient) {
        var syncDatabase = syncClient.getDatabase(TAILABLE_DB_NAME);

        MongoCollection<Document> tailableCollection = null;
        for (var name : syncDatabase.listCollectionNames()) {
            if (TAILABLE_COLLECTION_NAME.equals(name)) {
                tailableCollection = syncDatabase.getCollection(TAILABLE_COLLECTION_NAME);
                break;
            }
        }
        if (tailableCollection == null) {
            var options = new CreateCollectionOptions();
            options.capped(true).sizeInBytes(TAILABLE_COLLECTION_SIZE);
            syncDatabase.createCollection(TAILABLE_COLLECTION_NAME, options);
            tailableCollection = syncDatabase.getCollection(TAILABLE_COLLECTION_NAME);
            tailableCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(new Document());
        }

        return new SyncLockManager(syncClient);
    }

    private final MongoDatabase adminDatabase;
    private final MongoCollection<Document> tailableCollection;
    private final MongoCollection<Document> locksCollection;

    public SyncLockManager(MongoClient syncMongoClient) {
        this.adminDatabase = syncMongoClient.getDatabase("admin");
        this.tailableCollection = syncMongoClient.getDatabase(TAILABLE_DB_NAME).getCollection(TAILABLE_COLLECTION_NAME);
        this.locksCollection = syncMongoClient.getDatabase(TAILABLE_DB_NAME).getCollection(LOCK_COLLECTION_NAME);
    }

    public boolean getLock(Object lockId, String userClaiming) {
        var query = eq("_id", lockId);
        var newValues = new Document("locked", true).append("owner", userClaiming);
        var updateIfNotLocked = Aggregates.replaceWith(new Document("$cond",
                List.of("$locked", "$$ROOT", newValues)));

        var options = new FindOneAndUpdateOptions()
                .upsert(true)
                .returnDocument(ReturnDocument.AFTER);

        var retVal = locksCollection
                .withWriteConcern(WriteConcern.MAJORITY)
                .findOneAndUpdate(query, List.of(updateIfNotLocked), options);

        return retVal != null && userClaiming.equals(retVal.get("owner"));
    }

    public boolean releaseLock(Object lockId, String userClaiming) {
        var query = eq("_id", lockId);
        var newValues = new Document("locked", false).append("owner", userClaiming);
        var updateIfOwned = Aggregates.replaceWith(new Document("$cond",
                List.of(new Document("$eq", List.of("$owner", userClaiming)), newValues, "$$ROOT")));

        var options = new FindOneAndUpdateOptions()
                .upsert(true)
                .returnDocument(ReturnDocument.AFTER);

        var retVal = locksCollection
                .withWriteConcern(WriteConcern.MAJORITY)
                .findOneAndUpdate(query, List.of(updateIfOwned), options);

        return retVal != null && Boolean.FALSE.equals(retVal.get("locked"));
    }

}
