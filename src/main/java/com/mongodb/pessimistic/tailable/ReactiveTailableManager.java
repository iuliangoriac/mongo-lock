package com.mongodb.pessimistic.tailable;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

@Deprecated
public class ReactiveTailableManager {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveTailableManager.class);

    public static final String TAILABLE_DB_NAME = "TailableWorkers";
    public static final String TAILABLE_COLLECTION_NAME = "tailingWorkers";
    public static final int TAILABLE_COLLECTION_SIZE = 32000;
    public static final String LOCK_COLLECTION_NAME = "locks";

    public static ReactiveTailableManager of(MongoClient syncClient) {
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

        return new ReactiveTailableManager(syncClient);
    }


    private final MongoDatabase adminDatabase;
    private final MongoCollection<Document> tailableCollection;
    private final MongoCollection<Document> locksCollection;

    public ReactiveTailableManager(MongoClient syncMongoClient) {
        this.adminDatabase = syncMongoClient.getDatabase("admin");
        this.tailableCollection = syncMongoClient.getDatabase(TAILABLE_DB_NAME).getCollection(TAILABLE_COLLECTION_NAME);
        this.locksCollection = syncMongoClient.getDatabase(TAILABLE_DB_NAME).getCollection(LOCK_COLLECTION_NAME);
    }

    public long countSyncDocuments() {
        return tailableCollection.countDocuments();
    }

    /**
     * Make sure that all workers use the same MongoDB database technical user.
     * <p>
     * The equivalent mongosh command is
     * db.getSiblingDB("admin").aggregate([{ $currentOp: { allUsers: false } }, {$match: {"cursor.originatingCommand.filter.id":"1"}}]).toArray().length
     *
     * @param blockId the block identifier - can be identical with the lock id
     * @return the number of tailable cursors for the blockId
     */
    public synchronized int countBlocksFor(Object blockId) {
        var currentOpStage = new Document("$currentOp", new Document("allUsers", false));
        var matchStage = Aggregates.match(eq("cursor.originatingCommand.filter.id", blockId));
        var selection = new ArrayList<Document>();
        adminDatabase.aggregate(List.of(currentOpStage, matchStage)).forEach(selection::add);
        return selection.size();
    }

    public Disposable registerTailableCursor(com.mongodb.reactivestreams.client.MongoClient reactiveClient, Object lockId) {
        var reactiveTailableDatabase = reactiveClient.getDatabase(TAILABLE_DB_NAME);
        var reactiveTailableCollection = reactiveTailableDatabase.getCollection(TAILABLE_COLLECTION_NAME);
        var findWorkloads = reactiveTailableCollection.find(new Document("id", lockId))
                .cursorType(com.mongodb.CursorType.TailableAwait);
        return Flux.from(findWorkloads).doOnNext(System.out::println).subscribe();
    }


}
