package com.mongodb.pessimistic.tailable;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.pessimistic.tailable.TailableUtils.MAX_BACKOFF_TIME_IN_MILLIS;
import static com.mongodb.pessimistic.tailable.TailableUtils.findTailableLocksFor;
import static java.lang.String.format;

public final class TailableManager {
    private static final Logger logger = LoggerFactory.getLogger(TailableManager.class);

    private static final String TAILABLE_DB_NAME = "PessimisticLocks";
    private static final String TAILABLE_COLLECTION_NAME = "tailingCursors";
    private static final int TAILABLE_COLLECTION_SIZE = 32000;

    public static TailableManager of(MongoClient syncClient) {
        return of(syncClient, TAILABLE_DB_NAME, TAILABLE_COLLECTION_NAME);
    }

    public static TailableManager of(MongoClient syncClient, String databaseName, String collectionName) {
        var syncDatabase = syncClient.getDatabase(databaseName);

        MongoCollection<Document> tailableCollection = null;

        for (var name : syncDatabase.listCollectionNames()) {
            if (collectionName.equals(name)) {
                tailableCollection = syncDatabase.getCollection(collectionName);
                if (logger.isDebugEnabled()) {
                    logger.debug("Tailable locks collection `{}' already exists", collectionName);
                }
                break;
            }
        }

        if (tailableCollection == null) {
            var options = new CreateCollectionOptions();
            options.capped(true).sizeInBytes(TAILABLE_COLLECTION_SIZE);
            syncDatabase.createCollection(collectionName, options);
            tailableCollection = syncDatabase.getCollection(collectionName);
            tailableCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(new Document());
            if (tailableCollection.countDocuments() <= 0) {
                throw new UnsupportedOperationException(
                        format("Taliable locks collection`%s' could not be edited", collectionName));
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Tailable locks collection `{}' was created", collectionName);
            }
        }

        return new TailableManager(syncClient, tailableCollection);
    }


    private final MongoDatabase adminDatabase;
    private final MongoCollection<Document> tailableCollection;

    private TailableManager(MongoClient syncMongoClient, MongoCollection<Document> tailableCollection) {
        this.adminDatabase = syncMongoClient.getDatabase("admin");
        this.tailableCollection = tailableCollection;
    }

    public TailableLock acquireLock(Object tailableId) throws TailableLockException {
        return acquireLock(tailableId, 0);
    }

    @SuppressWarnings("BusyWait")
    public synchronized TailableLock acquireLock(Object tailableId, long timeoutHintMillis) throws TailableLockException {
        var t0 = System.currentTimeMillis();
        var lockHandle = new TailableLockHandle(tailableCollection, tailableId);
        var attemptCount = 1;
        if (logger.isDebugEnabled()) {
            logger.debug("Tailable lock `{}' for resource `{}' acquisition attempt #{}.",
                    lockHandle.getSignature(), tailableId, attemptCount);
        }
        try {
            long exponentialBackoffDelay = 10;
            while (true) {
                while (!lockHandle.isStarted()) {
                    Thread.sleep(1);
                }
                var cursorLocks = findTailableLocksFor(adminDatabase, tailableId);
                if (cursorLocks.size() == 1 && cursorLocks.contains(lockHandle.getSignature())) {
                    logger.info("Tailable lock `{}' for resource `{}' acquired after {} attempt(s)",
                            lockHandle.getSignature(), tailableId, attemptCount);



                    return new TailableLock(lockHandle, adminDatabase);
                }
                if (timeoutHintMillis > 0) {
                    var tDelta = System.currentTimeMillis() - t0;
                    if (tDelta > timeoutHintMillis) {
                        throw new TailableLockException(TailableLockException.TIMEOUT,
                                format("Lock acquisition timeout after %d milliseconds", tDelta));
                    }
                }
                Thread.sleep(exponentialBackoffDelay);
                exponentialBackoffDelay = Math.min(exponentialBackoffDelay * 2, MAX_BACKOFF_TIME_IN_MILLIS);
                ++attemptCount;
                if (logger.isDebugEnabled()) {
                    logger.debug("Tailable lock `{}' for resource `{}' acquisition attempt #{}",
                            lockHandle.getSignature(), tailableId, attemptCount);
                }
            }
        } catch (InterruptedException ex) {
            logger.error("Tailable lock `{}' for resource `{}' acquisition failed after {} attempt(s):",
                    lockHandle.getSignature(), tailableId, attemptCount, ex);
            lockHandle.deactivate();
            Thread.currentThread().interrupt();
            throw new TailableLockException(TailableLockException.INTERRUPTED, "Lock acquisition thread was interrupted");
        } catch (TailableLockException ex) {
            logger.warn("Tailable lock `{}' for resource `{}' acquisition failed after {} attempt(s) due to timeout",
                    lockHandle.getSignature(), tailableId, attemptCount);
            lockHandle.deactivate();
            throw ex;
        } catch (Exception ex) {
            logger.error("Tailable lock `{}' for resource `{}' acquisition failed after {} attempt(s):",
                    lockHandle.getSignature(), tailableId, attemptCount, ex);
            lockHandle.deactivate();
            throw new TailableLockException(ex);
        }
    }

}
