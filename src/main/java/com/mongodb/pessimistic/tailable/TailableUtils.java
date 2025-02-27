package com.mongodb.pessimistic.tailable;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.exists;

final class TailableUtils {
    private static final Logger logger = LoggerFactory.getLogger(TailableManager.class);

    static final String TAILABLE_ID_KEY = "tailableId";
    static final String LOCK_ID_KEY = "lockSignature";
    static final long TAILABLE_CURSOR_MAX_AWAIT_TIME_MILLIS = 1000;
    static final long MAX_BACKOFF_TIME_IN_MILLIS = TAILABLE_CURSOR_MAX_AWAIT_TIME_MILLIS;
    static final long TAILABLE_SNAPSHOT_INTERVAL_MILLIS = 1;
    static final int TAILABLE_SNAPSHOT_COUNT = 3;

    /**
     * Make sure that all workers use the same MongoDB database technical user.
     *
     * @param tailableId the block identifier - can be identical with the lock id
     * @return the list of lock ids of tailable cursors for the tailableId
     */
    static List<String> snapshotTailableLocksFor(MongoDatabase adminDatabase, Object tailableId) {
        var matchStage = Aggregates.match(exists("cursor.originatingCommand.filter"));
        var locks = new ArrayList<String>();
        adminDatabase
                .aggregate(List.of(new Document("$currentOp", new Document("allUsers", false)), matchStage))
                .forEach(document -> {
                    document = (Document) document.get("cursor");
                    document = (Document) document.get("originatingCommand");
                    document = (Document) document.get("filter");
                    @SuppressWarnings("unchecked")
                    var idSignature = (List<Document>) document.get("$and");
                    document = idSignature.get(0);
                    if (tailableId.equals(document.get(TAILABLE_ID_KEY))) {
                        locks.add(idSignature.get(1).getString(LOCK_ID_KEY));
                    }
                });
        if (logger.isTraceEnabled()) {
            logger.trace("Snapshot locks for resource `{}': {}", tailableId, locks);
        }
        return locks;
    }

    static Set<String> findTailableLocksFor(MongoDatabase adminDatabase, Object tailableId) {
        var locks = new HashSet<String>();
        var currentSnapshot = TAILABLE_SNAPSHOT_COUNT;
        while (currentSnapshot > 1) {
            locks.addAll(snapshotTailableLocksFor(adminDatabase, tailableId));
            try {
                Thread.sleep(TAILABLE_SNAPSHOT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            --currentSnapshot;
        }
        locks.addAll(snapshotTailableLocksFor(adminDatabase, tailableId));
        if (logger.isDebugEnabled()) {
            logger.debug("Found    locks for resource `{}': {}", tailableId, locks);
        }
        return locks;
    }

    static boolean isLockedBy(MongoDatabase adminDatabase, Object tailableId, String lockSignature) {
        var currentSnapshot = TAILABLE_SNAPSHOT_COUNT;
        while (currentSnapshot > 1) {
            if (isLockedInstance(adminDatabase, tailableId, lockSignature)) {
                return true;
            }
            try {
                Thread.sleep(TAILABLE_SNAPSHOT_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            --currentSnapshot;
        }
        return isLockedInstance(adminDatabase, tailableId, lockSignature);
    }

    private static boolean isLockedInstance(MongoDatabase adminDatabase, Object tailableId, String lockSignature) {
        var tailableLocks = snapshotTailableLocksFor(adminDatabase, tailableId);
        if (lockSignature == null) {
            return !tailableLocks.isEmpty();
        }
        return tailableLocks.contains(lockSignature);
    }

    private TailableUtils() {
    }

}
