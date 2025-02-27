package com.mongodb.pessimistic.leasing;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.unset;
import static java.lang.String.format;

public final class DocumentLease {
    private static final Logger logger = LoggerFactory.getLogger(DocumentLease.class);

    public static final String ATTR_LEASE = "_lease";
    public static final String ATTR_LEASE_ID = "id";
    public static final String ATTR_EXPIRES_AT = "expiresAt";

    private MongoCollection<Document> mongoCollection;
    private final Document documentSelector;
    private final ObjectId leaseId;
    private long expiresAt = -1;

    public DocumentLease(MongoCollection<Document> mongoCollection, Document documentSelector) {
        this.mongoCollection = mongoCollection;
        this.documentSelector = documentSelector;
        this.leaseId = ObjectId.get();
    }

    /**
     * Updates the lease duration.
     * This method will not take into account the previous duration.
     * It will simply set the next expiration time to current time since epoch in millis + newDurationMs
     *
     * @param newDurationMs - how long should the lease be kept active starting from the method call
     * @return true or false depending on the success of the operation
     */
    public synchronized boolean extendFor(long newDurationMs) {
        if (mongoCollection == null) {
            throw new UnsupportedOperationException(format("Lease %s is expired", leaseId));
        }

        logger.info("Lease {} acquisition attempt", getLogDescription());
        var success = false;

        var document = getDocumentById();

        var nextExpiresAt = System.currentTimeMillis() + newDurationMs;
        if (document == null) {

            document = new Document("_id", documentSelector.get("_id"));
            document.append(ATTR_LEASE, new Document(ATTR_LEASE_ID, leaseId).append(ATTR_EXPIRES_AT, nextExpiresAt));
            try {
                var insertResult = mongoCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(document);
                if (insertResult.wasAcknowledged()) {
                    success = true;
                } else {
                    logger.error("Lease {} acquisition failed - insert was not acknowledged", getLogDescription());
                }
            } catch (Exception ex) {
                logger.error("Lease {} acquisition failed - insert error", getLogDescription(), ex);
            }

        } else if (isFree(document) || isOwned(document)) {

            var filter = and(documentSelector, new Document(ATTR_LEASE + "." + ATTR_LEASE_ID, leaseId));
            var update = new Document("$set", new Document(ATTR_LEASE + "." + ATTR_EXPIRES_AT, nextExpiresAt));
            var options = new FindOneAndUpdateOptions();
            options.returnDocument(ReturnDocument.AFTER);
            var nextDocument = mongoCollection.withWriteConcern(WriteConcern.MAJORITY).findOneAndUpdate(filter, update, options);
            if (nextDocument == null || !isOwned(nextDocument)) {
                logger.error("Lease {} acquisition failed - expiration date could not be extended", getLogDescription());
            } else {
                success = true;
            }

        }

        if (success) {
            expiresAt = nextExpiresAt;
            logger.info("Lease {} acquired", getLogDescription());
            return true;
        }
        return false;
    }

    public synchronized boolean isActive() {
        if (expiresAt < System.currentTimeMillis()) {
            return false;
        }
        return isOwned(getDocumentById());
    }

    public synchronized void release() {
        if (mongoCollection == null) {
            logger.warn("Lease {} is already released", getLogDescription());
        }

        logger.info("Lease {} release attempt", getLogDescription());

        var filter = and(documentSelector, new Document(ATTR_LEASE + "." + ATTR_LEASE_ID, leaseId));
        var update = unset(ATTR_LEASE);
        var options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);
        var nextDocument = mongoCollection.withWriteConcern(WriteConcern.MAJORITY).findOneAndUpdate(filter, update, options);

        mongoCollection = null;
        if (nextDocument == null) {
            logger.warn("Lease {} was already released", getLogDescription());
        } else {
            logger.info("Lease {} released", getLogDescription());
        }
    }

    private Document getDocumentById() {
        return mongoCollection.find(documentSelector).projection(eq(ATTR_LEASE, 1)).first();
    }

    private boolean isFree(Document document) {
        document = (Document) document.get(ATTR_LEASE);
        if (document == null) {
            return true;
        } else {
            return (long) document.get(ATTR_EXPIRES_AT) < System.currentTimeMillis();
        }
    }

    private boolean isOwned(Document document) {
        document = (Document) document.get(ATTR_LEASE);
        if (document != null) {
            if (leaseId.equals(document.get(ATTR_LEASE_ID))) {
                return System.currentTimeMillis() < (long) document.get(ATTR_EXPIRES_AT);
            }
        }
        return false;
    }

    private String getLogDescription() {
        var description = new Document("_id", documentSelector.get("_id"));
        description.append(ATTR_LEASE, new Document(ATTR_LEASE_ID, leaseId).append(ATTR_EXPIRES_AT, expiresAt));
        return description.toJson();
    }

}
