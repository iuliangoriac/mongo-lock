package com.mongodb.pessimistic.leasing;

import com.mongodb.client.MongoCollection;
import com.mongodb.pessimistic.test_config.MongoDB;
import com.mongodb.pessimistic.test_config.TestAppender;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

class OneWorkerLeaseTest {

    private static final String DB_NAME = "testDB";
    private static final String COLLECTION_NAME = "leaseCollection";
    private static final String DOC_ID = "doc_1";

    private final TestAppender appender = TestAppender.getInstance();
    private MongoCollection<Document> leaseCollection;

    @BeforeEach
    void cleanupCollection() {
        leaseCollection = MongoDB.getSyncClient().getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        leaseCollection.deleteMany(new Document());
        appender.clearLogs();
    }

    @Test
    void acquireExtendAndRelease() throws InterruptedException {
        var doc = leaseCollection.find(eq("_id", DOC_ID)).first();
        assertNull(doc);

        var lease = MongoLandlord.getLease(MongoDB.getSyncClient(), DB_NAME, COLLECTION_NAME, DOC_ID, 0).orElseThrow();
        try {
            assertFalse(lease.isActive());

            doc = leaseCollection.find(eq("_id", DOC_ID)).first();
            assertNotNull(doc);
            assertNotNull(doc.get(DocumentLease.ATTR_LEASE));

            assertTrue(lease.extendFor(250));
            // perform one step of the business logic
            assertTrue(lease.isActive());

            Thread.sleep(300);
            assertFalse(lease.isActive());

            assertTrue(lease.extendFor(500));
            // perform another step of the business logic
            assertTrue(lease.isActive());

            doc = leaseCollection.find(eq("_id", DOC_ID)).first();
            assertNotNull(doc);
            assertNotNull(doc.get(DocumentLease.ATTR_LEASE));
        } finally {
            lease.release();
        }

        doc = leaseCollection.find(eq("_id", DOC_ID)).first();
        assertNotNull(doc);
        assertNull(doc.get(DocumentLease.ATTR_LEASE));
    }

}