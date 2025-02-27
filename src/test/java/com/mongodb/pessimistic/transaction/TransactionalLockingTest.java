package com.mongodb.pessimistic.transaction;


import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mongodb.pessimistic.test_config.MongoDB;

import static org.junit.jupiter.api.Assertions.*;

    class TransactionalLockingTest {

        private TransactionalLocking transactionalLocking;
        private MongoClient mongoClient;
        private MongoCollection<Document> collection;
        private final ObjectId testDocumentId = new ObjectId();
        private static final String DB_NAME = "testDB";
        private static final String LOCK_COLLECTION = "lockCollectionTransaction";
        private static final String USER_1 = "user_1";

        @BeforeEach
        void setUp() {
            mongoClient = MongoDB.getSyncClient();
            collection = mongoClient.getDatabase(DB_NAME).getCollection(LOCK_COLLECTION);
            collection.deleteMany(new Document()); // Clean up before tests
            collection.insertOne(new Document("_id", testDocumentId).append("data", "initial"));

            transactionalLocking = new TransactionalLocking(mongoClient, DB_NAME, LOCK_COLLECTION,USER_1 );
        }

        @Test
        void testAcquireLockWithSession() {
            try (ClientSession session = mongoClient.startSession()) {
                session.startTransaction();

                Optional<Document> lockedDocument = transactionalLocking.acquireLockWithSession(session, testDocumentId);
                assertTrue(lockedDocument.isPresent(), "Lock acquisition should return a document");
                assertEquals(USER_1, lockedDocument.get().get("lock", Document.class).getString("userId"));

                session.commitTransaction();
            }
        }


        @Test
        void testConcurrentLocking() throws InterruptedException {
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            CountDownLatch latch = new CountDownLatch(2);
            AtomicBoolean conflictDetected = new AtomicBoolean(false);

            Runnable lockTask = () -> {
                try (ClientSession session = mongoClient.startSession()) {
                    session.startTransaction();

                    Optional<Document> lockedDoc = transactionalLocking.acquireLockWithSession(session, testDocumentId);
                    if (lockedDoc.isEmpty()) {
                        conflictDetected.set(true);
                    }

                    session.commitTransaction();

                } catch (Exception e) {
                    conflictDetected.set(true);
                } finally {
                    latch.countDown();
                }
            };


            executorService.submit(lockTask);
            executorService.submit(lockTask);

            latch.await();
            executorService.shutdown();


            assertTrue(conflictDetected.get(), "One of the tasks should detect a conflict");
        }


        @Test
        void testLockPersistenceOnFailure() {
            try (ClientSession session = mongoClient.startSession()) {
                session.startTransaction();
                Optional<Document> lockedDocument = transactionalLocking.acquireLockWithSession(session, testDocumentId);

                assertTrue(lockedDocument.isPresent(), "Lock acquisition should return a document");
                session.abortTransaction();
            }


            Document document = collection.find(new Document("_id", testDocumentId)).first();
            assertNotNull(document, "Document should still exist");
            assertNull(document.get("lock"), "Lock field should not be present after abort");
        }


        @Test
        void testLockReleasedOnTransactionRollback() throws InterruptedException {

            ClientSession session = mongoClient.startSession();
            session.startTransaction();

            try {
                Optional<Document> lockedDoc = transactionalLocking.acquireLockWithSession(session, testDocumentId);
                assertTrue(lockedDoc.isPresent(), "Lock should be successfully acquired");
                System.out.println("Simulating transaction failure...");
                throw new RuntimeException("Simulated transaction failure");

            } catch (Exception e) {

                session.abortTransaction();
            }


            boolean isLockHeldAfterRollback = transactionalLocking.isLockHeld(testDocumentId);
            assertFalse(isLockHeldAfterRollback, "Lock should be released after transaction rollback");
            Document docAfterRollback = collection.find(Filters.eq("_id", testDocumentId)).first();
            assertNull(docAfterRollback.get("lock"), "Lock should be null after rollback");
            session.close();
        }

        @Test
        void testLockReleasedAfterCommit() throws InterruptedException {

            ClientSession session = mongoClient.startSession();
            session.startTransaction();

            try {

                Optional<Document> lockedDoc = transactionalLocking.acquireLockWithSession(session, testDocumentId);
                assertTrue(lockedDoc.isPresent(), "Lock should be successfully acquired");

                session.commitTransaction();
            } catch (Exception e) {

                session.abortTransaction();
            }

            transactionalLocking.releaseLockAfterCommit(session, testDocumentId);

            boolean isLockHeldAfterCommit = transactionalLocking.isLockHeld(testDocumentId);
            assertFalse(isLockHeldAfterCommit, "Lock should be released after transaction commit");

            Document docAfterCommit = collection.find(Filters.eq("_id", testDocumentId)).first();
            assertNull(docAfterCommit.get("lock"), "Lock should be null after commit");

            session.close();
        }

        @Test
        void stressTesting(){
            ExecutorService executor = Executors.newFixedThreadPool(50); // 50 concurrent threads
            List<Callable<Void>> tasks = new ArrayList<>();

            try {
                for (int i = 0; i < 500; i++) {  // Number of operations

                    tasks.add(() -> {
                        try (ClientSession session = mongoClient.startSession()) {
                            session.startTransaction();
                            Optional<Document> lockResult = transactionalLocking.acquireLockWithSession(session, testDocumentId);
                            if (lockResult.isPresent()) {
                                System.out.println(Thread.currentThread().getName() + " acquired lock.");
                                Thread.sleep(100); // Simulate work while holding lock
                                session.commitTransaction();
                                transactionalLocking.releaseLockAfterCommit(session, testDocumentId);
                            } else {
                                System.out.println(Thread.currentThread().getName() + " failed to acquire lock.");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
                }

                executor.invokeAll(tasks);
                executor.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }
