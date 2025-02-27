package com.mongodb.pessimistic.threadbeat;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class MongoLockManager {
    MongoClient mongoClient;
    MongoCollection<Document> lockCollection;
    Thread lockExtender;
    CopyOnWriteArrayList<LockInfo> lockList;
    Thread extender;

    // A Struct to hold info on what thread took a lock

    private class LockInfo {
        LockInfo(String lockId, Thread owner) {
            this.lockId = lockId;
            this.owner = owner;
        }

        Thread owner;
        String lockId;
    }


    //A looping thread that keeps locks alive
    private class LockExtender implements Runnable {

        @Override
        public void run() {
            while (true) {
                System.out.println("Extending lock Life");
                List<String> ids = new ArrayList<>();

                // Your repeating task


                for (LockInfo lockInfo : lockList) {
                    if (lockInfo.owner.isAlive()) {
                        ids.add(lockInfo.lockId);
                    }
                }
                //Extend the livign ones
                Document query = new Document("_id", new Document("$in", ids));
                LocalDateTime expireAt = LocalDateTime.now().plusMinutes(2);
                Document updates = new Document("$set", new Document("expireAt", expireAt));
                lockCollection.updateMany(query, updates);
                System.out.println("Extended life of " + ids.size() + " locks");
                try {
                    // Simulate unit of work by sleeping - adjust sleep duration as needed
                    Thread.sleep(30000); //Extend every 30 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // ensure the thread can stop properly if interrupted
                    break;
                }
            }
        }

    }


    public MongoLockManager(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.lockList = new CopyOnWriteArrayList<>();
        lockCollection = mongoClient.getDatabase("temenos").getCollection("TAFJ_LOCK");

        // Define a TTL index on the "expireAt" field

        long ttlValue = 0; // Immediately on hitting that time - N.B this is simplest option

        lockCollection.createIndex(
                Indexes.ascending("expireAt"),
                new com.mongodb.client.model.IndexOptions().expireAfter(ttlValue, TimeUnit.SECONDS)
        );
        System.out.println("TTL index created on 'expireAt' field with expiration time " + ttlValue + " seconds");

        extender = new Thread(new LockExtender());
        extender.start();

    }

    boolean getLock(String lockId) {
        Thread currentThread = Thread.currentThread(); //Who is taking the lock
        Document lockData = new Document("_id", lockId);
        LocalDateTime explireAt = LocalDateTime.now().plusMinutes(2);
        lockData.append("expireAt", explireAt);

        // Try to insert
        try {
            lockCollection.insertOne(lockData);
            LockInfo lockInfo = new LockInfo(lockId, currentThread);
            lockList.add(lockInfo);
        } catch (MongoWriteException e) {
            if (e.getError().getCategory().equals(com.mongodb.ErrorCategory.DUPLICATE_KEY)) {
                System.out.println("Lock already taken");
            }
            return false;
        }
        return true;
    }

    boolean releaseLock(String lockId) {
        Document lockData = new Document("_id", lockId);
        lockCollection.deleteOne(lockData);
        return true;
    }
}

