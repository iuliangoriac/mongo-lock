package com.mongodb.pessimistic.threadbeat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class LockDemo {
    public static void main(String[] args) {
        // Replace the URI string with your MongoDB deployment's connection string.
        String uri = "mongodb://localhost:27017";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("testdb");

            System.out.println("Connected to the database successfully!");

            MongoLockManager lockManager = new MongoLockManager(mongoClient);

            while(true) {
                boolean isLocked=lockManager.getLock("LOCK1");
                System.out.println("Thread: " + Thread.currentThread().getName() + " GET LOCK1 = " + isLocked);

                //Child thread
                Thread thread = new Thread(() -> {
                    System.out.println("Taking a lock in a child thread");
                    boolean childLocked=lockManager.getLock("LOCK2");
                    System.out.println("Thread: " + Thread.currentThread().getName() + " GET LOCK2 = " + isLocked);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                thread.start();

                Thread.sleep(10000);


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


