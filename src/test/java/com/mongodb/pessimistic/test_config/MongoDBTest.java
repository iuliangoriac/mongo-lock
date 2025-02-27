package com.mongodb.pessimistic.test_config;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoDBTest {

    @Test
    void getClient() {
        MongoClient client = MongoDB.getSyncClient();
        assertNotNull(client);
        AtomicInteger counter = new AtomicInteger(0);
        client.listDatabaseNames().forEach(name -> counter.incrementAndGet());
        assertTrue(counter.get() > 0);
    }

}
