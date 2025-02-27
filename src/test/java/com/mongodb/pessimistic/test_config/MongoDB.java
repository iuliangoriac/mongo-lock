package com.mongodb.pessimistic.test_config;

import com.mongodb.*;

import org.json.JSONObject;

import java.io.IOException;


public class MongoDB {
    private static MongoDB INSTANCE = null;

    public static synchronized com.mongodb.client.MongoClient getSyncClient() {
        if (INSTANCE == null) {
            INSTANCE = new MongoDB();
        }
        return INSTANCE.syncClient;
    }

    public static synchronized com.mongodb.reactivestreams.client.MongoClient getReactiveClient() {
        if (INSTANCE == null) {
            INSTANCE = new MongoDB();
        }
        return INSTANCE.reactiveClient;
    }

    private final com.mongodb.client.MongoClient syncClient;
    private final com.mongodb.reactivestreams.client.MongoClient reactiveClient;

    public MongoDB() {
        JSONObject config;
        try {
            config = ConfigJson.get("atlas").orElseThrow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var connectionString = new ConnectionString(config.getString("uri"));

        var credential = MongoCredential.createCredential(
                config.getString("userName"),
                "admin",
                config.getString("password").toCharArray());

        var serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();

        // var pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));

        var settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .serverApi(serverApi)
                .credential(credential)
                .retryWrites(true)
                .writeConcern(WriteConcern.MAJORITY)
                // .codecRegistry(pojoCodecRegistry)
                .build();

        syncClient = com.mongodb.client.MongoClients.create(settings);
        reactiveClient = com.mongodb.reactivestreams.client.MongoClients.create(settings);
    }

}
