package com.mongodb.pessimistic.tailable;

import com.mongodb.pessimistic.test_config.MongoDB;

import static org.junit.jupiter.api.Assertions.assertFalse;

@Deprecated
public class BlockCounterXP {

    public static void main(String[] args) {
        var adminDatabase = MongoDB.getSyncClient().getDatabase("admin");

        // Open a mongosh session to run these 3 commands to block a connection
        // use TailableWorkers
        // var handle = db.tailingWorkers.find({ id: "1"}).tailable({awaitData:true})
        // handle.next()

        // now the test should pass
        assertFalse(TailableUtils.snapshotTailableLocksFor(adminDatabase, "1").isEmpty(), "Is the mongosh session running?");
        System.exit(0);
    }

}
