package com.mongodb.pessimistic.tailable;

import com.mongodb.pessimistic.test_config.MongoDB;

@Deprecated
public class ConnectionBlockerXP {

    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    public static void main(String[] args) throws InterruptedException, TailableLockException {
        var lockManager = TailableManager.of(MongoDB.getSyncClient());
        try (var ignore = lockManager.acquireLock("2")) {
            while (true) Thread.sleep(1000);
        }

        // Now load the ConnectionBlockerXP.js in mongosh and monitor or debug it with
        // - load('C:/Users/iulian.goriac/Projects/pessimistic-lock/src/test/java/com/mongodb/pessimistic/tailable/ConnectionBlockerXP.js')
        // - monitor_tailable()
        // - debug_tailable()
        // - debug_tailable_no_aggregation()
    }

}
