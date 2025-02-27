package com.mongodb.pessimistic.tailable;

import com.mongodb.pessimistic.test_config.MongoDB;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated
class ReactiveTailableManagerTest {

    @Test
    @Disabled
    void asyncCall() throws InterruptedException {
        var flux = Flux.interval(Duration.ofSeconds(1)).take(5); // Emit 5 items
        flux.subscribe(System.out::println);
        for (int i = 0; i < 20; ++i) {
            System.out.println(".");
            Thread.sleep(250);
        }
    }

    @Test
    @Disabled
    void checkAcquireLockReactive() throws InterruptedException {
        var lockManager = ReactiveTailableManager.of(MongoDB.getSyncClient());

        assertEquals(0, lockManager.countBlocksFor("4"));

        var maxBlockCount = 0;
        var disposable = lockManager.registerTailableCursor(MongoDB.getReactiveClient(), "4");
        for (int i = 0; i < 100; ++i) {
            maxBlockCount = Math.max(maxBlockCount, lockManager.countBlocksFor("4"));
            if (maxBlockCount > 0) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(maxBlockCount > 0);

        disposable.dispose();
        Thread.sleep(100);
        assertEquals(0, lockManager.countBlocksFor("4"));
    }

}
