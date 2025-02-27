package com.mongodb.pessimistic.heartbeat;

import one.util.streamex.StreamEx;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class TemporalAlignmentStrategyTest {

    @Test
    void checkDefault() {
        var temporalStrategy = TemporalAlignmentStrategy.getDefault(1000);
        assertEquals(1000, temporalStrategy.getLockTtlMillis());
        assertEquals(333, temporalStrategy.getBeatIntervalMillis());
        assertEquals(1100, temporalStrategy.getBufferedLockTtlMillis(temporalStrategy.getLockTtlMillis()));
    }

    @Test
    void checkTtlTooSmall() {
        var temporalStrategy = TemporalAlignmentStrategy.getDefault(2000);
        var ex = assertThrows(UnsupportedOperationException.class,
                () -> temporalStrategy.getBufferedLockTtlMillis(1211));
        assertTrue(ex.getMessage().contains("1212"));
    }

    @Test
    void checkDelayProgression() {
        var temporalStrategy = TemporalAlignmentStrategy.getDefault(10000);
        var progression = IntStream.range(0, 5)
                .mapToObj(temporalStrategy::calculateDelay)
                .toList();
        var deltas = StreamEx.of(progression)
                .pairMap((a, b) -> b - a)
                .pairMap((a, b) -> (double) b / a)
                .toList();
        assertEquals(deltas.size(), deltas.stream().filter(v -> v > 1).count());
    }

    @Test
    void checkCalculateDelay_MaxDelayEnforced() {
        var temporalStrategy = TemporalAlignmentStrategy.getDefault(5000);
        assertEquals(4998, temporalStrategy.calculateDelay(10));
    }


}