package com.mongodb.pessimistic.heartbeat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.pessimistic.heartbeat.MongoLockManager.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SystemStubsExtension.class)
class MongoLockUtilsTest {

    @Test
    void checkEnvVariableNotSet(EnvironmentVariables environmentVariables) {
        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, null);
        var result = MongoLockUtils.getEffectiveLockTtlMillis();
        assertEquals(DEFAULT_LOCK_TTL_MILLIS, result, "Should return default value if env variable is invalid");
    }


    @Test
    void checkEnvVariableSetToValidValueAboveMin(EnvironmentVariables environmentVariables) {
        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, "60000");
        var result = MongoLockUtils.getEffectiveLockTtlMillis();
        assertEquals(60000L, result, "Should return the value from env variable if it is valid and above min");

    }

    @Test
    void checkEnvVariableSetToValidValueBelowMin(EnvironmentVariables environmentVariables) {
        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, "100");
        var result = MongoLockUtils.getEffectiveLockTtlMillis();
        assertEquals(MongoLockUtils.MIN_LOCK_TTL_MILLIS, result, "Should return MIN_LOCK_TTL_MILLIS if env variable value is below min");
    }

    @Test
    void checkEnvVariableSetToInvalidValue(EnvironmentVariables environmentVariables) {
        environmentVariables.set(DEFAULT_LOCK_TTL_MILLIS_KEY, "invalid");
        var result = MongoLockUtils.getEffectiveLockTtlMillis();
        assertEquals(DEFAULT_LOCK_TTL_MILLIS, result, "Should return default value if env variable is invalid");
    }

    @Test
    void checkHeartbeatInitializedAndScheduled() {
        // Arrange
        var heartbeat = new AtomicReference<ScheduledExecutorService>();
        var command = mock(Runnable.class);
        var beatIntervalMillis = 100;

        // Act
        MongoLockUtils.startHeartbeat(heartbeat, command, beatIntervalMillis);

        // Assert
        assertNotNull(heartbeat.get(), "Heartbeat should be initialized");
        verify(command, timeout(beatIntervalMillis * 2).atLeast(1)).run();

        // Cleanup
        heartbeat.get().shutdownNow();
    }

    @Test
    void checkHeartbeatNotReinitializedIfAlreadySet() {
        // Arrange
        var existingHeartbeat = mock(ScheduledExecutorService.class);
        var heartbeat = new AtomicReference<>(existingHeartbeat);
        var command = mock(Runnable.class);
        var beatIntervalMillis = 100;

        // Act
        MongoLockUtils.startHeartbeat(heartbeat, command, beatIntervalMillis);

        // Assert
        assertSame(existingHeartbeat, heartbeat.get(), "Existing heartbeat should not be replaced");
        verify(existingHeartbeat, never()).shutdown();

        // Verify that the new executor is properly shut down to avoid resource leakage
        verify(existingHeartbeat, never()).shutdownNow();
    }

    @Test
    void checkResourceCleanupOnFailedInitialization() {
        // Arrange
        var existingHeartbeat = Executors.newScheduledThreadPool(1);
        var heartbeat = new AtomicReference<>(existingHeartbeat);

        var command = mock(Runnable.class);
        var beatIntervalMillis = 100;

        var spyExecutor = spy(Executors.newScheduledThreadPool(1));

        try (var mockedExecutors = Mockito.mockStatic(Executors.class)) {
            mockedExecutors.when(() -> Executors.newScheduledThreadPool(1)).thenReturn(spyExecutor);

            // Act
            MongoLockUtils.startHeartbeat(heartbeat, command, beatIntervalMillis);
        }

        // Assert
        assertSame(existingHeartbeat, heartbeat.get(), "Existing heartbeat should not be replaced");
        verify(spyExecutor).shutdown(); // Ensure the new executor is shut down

        // Cleanup
        existingHeartbeat.shutdownNow();
    }

}
