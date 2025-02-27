package com.mongodb.pessimistic.reactivebeat;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;

import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.lt;

public final class ReactiveBeatLocker {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveBeatLocker.class);

    static final long BEAT_INTERVAL_IN_MILLIS = 1000;
    static final long EXTENSION_INTERVAL_IN_MILLIS = 1100;
    static final String SEED_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    static final String EXPIRE_AT_ATTR = "expireAt";

    private static final String REACTIVE_BEAT_DB_NAME = "PessimisticLocks";
    private static final String REACTIVE_BEAT_COLLECTION_NAME = "reactiveLocks";

    private static final ConcurrentMap<String, ReactiveBeatLocker> INSTANCES = new ConcurrentHashMap<>();

    public static ReactiveBeatLocker getInstance(MongoClient reactiveClient) {
        return INSTANCES.computeIfAbsent("default", id -> {
            var lockDatabase = reactiveClient.getDatabase(REACTIVE_BEAT_DB_NAME);
            var lockCollection = lockDatabase.getCollection(REACTIVE_BEAT_COLLECTION_NAME);

            var query = new Document("_id", SEED_ID);
            var update = Updates.set(EXPIRE_AT_ATTR, LocalDateTime.now().plusYears(1000));
            var options = new UpdateOptions().upsert(true);
            Mono.from(lockCollection.updateOne(query, update, options)).block();

            return new ReactiveBeatLocker(id, lockCollection);
        });
    }

    private final MongoCollection<Document> lockCollection;
    private final List<ReactiveBeatLock> locks = new CopyOnWriteArrayList<>();

    private ReactiveBeatLocker(String serverId, MongoCollection<Document> lockCollection) {
        this.lockCollection = lockCollection;
        Flux.interval(Duration.ofMillis(BEAT_INTERVAL_IN_MILLIS)).doOnNext(this::onBeat).subscribe();
        logger.debug("Initialized reactive heartbeat for server `{}.{}.{}'",
                serverId, REACTIVE_BEAT_DB_NAME, REACTIVE_BEAT_COLLECTION_NAME);
    }

    public Lock getLockFor(String lockId) {
        var lock = new ReactiveBeatLock(this, lockId);
        locks.add(lock);
        return lock;
    }

    MongoCollection<Document> getLockCollection() {
        return lockCollection;
    }

    private void onBeat(long beatIndex) {
        logger.trace("Beat #{}", beatIndex);
        lockCollection.aggregate(List.of(project(new Document("currentDate", new Document("$toLong", "$$NOW")))))
                .first()
                .subscribe(new BaseSubscriber<>() {

                    private Long serverTimeMillis;

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        logger.error("Beat #{} error reading server time", beatIndex, throwable);
                    }

                    @Override
                    protected void hookOnNext(Document value) {
                        serverTimeMillis = value.getLong("currentDate");
                    }

                    @Override
                    protected void hookOnComplete() {
                        if (serverTimeMillis != null) {
                            extendLocks(beatIndex, serverTimeMillis);
                        }
                    }
                });
    }

    private void extendLocks(long beatIndex, long serverTimeMillis) {
        var jvmIds = locks.stream()
                .filter(ReactiveBeatLock::isActive)
                .map(ReactiveBeatLock::getLockId)
                .toList();
        if (jvmIds.isEmpty()) {
            cleanupLocks(beatIndex, serverTimeMillis);
        } else {
            var query = new Document("_id", new Document("$in", jvmIds));
            var updates = Updates.set(EXPIRE_AT_ATTR, serverTimeMillis + EXTENSION_INTERVAL_IN_MILLIS);
            lockCollection.updateMany(query, updates).subscribe(new BaseSubscriber<>() {

                @Override
                protected void hookOnError(Throwable throwable) {
                    logger.error("Beat #{} error updating locks", beatIndex, throwable);
                }

                @Override
                protected void hookOnComplete() {
                    cleanupLocks(beatIndex, serverTimeMillis);
                }
            });
        }
    }

    private void cleanupLocks(long beatIndex, long serverTimeMillis) {
        logger.debug("Beat #{} cleaning up locks", beatIndex);
        lockCollection.deleteMany(lt(EXPIRE_AT_ATTR, serverTimeMillis)).subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnError(Throwable throwable) {
                logger.error("Beat #{} error deleting locks", beatIndex, throwable);
            }
        });
    }

}
