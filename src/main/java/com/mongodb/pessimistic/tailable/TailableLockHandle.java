package com.mongodb.pessimistic.tailable;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.pessimistic.tailable.TailableUtils.*;

public final class TailableLockHandle implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TailableManager.class);

    private final Object id;
    private final String signature;
    private final Thread thread;
    private final MongoCursor<Document> cursor;
    private boolean started = false;
    private boolean explicitStop = false;

    TailableLockHandle(MongoCollection<Document> tailableCollection, Object id) {
        this.id = id;
        this.signature = UUID.randomUUID().toString();

        this.cursor = tailableCollection.find(and(eq(TAILABLE_ID_KEY, id), eq(LOCK_ID_KEY, signature)))
                .cursorType(com.mongodb.CursorType.TailableAwait)
                .maxAwaitTime(TAILABLE_CURSOR_MAX_AWAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .iterator();

        var prevUeh = Thread.currentThread().getUncaughtExceptionHandler();
        Thread.currentThread().setUncaughtExceptionHandler(new TailableUncaughtExceptionHandler(this, prevUeh));

        this.thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    Object getId() {
        return id;
    }

    String getSignature() {
        return signature;
    }

    Thread getThread() {
        return thread;
    }

    boolean isStarted() {
        return started;
    }

    void deactivate() {
        explicitStop = true;
        cursor.close();
    }

    @Override
    public void run() {
        try {
            started = true;
            cursor.next();
        } catch (Exception ex) {
            if (explicitStop) {
                logger.info("Tailable lock handle `{}' for resource `{}' was released explicitly", signature, id);
            } else {
                logger.error("Tailable lock handle `{}' for resource `{}' was released by:", signature, id, ex);
            }
        }
    }

}
