package com.mongodb.pessimistic.tailable;

import com.mongodb.client.MongoDatabase;

import static com.mongodb.pessimistic.tailable.TailableUtils.isLockedBy;

public final class TailableLock implements AutoCloseable {

    private final TailableLockHandle lockHandle;
    private final MongoDatabase adminDatabase;

    public TailableLock(TailableLockHandle lockHandle, MongoDatabase adminDatabase) {
        this.lockHandle = lockHandle;
        this.adminDatabase = adminDatabase;
    }

    public synchronized boolean isActive() {
        return isLockedBy(adminDatabase, lockHandle.getId(), lockHandle.getSignature());
    }

    @Override
    public synchronized void close() {
        lockHandle.deactivate();
        try {
            lockHandle.getThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
