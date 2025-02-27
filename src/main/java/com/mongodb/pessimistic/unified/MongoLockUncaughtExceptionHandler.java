package com.mongodb.pessimistic.unified;

import java.util.concurrent.locks.Lock;

public class MongoLockUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final Lock lock;
    private final Thread.UncaughtExceptionHandler prevUeh;

    MongoLockUncaughtExceptionHandler(Lock lock, Thread.UncaughtExceptionHandler prevUeh) {
        this.lock = lock;
        this.prevUeh = prevUeh == null ? (t, e) -> {
        } : prevUeh;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        lock.unlock();
        prevUeh.uncaughtException(t, e);
    }

}
