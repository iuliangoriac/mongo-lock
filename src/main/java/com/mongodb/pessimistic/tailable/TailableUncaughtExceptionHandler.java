package com.mongodb.pessimistic.tailable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailableUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(TailableManager.class);

    private final TailableLockHandle tlh;
    private final Thread.UncaughtExceptionHandler prevUeh;

    public TailableUncaughtExceptionHandler(TailableLockHandle tlh, Thread.UncaughtExceptionHandler prevUeh) {
        this.tlh = tlh;
        this.prevUeh = prevUeh;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            tlh.deactivate();
        } catch (Exception ex) {
            logger.error("Realising tailable lock `{}'", tlh.getSignature(), ex);
        }
        if (prevUeh != null) {
            prevUeh.uncaughtException(t, e);
        }
    }

}
