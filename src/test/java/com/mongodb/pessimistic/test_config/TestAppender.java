package com.mongodb.pessimistic.test_config;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TestAppender extends AppenderBase<ILoggingEvent> {

    private static TestAppender INSTANCE;

    public static synchronized TestAppender getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new TestAppender();
            INSTANCE.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
            INSTANCE.start();
            INSTANCE.getContext().getLogger(Logger.ROOT_LOGGER_NAME).addAppender(INSTANCE);
            INSTANCE.setShowMongoLogs(false);
        }
        return INSTANCE;
    }

    private final List<ILoggingEvent> logList = new ArrayList<>();

    @Override
    protected void append(ILoggingEvent o) {
        if (o.getLoggerName().startsWith("com.mongodb.pessimistic.")) {
            logList.add(o);
        }
    }

    @Override
    public LoggerContext getContext() {
        return (LoggerContext) super.getContext();
    }

    public void setlogLevel(Level newLevel) {
        getContext().getLogger(Logger.ROOT_LOGGER_NAME).setLevel(newLevel);
    }

    public void setShowMongoLogs(boolean flag) {
        var mongoLogger = getContext().getLogger("org.mongodb.driver");
        if (flag) {
            mongoLogger.setLevel(Level.DEBUG);
        } else {
            mongoLogger.setLevel(Level.OFF);
        }
    }

    public List<ILoggingEvent> getLogs() {
        return logList;
    }

    public void clearLogs() {
        logList.clear();
    }

}
