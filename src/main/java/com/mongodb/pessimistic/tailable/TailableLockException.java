package com.mongodb.pessimistic.tailable;

public class TailableLockException extends Exception {

    public static final int TIMEOUT = 1;
    public static final int INTERRUPTED = 2;
    public static final int UNEXPECTED = 3;

    private final int errorCode;
    private final String message;

    public TailableLockException(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public TailableLockException(Throwable cause) {
        super(cause);
        this.errorCode = UNEXPECTED;
        this.message = null;
    }

    @Override
    public String getMessage() {
        if (message != null) {
            return message;
        }
        if (getCause() != null) {
            return getCause().getMessage();
        }
        return "Unknown reason";
    }

}
