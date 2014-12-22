package de.zalando.grok;

public class GrokMapperException extends RuntimeException {

    public GrokMapperException() { }

    public GrokMapperException(final String message) {
        super(message);
    }

    public GrokMapperException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GrokMapperException(final Throwable cause) {
        super(cause);
    }

    public GrokMapperException(final String message, final Throwable cause, final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
