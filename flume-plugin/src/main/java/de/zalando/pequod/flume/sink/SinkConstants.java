package de.zalando.pequod.flume.sink;

final class SinkConstants {

    // -- config keys

    /**
     * time to wait after last read in ms before the sink stops (after having received a kill signal).
     */
    public static final String CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS = "waitTimeSinceLastPutInMs";

    // -- default values

    public static final long DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS = 5000L;

    private SinkConstants() { }
}
