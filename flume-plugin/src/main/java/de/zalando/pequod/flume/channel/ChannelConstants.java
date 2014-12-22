package de.zalando.pequod.flume.channel;

final class ChannelConstants {

    // -- config keys

    /**
     * time to wait after last event receive in ms before the channel stops (after having received a kill signal).
     */
    public static final String CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS = "waitTimeSinceLastPutBeforeStopInMs";

    // -- default values

    public static final long DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS = 5000L;

    private ChannelConstants() { }
}
