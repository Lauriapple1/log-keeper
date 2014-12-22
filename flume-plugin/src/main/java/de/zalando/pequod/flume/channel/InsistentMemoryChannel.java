package de.zalando.pequod.flume.channel;

import static com.google.common.base.Preconditions.checkArgument;

import static de.zalando.pequod.flume.channel.ChannelConstants.CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS;
import static de.zalando.pequod.flume.channel.ChannelConstants.DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * {@link org.apache.flume.channel.MemoryChannel} extension which shuts down only if no operation has been performed on
 * it since [waitTimeSinceLastPutBeforeStopInMs] ms.
 */
public final class InsistentMemoryChannel extends MemoryChannel {

    private long lastUsageTime;
    private long waitTimeSinceLastPutBeforeStopInMs;

    private static final long WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS = 1000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(InsistentMemoryChannel.class);

    public InsistentMemoryChannel() {
        lastUsageTime = 0L;
        waitTimeSinceLastPutBeforeStopInMs = 0L;
    }

    @Override
    public void configure(final Context context) {
        super.configure(context);

        waitTimeSinceLastPutBeforeStopInMs = context.getLong(CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS,
                DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS);
        checkArgument(waitTimeSinceLastPutBeforeStopInMs > -1L,
            "wait time since last put before stop [configKey=%s] must not be lower than 0. Got %s",
            CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS, waitTimeSinceLastPutBeforeStopInMs);
    }

    @Override
    public void put(final Event event) {
        super.put(event);

        synchronized (this) {
            lastUsageTime = System.currentTimeMillis();
        }

        LOGGER.debug("put event {}", event);
    }

    @Override
    public Event take() {
        final Event event = super.take();

        if (event != null) {
            synchronized (this) {
                lastUsageTime = System.currentTimeMillis();
            }
        }

        LOGGER.debug("took event {}", event);

        return event;
    }

    @Override
    public void start() {
        LOGGER.debug("starting channel...");
        super.start();

        synchronized (this) {
            lastUsageTime = System.currentTimeMillis();
        }

        LOGGER.debug("channel has been started");
    }

    @Override
    public void stop() {
        LOGGER.debug("stopping channel...");

        blockUntilStopIsAllowed();
        super.stop();

        LOGGER.debug("channel has been stopped");
    }

    private void blockUntilStopIsAllowed() {
        while (!isWaitTimeForNextPutExpired()) {
            try {

                if (LOGGER.isDebugEnabled()) {
                    final long approximateWaitTimeInMs = System.currentTimeMillis() - lastUsageTime;
                    LOGGER.debug("waiting for allowed channel stop...[approximateWaitTimeInMs={}]",
                        approximateWaitTimeInMs);
                }

                Thread.sleep(WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS);
            } catch (final InterruptedException e) {
                // do nothing
            }
        }
    }

    private boolean isWaitTimeForNextPutExpired() {
        return System.currentTimeMillis() - lastUsageTime >= waitTimeSinceLastPutBeforeStopInMs;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("lastUsageTime", lastUsageTime)
                      .add("waitTimeSinceLastPutBeforeStopInMs", waitTimeSinceLastPutBeforeStopInMs).toString();
    }
}
