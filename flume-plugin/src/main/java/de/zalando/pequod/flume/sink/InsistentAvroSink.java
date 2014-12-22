package de.zalando.pequod.flume.sink;

import static com.google.common.base.Preconditions.checkArgument;

import static de.zalando.pequod.flume.sink.SinkConstants.CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS;
import static de.zalando.pequod.flume.sink.SinkConstants.DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.sink.AvroSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InsistentAvroSink extends AvroSink {

    private ChannelProxy channelProxy;

    private long waitTimeSinceLastPutBeforeStopInMs;

    private static final long WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS = 1000L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSink.class);

    @Override
    public void configure(final Context context) {
        super.configure(context);

        waitTimeSinceLastPutBeforeStopInMs = context.getLong(CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS,
                DEFAULT_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS);

        checkArgument(waitTimeSinceLastPutBeforeStopInMs > -1L,
            "configured wait time since last put in ms [configKey=%s] must not be < 0. Got %s",
            CONFIG_WAIT_TIME_SINCE_LAST_PUT_BEFORE_STOP_IN_MS, waitTimeSinceLastPutBeforeStopInMs);
    }

    @Override
    public void stop() {

        LOGGER.debug("stopping sink...");

        if (channelProxy == null) {
            LOGGER.debug("channelProxy is null -> immediately initiating stop");
            super.stop();
        } else {

            // it might have been some time since the last batch was received. so we give it a fair chance
            // to receive other last events. this works because when TailFileSource receives the
            // kill() call, batching is disabled and everything is flushed immediately.
            channelProxy.touch();
            blockUntilStopIsAllowed();
            super.stop();
        }

        LOGGER.debug("sink has been stopped");
    }

    private void blockUntilStopIsAllowed() {
        while (!isWaitTimeForNextEventTransmissionExpired()) {
            try {

                if (LOGGER.isDebugEnabled()) {
                    final long approximateWaitTime = System.currentTimeMillis() - channelProxy.getLastUsageTime();
                    LOGGER.debug("waiting for allowed sink stop...[approximateWaitTime={}]", approximateWaitTime);
                }

                Thread.sleep(WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS);
            } catch (final InterruptedException e) {
                // do nothing
            }
        }
    }

    private boolean isWaitTimeForNextEventTransmissionExpired() {
        return (System.currentTimeMillis() - channelProxy.getLastUsageTime()) >= waitTimeSinceLastPutBeforeStopInMs;
    }

    @Override
    public synchronized Channel getChannel() {
        if (channelProxy == null) {
            channelProxy = new ChannelProxy(super.getChannel());
        }

        return channelProxy;
    }
}
