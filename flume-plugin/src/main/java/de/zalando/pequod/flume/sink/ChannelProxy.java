package de.zalando.pequod.flume.sink;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.lifecycle.LifecycleState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proxy to record time of last utilization..
 */
final class ChannelProxy implements Channel {

    private long lastUsageTime;
    private final Channel channel;

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelProxy.class);

    public ChannelProxy(final Channel channel) {
        checkArgument(channel != null, "channel must not be null");
        this.channel = channel;
        this.lastUsageTime = System.currentTimeMillis();
    }

    @Override
    public void put(final Event event) throws ChannelException {
        channel.put(event);

        synchronized (this) {
            lastUsageTime = System.currentTimeMillis();
        }

        LOGGER.debug("put event {}", event);
    }

    @Override
    public Event take() throws ChannelException {
        final Event event = channel.take();
        if (event != null) {
            synchronized (this) {
                lastUsageTime = System.currentTimeMillis();
            }
        }

        LOGGER.debug("took event {}", event);
        return event;
    }

    @Override
    public Transaction getTransaction() {
        return channel.getTransaction();
    }

    @Override
    public void start() {
        LOGGER.debug("starting channel proxy...");

        channel.start();

        synchronized (this) {
            lastUsageTime = System.currentTimeMillis();
        }

        LOGGER.debug("channel proxy has been started");
    }

    @Override
    public void stop() {
        LOGGER.debug("stopping channel proxy...");

        channel.stop();

        synchronized (this) {
            lastUsageTime = System.currentTimeMillis();
        }

        LOGGER.debug("channel proxy has been stopped");
    }

    @Override
    public LifecycleState getLifecycleState() {
        return channel.getLifecycleState();
    }

    @Override
    public void setName(final String name) {
        channel.setName(name);
    }

    @Override
    public String getName() {
        return channel.getName();
    }

    /**
     * Updates last usage time to now.
     */
    public synchronized void touch() {
        lastUsageTime = System.currentTimeMillis();
    }

    /**
     * Returns last usage time.
     *
     * @return  last usage time in ms
     */
    public synchronized long getLastUsageTime() {
        return lastUsageTime;
    }
}
