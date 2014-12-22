package de.zalando.pequod.flume.source;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import static de.zalando.pequod.flume.source.SourceConstants.CONFIG_BATCH_SIZE;
import static de.zalando.pequod.flume.source.SourceConstants.CONFIG_CHARSET;
import static de.zalando.pequod.flume.source.SourceConstants.CONFIG_FILE_RECORD_MAPPING;
import static de.zalando.pequod.flume.source.SourceConstants.CONFIG_MAX_EVENT_FLUSH_DELAY_IN_MS;
import static de.zalando.pequod.flume.source.SourceConstants.DEFAULT_BATCH_SIZE;
import static de.zalando.pequod.flume.source.SourceConstants.DEFAULT_CHARSET;
import static de.zalando.pequod.flume.source.SourceConstants.DEFAULT_FLUSH_DELAY_IN_MS;

import java.nio.charset.Charset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Consumer for records read by {@link de.zalando.pequod.flume.source.LogFileReader}. Each record is mapped according to
 * the configured GROK pattern (see config parameter
 * {@link de.zalando.pequod.flume.source.SourceConstants#CONFIG_FILE_RECORD_MAPPING}) and creates an event out of the
 * record and the mapping data. Note that the mapping data is stored in the "fields" tag.
 */
final class RecordConsumer implements Runnable, Configurable {

    private ChannelProcessor channelProcessor;
    private final BlockingQueue<String> inputQueue;
    private final ImmutableMap<String, String> configuredPatterns;
    private GrokMapper recordMapper;

    private volatile boolean isRunning;

    private int eventBatchSize;
    private long maxEventFlushDelayInMs;
    private String fileRecordMapping;
    private Charset charset;

    private boolean isConfigured;

    private static final long QUEUE_POLL_TIMEOUT_IN_MS = 10000L;

    private static final String THREAD_NAME_TEMPLATE = RecordConsumer.class.getSimpleName() + "(%s)";

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordConsumer.class);

    public RecordConsumer(final BlockingQueue<String> inputQueue,
            final ImmutableMap<String, String> configuredPatterns) {

        checkArgument(inputQueue != null, "input queue must not be null");
        checkArgument(configuredPatterns != null, "map of configured patterns must not be null");

        this.inputQueue = inputQueue;
        this.isConfigured = false;
        this.configuredPatterns = configuredPatterns;
    }

    public void setChannelProcessor(final ChannelProcessor channelProcessor) {
        checkArgument(channelProcessor != null, "channel processor must not be null");
        this.channelProcessor = channelProcessor;
    }

    @Override
    public void configure(final Context context) {
        LOGGER.info("configuring event consumer");

        eventBatchSize = context.getInteger(CONFIG_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        maxEventFlushDelayInMs = context.getLong(CONFIG_MAX_EVENT_FLUSH_DELAY_IN_MS, DEFAULT_FLUSH_DELAY_IN_MS);
        fileRecordMapping = context.getString(CONFIG_FILE_RECORD_MAPPING);

        final String charsetString = context.getString(CONFIG_CHARSET, DEFAULT_CHARSET);
        checkArgument(!isNullOrEmpty(charsetString), "charset [configKey=%s] must not be null or empty",
            CONFIG_CHARSET);

        checkArgument(eventBatchSize > 0, "event batch size [configKey=%s] must not be lower than 1. Got %s",
            CONFIG_BATCH_SIZE, eventBatchSize);

        checkArgument(maxEventFlushDelayInMs > 0,
            "max event flush delay [configKey=%s] must not be lower than 1. Got %s", CONFIG_MAX_EVENT_FLUSH_DELAY_IN_MS,
            maxEventFlushDelayInMs);

        checkArgument(!isNullOrEmpty(fileRecordMapping), "file record mapping [configKey=%s] must not be null or empty",
            CONFIG_FILE_RECORD_MAPPING);

        charset = Charset.forName(charsetString);
        recordMapper = new GrokMapper(fileRecordMapping, configuredPatterns);

        isConfigured = true;
        LOGGER.info("event consumer has been configured");

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("state after configuration: {}", toString());
        }
    }

    /**
     * Let's this instance stop as soon as no more events are available in the queue.
     */
    public void kill() {
        this.isRunning = false;
    }

    private void setThreadName() {
        final Thread currentThread = Thread.currentThread();
        final String currentName = currentThread.getName();
        currentThread.setName(String.format(THREAD_NAME_TEMPLATE, currentName));
    }

    @Override
    public void run() {
        try {
            setThreadName();
            consume();
        } catch (final RuntimeException e) {
            LOGGER.error("an unexpected error occurred while consuming event data", e);
        }

    }

    private void consume() {
        LOGGER.info("event consumer has been started");

        checkState(isConfigured, "FLushService has not been configured");
        checkState(channelProcessor != null, "no channel process set");

        final ArrayList<Event> eventBatch = Lists.newArrayListWithCapacity(eventBatchSize);
        isRunning = true;

        long lastFlushTime = System.currentTimeMillis();

        Map<String, String> recordMappings;
        Event event;
        String record;
        while (isRunning || !inputQueue.isEmpty()) {
            try {

                record = inputQueue.poll(QUEUE_POLL_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
                LOGGER.debug("consuming [record={}]...", record);

                if (record != null) {
                    recordMappings = recordMapper.map(record);
                    event = EventBuilder.withBody(record, charset, recordMappings);
                    eventBatch.add(event);

                    // NOTE: if kill is initiated, we want to get rid of our events as soon as possbile
                    if (!isRunning || eventBatch.size() >= eventBatchSize || isFlushTime(lastFlushTime)) {
                        flushEventBatch(eventBatch);
                        lastFlushTime = System.currentTimeMillis();
                    }
                }

            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        flushEventBatch(eventBatch);

        LOGGER.info("event consumer has been stopped");
    }

    private boolean isFlushTime(final long lastFlush) {
        return System.currentTimeMillis() - lastFlush >= maxEventFlushDelayInMs;
    }

    private void flushEventBatch(final List<Event> eventBatch) {
        channelProcessor.processEventBatch(eventBatch);
        eventBatch.clear();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("channelProcessor", channelProcessor).add("inputQueue", inputQueue)
                      .add("recordMapper", recordMapper).add("isRunning", isRunning)
                      .add("eventBatchSize", eventBatchSize).add("maxEventFlushDelayInMs", maxEventFlushDelayInMs)
                      .add("fileRecordMapping", fileRecordMapping).add("charset", charset)
                      .add("isConfigured", isConfigured).toString();
    }
}
