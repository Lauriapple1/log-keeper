package de.zalando.pequod.flume.source;

final class SourceConstants {

    // -- config keys

    /**
     * event batch size. NOTE: the event batch can also be put to the channel when maxEventFlushDelayInMs has passed
     */
    public static final String CONFIG_BATCH_SIZE = "eventBatchSize";

    /**
     * max time in ms which can pass till the current event batch has to flushed to the channel.
     */
    public static final String CONFIG_MAX_EVENT_FLUSH_DELAY_IN_MS = "maxEventFlushDelayInMs";

    /**
     * GROK pattern e.g. "%{FLUME_TIMESTAMP:record_time} %{LOGLEVEL:logLevel} %{GREEDYDATA:loggingMessage}"
     */
    public static final String CONFIG_FILE_RECORD_MAPPING = "fileRecordMapping";

    /**
     * file charset.
     */
    public static final String CONFIG_CHARSET = "charset";

    /**
     * target file.
     */
    public static final String CONFIG_TARGET_FILE = "file";

    /**
     * buffer size for read operations.
     */
    public static final String CONFIG_INPUT_BUFFER_SIZE = "inputBufferSize";

    /**
     * location of logstash pattern (folder of patterns belonging to this project).
     */
    public static final String CONFIG_PATTERN_DIRECTORY = "patternDirectory";

    /**
     * number of consumers performing record to field mappings, event creation and putting events to the channel.
     */
    public static final String CONFIG_NUMBER_OF_CONSUMERS = "numberOfConsumers";

    /**
     * capacity of the queue which is shared between the log file reader and its consumer.
     */
    public static final String CONFIG_QUEUE_CAPACITY = "sharedQueueCapacity";

    /**
     * capacity of the queue which is shared between the log file reader and its consumer.
     */
    public static final String CONFIG_LAST_READ_WAIT_TIME_FOR_KILL_IN_MS = "lastReadWaitTimeForKillInMs";

    /**
     * the delay between checks of the file for new content in milliseconds.
     */
    public static final String CONFIG_TAILER_DELAY_MS = "tailerDelayMs";

    /**
     * set to true to tail from the end of the file, false to tail from the beginning of the file.
     */
    public static final String CONFIG_TAILER_START_FROM_END = "tailerStartFromEnd";

    /**
     * whether to close/reopen the file between chunks.
     */
    public static final String CONFIG_TAILER_REOPEN = "tailerReopen";

    // -- default values

    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final int DEFAULT_BATCH_SIZE = 10;
    public static final long DEFAULT_FLUSH_DELAY_IN_MS = 1000L;
    public static final int DEFAULT_INPUT_BUFFER_SIZE = 1024;
    public static final String DEFAULT_PATTERN_DIRECTORY = "./conf/logstash_patterns";
    public static final int DEFAULT_NUMBER_OF_CONSUMERS = 2;
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final long DEFAULT_TAILER_DELAY_MS = 500L;
    public static final boolean DEFAULT_TAILER_START_FROM_END = true;
    public static final boolean DEFAULT_TAILER_REOPEN = true;
    public static final long DEFAULT_LAST_READ_WAIT_TIME_FOR_KILL_IN_MS = 10000L;

    private SourceConstants() { }
}
