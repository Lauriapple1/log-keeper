package de.zalando.pequod.flume.source;

import static java.nio.file.Files.exists;
import static java.nio.file.Files.isRegularFile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.File;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Reads target file according to tail semantics and puts each read record to the a queue which is shared with at least
 * 1 {@link de.zalando.pequod.flume.source.RecordConsumer}.
 */
final class LogFileReader extends TailerListenerAdapter implements Runnable, Configurable {

    private final BlockingQueue<String> outputQueue;

    private Tailer tailer;
    private String inputFile;
    private int inputBufferSize;
    private boolean isConfigured;

    private long lastRecordReadTime;
    private long lastReadWaitTimeForKillInMs;

    private static final long QUEUE_OFFER_TIMEOUT_IN_MS = 1000L;
    private static final long WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS = 1000L;

    private static final String THREAD_NAME_TEMPLATE = LogFileReader.class.getSimpleName() + "(%s)";

    private static final Logger LOGGER = LoggerFactory.getLogger(LogFileReader.class);

    public LogFileReader(final BlockingQueue<String> outputQueue) {

        checkArgument(outputQueue != null, "output queue must not be null");
        this.outputQueue = outputQueue;
        this.isConfigured = false;
    }

    @Override
    public void configure(final Context context) throws FlumeException {

        LOGGER.info("configuring log file reader");

        inputFile = context.getString(SourceConstants.CONFIG_TARGET_FILE);
        checkConfiguredInputFile();

        inputBufferSize = context.getInteger(SourceConstants.CONFIG_INPUT_BUFFER_SIZE,
                SourceConstants.DEFAULT_INPUT_BUFFER_SIZE);
        Preconditions.checkArgument(inputBufferSize > 0,
            "input buffer size [configKey=%s] has to be greater than 0. Got %s",
            SourceConstants.CONFIG_INPUT_BUFFER_SIZE, inputBufferSize);

        final long tailerDelayMs = context.getLong(SourceConstants.CONFIG_TAILER_DELAY_MS,
                SourceConstants.DEFAULT_TAILER_DELAY_MS);
        Preconditions.checkArgument(tailerDelayMs > 0L,
            "the configured the delay between checks [configKey=%s] must not be lower than 1",
            SourceConstants.CONFIG_TAILER_DELAY_MS);

        lastReadWaitTimeForKillInMs = context.getLong(SourceConstants.CONFIG_LAST_READ_WAIT_TIME_FOR_KILL_IN_MS,
                SourceConstants.DEFAULT_LAST_READ_WAIT_TIME_FOR_KILL_IN_MS);

        Preconditions.checkArgument(lastReadWaitTimeForKillInMs > -1L,
            "configured time to wait after last read before kill [configKey=%s] must not be lower than 0",
            SourceConstants.CONFIG_LAST_READ_WAIT_TIME_FOR_KILL_IN_MS);

        final boolean tailerStartsFromEnd = context.getBoolean(SourceConstants.CONFIG_TAILER_START_FROM_END,
                SourceConstants.DEFAULT_TAILER_START_FROM_END);

        final boolean tailerReopen = context.getBoolean(SourceConstants.CONFIG_TAILER_REOPEN,
                SourceConstants.DEFAULT_TAILER_REOPEN);

        tailer = Tailer.create(new File(inputFile), this, tailerDelayMs, tailerStartsFromEnd, tailerReopen,
                inputBufferSize);

        isConfigured = true;
        LOGGER.info("log file reader has been configured");

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("state after configuration: {}", toString());
        }
    }

    private void checkConfiguredInputFile() {
        Preconditions.checkArgument(!isNullOrEmpty(inputFile),
            "input file name configured with [configKey=%s] must not be null or empty",
            SourceConstants.CONFIG_TARGET_FILE);

        final Path inputFilePath = FileSystems.getDefault().getPath(inputFile);
        Preconditions.checkArgument(exists(inputFilePath),
            "[inputFile=%s] configured with [configKey=%s] does not exist", inputFile,
            SourceConstants.CONFIG_TARGET_FILE);

        Preconditions.checkArgument(isRegularFile(inputFilePath),
            "[inputFile=%s]  configured with [configKey=%s] is not a regular file", inputFile,
            SourceConstants.CONFIG_TARGET_FILE);
    }

    @Override
    public void fileNotFound() {
        LOGGER.warn("could not find [inputFile={}] -> waiting for it to show up", inputFile);
    }

    @Override
    public void fileRotated() {
        LOGGER.info("[inputFile={}] has rotated", inputFile);
    }

    @Override
    public void handle(final String line) {
        enqueueEvent(line);
    }

    @Override
    public void handle(final Exception ex) {
        LOGGER.warn("an error while tailing [inputFile={}]. current configuration is [this={}]",
            new Object[] {inputFile, toString()}, ex);
    }

    public synchronized void kill() {
        blockUntilStopIsAllowed();
        tailer.stop();
    }

    private void blockUntilStopIsAllowed() {

        while (!isWaitTimeForNextRecordExpired()) {
            try {
                LOGGER.debug("waiting for allowed reader stop...");
                Thread.sleep(WAIT_FOR_ALLOWED_KILL_DELAY_IN_MS);
            } catch (final InterruptedException e) {
                // do nothing
            }
        }
    }

    private boolean isWaitTimeForNextRecordExpired() {
        return System.currentTimeMillis() - lastRecordReadTime >= lastReadWaitTimeForKillInMs;
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
            LOGGER.info("start reading [inputFile={}]", inputFile);
            checkState(isConfigured, "LogFileReader has not been configured");
            synchronized (this) {
                lastRecordReadTime = System.currentTimeMillis();
            }

            tailer.run(); // no need for extra thread because LogFileReader is already executed in its own thread

            LOGGER.info("log file reader has been stopped");
        } catch (final RuntimeException e) {
            LOGGER.error("an unexpected error occurred while reading [inputFile={}]", inputFile, e);
        }
    }

    private void enqueueEvent(final String record) {
        try {
            while (!outputQueue.offer(record, QUEUE_OFFER_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("waiting for sufficient space in shared queue...");
            }

            synchronized (this) {
                lastRecordReadTime = System.currentTimeMillis();
            }

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("outputQueue", outputQueue).add("tailer", tailer)
                      .add("inputFile", inputFile).add("inputBufferSize", inputBufferSize)
                      .add("isConfigured", isConfigured).toString();
    }

}
