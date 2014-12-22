package de.zalando.pequod.flume.source;

import java.io.IOException;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.base.Objects;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.source.AbstractEventDrivenSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static de.zalando.pequod.flume.source.SourceConstants.*;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;

/**
 * Flume source which reads a configured target file according to tail semantics and maps all
 * read records according to a configured GROK pattern.
 */
public final class TailFileSource extends AbstractEventDrivenSource {

    private LogFileReader logFileReader;
    private final List<RecordConsumer> consumers;
    private int numberOfConsumers;
    private boolean isConfigured;
    
    private ExecutorService executor;

    private static final int NUMBER_OF_READERS = 1;
    
    
    private static final long AWAIT_TERMINATION_WAIT_TIME = 500L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TailFileSource.class);

    public TailFileSource() {
        consumers = Lists.newArrayList();
        isConfigured = false;
    }

    @Override
    public void doConfigure(final Context context) throws FlumeException {

        try {
            LOGGER.info("configuring source...");

            final int queueCapacity = context.getInteger(CONFIG_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY);
            checkArgument(queueCapacity > 0, "queue capacity [configKey=%s] must not be lower than 1", CONFIG_QUEUE_CAPACITY);
            
            final ArrayBlockingQueue<String> sharedQueue = new ArrayBlockingQueue<>(queueCapacity, true);
            configureReader(context, sharedQueue);
            configureConsumers(context,sharedQueue);
            isConfigured = true;

            LOGGER.info("source has been configured successfully");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("state after configuration: {}", toString());
            }
        } catch (final IOException e) {
            throw new FlumeException("an error occurred while configuring source", e);
        }
    }

    private void configureReader(final Context context, final BlockingQueue<String> sharedQueue)  {
        logFileReader = new LogFileReader(sharedQueue);
        logFileReader.configure(context);
    }
    
    private void configureConsumers(final Context context, final BlockingQueue<String> sharedQueue) throws IOException {

        final String patternDirectory = context.getString(CONFIG_PATTERN_DIRECTORY, DEFAULT_PATTERN_DIRECTORY);
        final Path patternDirectoryPath = FileSystems.getDefault().getPath(patternDirectory);
        checkArgument(exists(patternDirectoryPath), "[patternDirectory=%s] does not exist", patternDirectory);
        checkArgument(isDirectory(patternDirectoryPath), "[patternDirectory=%s] is not a directory", patternDirectory);

        numberOfConsumers = context.getInteger(CONFIG_NUMBER_OF_CONSUMERS, DEFAULT_NUMBER_OF_CONSUMERS);
        checkArgument(numberOfConsumers > 0, "there has to be at least 1 consumer! configured: [numberOfConsumers=%s]",
                numberOfConsumers);

        final ImmutableMap<String, String> configuredPatterns = PatternLoader.load(patternDirectory);

        for (int i = 0; i < numberOfConsumers; i++) {
            final RecordConsumer recordConsumer = new RecordConsumer(sharedQueue, configuredPatterns);
            recordConsumer.configure(context);
            consumers.add(recordConsumer);
        }
    }
    
    @Override
    protected void doStart() throws FlumeException {
        LOGGER.info("starting source...");
        checkState(isConfigured, "source has not been configured");

        executor = Executors.newFixedThreadPool(numberOfConsumers + NUMBER_OF_READERS);

        for (RecordConsumer recordConsumer : consumers) {
            recordConsumer.setChannelProcessor(getChannelProcessor());
            executor.submit(recordConsumer);
        }

        executor.submit(logFileReader);

        LOGGER.info("source has been started");
    }

    @Override
    protected void doStop() throws FlumeException {

        LOGGER.info("stopping source...");
      
        logFileReader.kill();
        consumers.stream().forEach(RecordConsumer::kill);

        executor.shutdown();
        
        while (!executor.isTerminated()) {
            LOGGER.debug("waiting for source to stop...");
            try {
                executor.awaitTermination(AWAIT_TERMINATION_WAIT_TIME, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        LOGGER.info("source has been stopped");
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("logFileReader", logFileReader)
                .add("consumers", consumers)
                .add("numberOfConsumers", numberOfConsumers)
                .add("isConfigured", isConfigured)
                .add("executor", executor)
                .toString();
    }
}
