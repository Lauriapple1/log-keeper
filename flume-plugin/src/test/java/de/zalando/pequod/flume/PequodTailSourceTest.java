package de.zalando.pequod.flume;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;

import org.junit.Test;

import de.zalando.grok.GrokMapper;

import de.zalando.pequod.flume.source.TailFileSource;

public class PequodTailSourceTest {

    private static final String RECORD_MAPPING_DEFINITION = "%{LOGLEVEL:logLevel} %{GREEDYDATA:actualLoggingMessage}";

    private static final String KEY_LOG_LEVEL = "logLevel";
    private static final String KEY_DATA = "actualLoggingMessage";

// @Test
    public void test() throws Exception {

        final Context context = new Context();

        context.put("file", "apache-flume-1.5.0.1-bin/logs/flume.log");
        context.put("patternDirectory", "/conf/logstash_patterns");

        context.put("fileRecordMapping",
            "%{FLUME_TIMESTAMP:record_time} %{LOGLEVEL:logLevel} %{DATA:actualLoggingMessage}");

        final TailFileSource source = new TailFileSource();
        source.configure(context);

        final Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        channel.start();

        source.start();

        Thread.sleep(99999999999999L);
    }

    @Test
    public void testRecordMapping() {
        final GrokMapper.Builder builder = new GrokMapper.Builder();

        final GrokMapper mapper = builder.withDefaultPatternDefinitions()
                                         .withRecordMappingDefinition(RECORD_MAPPING_DEFINITION).build();

        final Map<String, String> mapping = mapper.map("INFO my test message");

        assertNotNull(mapping);
        assertFalse("mapping must not be empty", mapping.isEmpty());
        assertEquals("INFO", mapping.get(KEY_LOG_LEVEL));
        assertEquals("my test message", mapping.get(KEY_DATA));
    }
}
