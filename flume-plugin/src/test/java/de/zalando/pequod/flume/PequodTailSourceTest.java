package de.zalando.pequod.flume;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;

import de.zalando.pequod.flume.source.TailFileSource;

public class PequodTailSourceTest {

// @Test
    public void test() throws Exception {

        final Context context = new Context();

        context.put("file",
            "/Users/bfriedrich/Documents/workspace_pequot/logging/apache-flume-1.5.0.1-bin/logs/flume.log");
        context.put("patternDirectory",
            "/Users/bfriedrich/Documents/workspace_pequot/logging/apache-flume-1.5.0.1-bin/conf/logstash_patterns");

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
}
