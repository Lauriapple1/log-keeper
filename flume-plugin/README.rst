===================
Pequod Flume Plugin
===================

Flume plugin containing

- de.zalando.pequod.flume.source.TailFileSource
- de.zalando.pequod.flume.sink.InsistentAvroSink
- de.zalando.pequod.flume.channel.InsistentMemoryChannel


Installation
============

- You need JDK 8.
- install `grok-mapper <https://github.com/zalando/log-keeper/tree/master/grok-mapper>`_ in your local repository
- ``mvn clean install -Passemble-artifacts``
- create folder ``$FLUME_HOME/plugins.d`` 
- copy and extract ``pequod-flume-plugin-1-dist.tar.gz`` in ``plugins.d``
- replace commons-io library in ``$FLUME_HOME/lib`` with the commons-io version the plugin's lib directory (at least version 2.4 is required)


Usage
=====

``bin/flume-ng agent -Xmx1G -n agent -c conf -f conf/my-flume-conf.properties``


Configuration Example
=====================

    agent.sources = src
    agent.channels = memoryChannel
    agent.sinks = avroSink
    
    agent.sources.src.type     = de.zalando.pequod.flume.source.TailFileSource
    agent.sources.src.file     = /var/log/my-log-file.log
    agent.sources.src.channels = memoryChannel
    
    agent.sources.src.patternDirectory = /opt/apache-flume-1.5.0.1-bin/conf/logstash_patterns
    agent.sources.src.fileRecordMapping  = %{FLUME_TIMESTAMP:record_time} %{LOGLEVEL:logLevel} %{GREEDYDATA:actualLoggingMessage}
    agent.channels.memoryChannel.type = de.zalando.pequod.flume.channel.InsistentMemoryChannel
    agent.channels.memoryChannel.capacity = 10000
    
    agent.sinks.avroSink.type = de.zalando.pequod.flume.sink.InsistentAvroSink
    agent.sinks.avroSink.channel = memoryChannel
    agent.sinks.avroSink.hostname = localhost
    agent.sinks.avroSink.port = 4545
    agent.sinks.avroSink.batch-size = 10
    
    
Available Configuration Parameters
----------------------------------

de.zalando.pequod.flume.source.TailFileSource:
----------------------------------------------

+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+ 
| Parameter                   | Default Value           | Description                                                                                                   | 
+=============================+=========================+===============================================================================================================+ 
| eventBatchSize              | 10                      | event batch size. NOTE: the event batch can also be put to the channel when maxEventFlushDelayInMs has passed |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| maxEventFlushDelayInMs      | 1000                    | max time in ms which can pass till the current event batch has to flushed to the channel                      | 
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| fileRecordMapping           | none                    | GROK pattern e.g. "%{FLUME_TIMESTAMP:record_time} %{LOGLEVEL:logLevel} %{GREEDYDATA:loggingMessage}"          |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| charset                     | UTF-8                   | file charset                                                                                                  |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| file                        | none                    | target file                                                                                                   |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| inputBufferSize             | 1024                    | buffer size for read operations                                                                               |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| patternDirectory            | ./conf/logstash_patterns| location of logstash pattern (folder of patterns belonging to this project)                                   |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| numberOfConsumers           | 2                       | number of consumers performing record to field mappings, event creation and putting events to the channel     |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| sharedQueueCapacity         | 10000                   | capacity of the queue which is shared between the log file reader and its consumer                            |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| lastReadWaitTimeForKillInMs | 10000                   | time to wait after last read in ms before the reader stops (after having received a kill signal)              |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| tailerDelayMs               | 500                     | the delay between checks of the file for new content in ms                                                    |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| tailerStartFromEnd          | true                    | set to true to tail from the end of the file, false to tail from the beginning of the file                    |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+
| tailerReopen                | true                    | whether to close/reopen the file between chunks                                                               |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+



de.zalando.pequod.flume.sink.InsistentAvroSink
----------------------------------------------

Besides the configuration parameter available for the usual Avro Sink (http://flume.apache.org/FlumeUserGuide.html#avro-sink) the following parameters are available:

+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+ 
| Parameter                   | Default Value           | Description                                                                                                   | 
+=============================+=========================+===============================================================================================================+ 
| waitTimeSinceLastPutInMs    | 5000                    |  time to wait after last event receive in ms before the sink stops (after having received a kill signal)      |
+-----------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+


de.zalando.pequod.flume.channel.InsistentMemoryChannel
------------------------------------------------------

Besides the configuration parameter available for the usual Memory Channel (http://flume.apache.org/FlumeUserGuide.html#memory-channel) the following parameters are available:

+------------------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+ 
| Parameter                          | Default Value           | Description                                                                                                   | 
+====================================+=========================+===============================================================================================================+ 
| waitTimeSinceLastPutBeforeStopInMs | 5000                    |  time to wait after last event receive in ms before the channel stops (after having received a kill signal)   |
+------------------------------------+-------------------------+---------------------------------------------------------------------------------------------------------------+


