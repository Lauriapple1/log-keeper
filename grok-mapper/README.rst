===========
GROK Mapper
===========

Java utility to utilize GROK patterns (see http://logstash.net/docs/1.3.2/filters/grok). 
GROK Mapper already contains pattern definitions defined in https://github.com/logstash-plugins/logstash-patterns-core/tree/master/patterns)
but also allows to extend and / or define your own pattern library.

Requirements
============
- Java 1.8
- Apache Maven 3.x

Usage
=====
.. code:: java

   GrokMapper.Builder builder = new GrokMapper.Builder();
   GrokMapper mapper = builder.withDefaultPatternDefinitions().withRecordMappingDefinition("%{LOGLEVEL:logLevel} %{GREEDYDATA:actualLoggingMessage}").build();
   Map<String,String> mapping = mapper.map("INFO my test message");

   assertEquals("INFO", mapping.get("logLevel"));
   assertEquals("my test message", mapping.get("actualLoggingMessage"));