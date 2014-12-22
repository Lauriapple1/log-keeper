package de.zalando.grok;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public final class GrokMapperTest {

    private GrokMapper.Builder builder;

    private static final String RECORD_MAPPING_DEFINITION = "%{LOGLEVEL:logLevel} %{GREEDYDATA:actualLoggingMessage}";

    private static final String KEY_LOG_LEVEL = "logLevel";
    private static final String KEY_DATA = "actualLoggingMessage";

    @Before
    public void setup() {
        builder = new GrokMapper.Builder();
    }

    @Test
    public void testBuilder() throws Exception {
        builder.withDefaultPatternDefinitions().withRecordMappingDefinition(RECORD_MAPPING_DEFINITION).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderWithoutRecordMappingDefinition() {
        builder.withDefaultPatternDefinitions().build();
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderWithoutAnyDefinition() {
        builder.build();
    }

    @Test
    public void testRecordMapping() {
        final GrokMapper mapper = builder.withDefaultPatternDefinitions()
                                         .withRecordMappingDefinition(RECORD_MAPPING_DEFINITION).build();

        final Map<String, String> mapping = mapper.map("INFO my test message");

        assertNotNull(mapping);
        assertFalse("mapping must not be empty", mapping.isEmpty());
        assertEquals("INFO", mapping.get(KEY_LOG_LEVEL));
        assertEquals("my test message", mapping.get(KEY_DATA));
    }

    @Test
    public void testRecordMappingWithoutDefaultPatterns() {
        final GrokMapper mapper = builder.withRecordMappingDefinition(RECORD_MAPPING_DEFINITION).build();

        final Map<String, String> mapping = mapper.map("INFO my test message");
        assertNotNull(mapping);
        assertTrue("mapping should be empty", mapping.isEmpty());
    }
}
