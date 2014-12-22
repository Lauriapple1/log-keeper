package de.zalando.grok;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;

import java.net.URL;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.joni.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

//J-

/**
 * Maps records to fields according to logstash syntax. In fact, GROK patterns are fully supported here.<br></br>
 *
 * see
 * <ul>
 * <li>
 * <a href="https://github.com/elasticsearch/logstash/tree/master/patterns">
 *   https://github.com/elasticsearch/logstash/tree/master/patterns
 * </a>
 * </li>
 * <li>
 * <a href="http://logstash.net/docs/1.3.2/filters/grok">
 *    http://logstash.net/docs/1.3.2/filters/grok
 * </a>
 * </li>
 * <li>
 * <a href="https://github.com/elasticsearch/logstash/blob/0aaf8c68742bafa78f83492920902648e651c763/lib/logstash/filters/grok.rb">
 *    https://github.com/elasticsearch/logstash/blob/0aaf8c68742bafa78f83492920902648e651c763/lib/logstash/filters/grok.rb
 * </a>
 * </li>
 * <li>
 * <a href="http://www.geocities.jp/kosako3/oniguruma/doc/RE.txt">
 *     http://www.geocities.jp/kosako3/oniguruma/doc/RE.txt
 * </a>
 * </li>
 * <li>
 * <a href="https://github.com/elasticsearch/logstash/blob/master/patterns/grok-patterns">
 *    https://github.com/elasticsearch/logstash/blob/master/patterns/grok-patterns
 * </a>
 * </li>
 * </ul>
 */
//J+
public final class GrokMapper {

    private final Regex regex;

    // see  (?<name>subexp)
    private static final String NAMED_RULE_REF_REGEX_PATTERN_TEMPLATE = "(?<%s>%s)";
    private static final String RULE_REGEX_PATTERN_TEMPLATE = "(%s)";

    // %{SYSLOGTIMESTAMP:timestamp}
    // %{named_rule:field}
    private static final Pattern RULE_REFERENCE_PATTERN = Pattern.compile("%\\{(\\w+)(:(\\w+))?\\}");

    private static final Logger LOGGER = LoggerFactory.getLogger(GrokMapper.class);

    private GrokMapper(@Nonnull final String recordMappingDefinition,
            @Nonnull final ImmutableMap<String, String> configuredPatterns) {

        checkArgument(!isNullOrEmpty(recordMappingDefinition), "record mapping definition must not be null or empty");
        checkArgument(configuredPatterns != null, "map of configured patterns must not be null or empty");

        final String regexExpression = expandRule(recordMappingDefinition, configuredPatterns);
        LOGGER.debug("expanded rule [recordMappingDefinition={}] to [regexExpression={}]", recordMappingDefinition,
            regexExpression);

        regex = new Regex(regexExpression);
    }

    private String expandRule(final String rule, final Map<String, String> configuredRules) {

        LOGGER.debug("expanding [rule={}]", rule);

        final java.util.regex.Matcher matcher = RULE_REFERENCE_PATTERN.matcher(rule);

        String expanded = rule;
        while (matcher.find()) {
            String entireMatch = matcher.group();
            final String matchedRule = matcher.group(1);
            final String configuredPattern = configuredRules.get(matchedRule);

            if (configuredPattern == null) {

                // could not expand further
                continue;
            }

            final String matchedRuleName = matcher.group(3);
            final String expandedRule;
            if (matchedRuleName == null) {
                expandedRule = String.format(RULE_REGEX_PATTERN_TEMPLATE, configuredPattern);
            } else {
                expandedRule = String.format(NAMED_RULE_REF_REGEX_PATTERN_TEMPLATE, matchedRuleName, configuredPattern);
            }

            expanded = expanded.replace(entireMatch, expandedRule);
        }

        if (Objects.equal(expanded, rule)) {
            LOGGER.debug("[rule={}] has been expanded to [expandedRule={}]", rule, expanded);
            return expanded;
        } else {
            LOGGER.debug("going to expand [expanded={}]", expanded);
            return expandRule(expanded, configuredRules);
        }
    }

    /**
     * Maps given input record according to record mapping definition (specified in {#Builder}).
     *
     * @param   input  input record
     *
     * @return  record mapping which might be empty (but never null)
     */
    @Nonnull
    public Map<String, String> map(@Nonnull final String input) {
        checkNotNull(input, "input most not be null");

        final Matcher matcher = regex.matcher(input.getBytes());
        final int result = matcher.search(0, input.length(), Option.DEFAULT);

        if (result == -1) {
            return Collections.emptyMap();
        } else {

            final HashMap<String, String> mappings = Maps.newHashMap();

            int lastMatchEnd = -1;
            int backRefId;
            int matchBegin;
            int matchEnd;
            NameEntry nameEntry;

            final Region region = matcher.getRegion();
            final Iterator<NameEntry> nameEntryIterator = regex.namedBackrefIterator();
            while (nameEntryIterator.hasNext()) {
                nameEntry = nameEntryIterator.next();
                backRefId = nameEntry.getBackRefs()[0];

                matchBegin = region.beg[backRefId];
                matchEnd = region.end[backRefId];

                // we do not consider named sub expressions in our main expression
                if (matchBegin < lastMatchEnd) {
                    continue;
                } else {
                    lastMatchEnd = matchEnd;
                }

                mappings.put(getRuleName(nameEntry), input.substring(matchBegin, matchEnd));
            }

            return mappings;
        }
    }

    private String getRuleName(final NameEntry nameEntry) {
        return new String(nameEntry.name, nameEntry.nameP, nameEntry.nameEnd - nameEntry.nameP);
    }

    /**
     * {@link de.zalando.grok.GrokMapper} Builder.
     */
    public static final class Builder {

        private String recordMappingDefinition;
        private final HashMap<String, String> patternDefinitions;

        public Builder() {
            patternDefinitions = Maps.newHashMap();
        }

        public Builder withRecordMappingDefinition(@Nonnull final String recordMappingDefinition) {

            checkArgument(!isNullOrEmpty(recordMappingDefinition),
                "record mapping definition must not be null or empty");
            this.recordMappingDefinition = recordMappingDefinition;
            return this;
        }

        public Builder withPatternDefinition(final String patternId, final String pattern) {

            checkArgument(!isNullOrEmpty(patternId), "pattern id must not be null or empty");
            checkArgument(!isNullOrEmpty(pattern), "pattern must not be null or empty");

            patternDefinitions.put(patternId, pattern);
            return this;
        }

        public Builder withPatternDefinitions(final Map<String, String> patternDefinitions) {

            checkNotNull(patternDefinitions, "map of pattern definitions must not be null");
            checkArgument(!patternDefinitions.isEmpty(), "map of pattern definitions must not be empty");

            patternDefinitions.putAll(patternDefinitions);
            return this;
        }

        public Builder withDefaultPatternDefinitions() {
            final URL url = GrokMapper.class.getResource("/logstash_patterns");
            return withPatternDefinitionsFromDirectory(url);
        }

        public Builder withPatternDefinitionsFromDirectory(final URL url) {
            try {
                final ImmutableMap<String, String> patternsFromDirectory = PatternLoader.load(url);
                patternDefinitions.putAll(patternsFromDirectory);
                return this;
            } catch (final IOException e) {
                LOGGER.warn("could not load GROK pattern from [directoryPath={}]", url, e);
                throw new GrokMapperException(e);
            }
        }

        public GrokMapper build() {
            checkState(!isNullOrEmpty(recordMappingDefinition),
                "record mapping definition was not specified or is null");

            return new GrokMapper(recordMappingDefinition, ImmutableMap.copyOf(patternDefinitions));
        }

    }

}
