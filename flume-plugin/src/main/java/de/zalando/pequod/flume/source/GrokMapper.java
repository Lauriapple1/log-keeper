package de.zalando.pequod.flume.source;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

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
final class GrokMapper {

    private final Regex regex;

    // see  (?<name>subexp)
    private static final String NAMED_RULE_REF_REGEX_PATTERN_TEMPLATE = "(?<%s>%s)";
    private static final String RULE_REGEX_PATTERN_TEMPLATE = "(%s)";

    // %{SYSLOGTIMESTAMP:timestamp}
    // %{named_rule:field}
    private static final Pattern RULE_REFERENCE_PATTERN = Pattern.compile("%\\{(\\w+)(:(\\w+))?\\}");

    private static final Logger LOGGER = LoggerFactory.getLogger(GrokMapper.class);

    public GrokMapper(@Nonnull final String recordMappingDefinition,
            @Nonnull final ImmutableMap<String, String> configuredPatterns) {

        checkArgument(!isNullOrEmpty(recordMappingDefinition), "record mapping definition must not be null or empty");
        checkArgument(configuredPatterns != null, "map of configured pattterns must not be null or empty");

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
}
