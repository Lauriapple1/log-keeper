package de.zalando.pequod.flume.source;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.isReadable;

final class PatternLoader {
    
    private PatternLoader(){}
    
    private static final Pattern CONFIG_PATTERN = Pattern.compile("(\\w+)\\s+(.+?)(#.*)?");
        
    private static final Logger LOGGER = LoggerFactory.getLogger(PatternLoader.class);
    
    @Nonnull
    public static  ImmutableMap<String, String> load(@Nonnull final String patternDirectoryPathString) throws IOException {

        checkArgument(! isNullOrEmpty(patternDirectoryPathString), "pattern directory path must not be null or empty");
        
        final Path patternDirectoryPath = FileSystems.getDefault().getPath(patternDirectoryPathString);
        
        checkArgument(exists(patternDirectoryPath), "path %s does not exist", patternDirectoryPathString);
        checkArgument(isDirectory(patternDirectoryPath), "%s is not a directory", patternDirectoryPathString);
        checkArgument(isReadable(patternDirectoryPath), "no read permissions for %s", patternDirectoryPathString);
        
        final Stream<Path> patternFilesStream = Files.list(patternDirectoryPath);
        
        final List<Map<String, String>> patternMaps = patternFilesStream.parallel()
                                                                        .map(PatternLoader::parsePatternFile)
                                                                        .collect(Collectors.toList());

        final HashMap<String,String> resultMap = Maps.newHashMap();
        patternMaps.stream().forEach(resultMap::putAll);

        return ImmutableMap.copyOf(resultMap);
    }

    @Nonnull
    private static Map<String, String> parsePatternFile(@Nonnull final Path inputFilePath) {

        LOGGER.debug("parsing file [inputFilePath={}]", inputFilePath);
        final HashMap<String, String> configuredPatternsMap = Maps.newHashMap();
                
        try {
            final List<String> records = Files.readAllLines(inputFilePath);

            Matcher matcher;
            for(String record : records) {
                matcher = CONFIG_PATTERN.matcher(record);
                if(matcher.matches()) {
                    configuredPatternsMap.put(matcher.group(1), matcher.group(2));
                }
                else {
                   LOGGER.debug("[record={}] does not represent pattern configuration -> skipped", record);
                }
            }
        }
        catch(final IOException e) {
           LOGGER.warn("could not parse pattern file [inputFilePath={}]", inputFilePath, e);       
        }
        
        LOGGER.debug("patterns loaded from [inputFilePath={}]: {}", inputFilePath, configuredPatternsMap);
        return configuredPatternsMap;
    }
}
