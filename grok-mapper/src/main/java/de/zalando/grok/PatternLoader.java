package de.zalando.grok;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

final class PatternLoader {
    
    private PatternLoader(){}
    
    private static final Pattern CONFIG_PATTERN = Pattern.compile("(\\w+)\\s+(.+?)(#.*)?");
        
    private static final String URL_PROTOCOL_FILE = "file";
    private static final String URL_PROTOCOL_JAR = "jar";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PatternLoader.class);
    
    @Nonnull
    public static  ImmutableMap<String, String> load(@Nonnull final URL url) throws IOException {

        checkNotNull(url, "Given URL must not be null");

        final List<Map<String, String>> patternMaps = Lists.newArrayList();
        
        final String protocol = url.getProtocol();
        if(URL_PROTOCOL_FILE.equals(protocol)){
            final Path patternDirectoryPath = FileSystems.getDefault().getPath(url.getPath());
            final Stream<Path> patternFilesStream = Files.list(patternDirectoryPath);

            patternFilesStream.parallel()
                              .map(path -> {
                                  try {
                                      return new BufferedInputStream(new FileInputStream(path.toFile()));
                                  } catch (final FileNotFoundException e) {
                                      throw new GrokMapperException("could not find file " + path, e);
                                  }
                              })
                              .map(PatternLoader::parseInputStream)
                              .forEach(patternMap -> patternMaps.add(patternMap));
        }
        else if(URL_PROTOCOL_JAR.equals(protocol)) {
            final String jarPath = url.getPath().substring(5, url.getPath().indexOf("!"));
            final JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
            final Enumeration<JarEntry> jarEntries = jar.entries();

            JarEntry jarEntry;
            InputStream in;
            while(jarEntries.hasMoreElements()) {
                jarEntry = jarEntries.nextElement();
                in = jar.getInputStream(jarEntry);
                patternMaps.add(parseInputStream(in));                
            }
        }
        else {
            LOGGER.warn("Unsupported protocol URL {} -> no patterns are parsed from this URL", protocol);
        }
        
        
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        patternMaps.stream().forEach(builder::putAll);
        return builder.build();
    }

    @Nonnull
    public static Map<String, String> parseInputStream(@Nonnull final InputStream in) {
        
        final HashMap<String, String> configuredPatternsMap = Maps.newHashMap();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        final List<String> records = reader.lines().collect(Collectors.toList());

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
  
        
        LOGGER.debug("patterns loaded from given InputStream: {}", configuredPatternsMap);
        return configuredPatternsMap;
    }
}
