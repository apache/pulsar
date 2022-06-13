/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Configuration class for the File Source Connector.
 */
@Data
@Accessors(chain = true)
public class FileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The input directory from which to pull files.
     */
    private String inputDirectory;

    /**
     * Indicates whether or not to pull files from sub-directories.
     */
    private Boolean recurse;

    /**
     * If true, the file is not deleted after it has been processed and
     * causes the file to be picked up continually.
     */
    private Boolean keepFile = Boolean.FALSE;

    /**
     * Only files whose names match the given regular expression will be picked up.
     */
    private String fileFilter = "[^.].*";

    /**
     * When 'recurse' property is true, then only sub-directories whose
     * path matches the given regular expression will be scanned.
     */
    private String pathFilter;

    /**
     * The minimum age that a file must be in order to be processed; any file younger
     * than this amount of time (according to last modification date) will be ignored.
     */
    private Integer minimumFileAge;

    /**
     * The maximum age that a file must be in order to be processed; any file older
     * than this amount of time (according to last modification date) will be ignored.
     */
    private Long maximumFileAge;

    /**
     * The minimum size (in bytes) that a file must be in order to be processed.
     */
    private Integer minimumSize;

    /**
     * The maximum size (in bytes) that a file can be in order to be processed.
     */
    private Double maximumSize;

    /**
     * Indicates whether or not hidden files should be ignored or not.
     */
    private Boolean ignoreHiddenFiles;

    /**
     * Indicates how long to wait before performing a directory listing.
     */
    private Long pollingInterval;

    /**
     * The number of worker threads that will be processing the files.
     * This allows you to process a larger number of files concurrently.
     * However, setting this to a value greater than 1 will result in the data
     * from multiple files being "intermingled" in the target topic.
     */
    private Integer numWorkers = 1;

    /**
     * If set, do not delete but only rename file that has been processed.
     * This config only work when 'keepFile' property is false.
     */
    private String processedFileSuffix;

    public static FileSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), FileSourceConfig.class);
    }

    public static FileSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), FileSourceConfig.class);
    }

    public void validate() {
        if (StringUtils.isBlank(inputDirectory)) {
            throw new IllegalArgumentException("Required property not set.");
        } else if (Files.notExists(Paths.get(inputDirectory), LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Specified input directory does not exist");
        } else if (!Files.isReadable(Paths.get(inputDirectory))) {
            throw new IllegalArgumentException("Specified input directory is not readable");
        } else if (!Optional.ofNullable(keepFile).orElse(false) && !Files.isWritable(Paths.get(inputDirectory))) {
            throw new IllegalArgumentException("You have requested the consumed files to be deleted, but the "
                    + "source directory is not writeable.");
        }

        if (StringUtils.isNotBlank(fileFilter)) {
            try {
                Pattern.compile(fileFilter);
            } catch (final PatternSyntaxException psEx) {
                throw new IllegalArgumentException("Invalid Regex pattern provided for fileFilter");
            }
        }

        if (minimumFileAge != null &&  Math.signum(minimumFileAge) < 0) {
            throw new IllegalArgumentException("The property minimumFileAge must be non-negative");
        }

        if (maximumFileAge != null && Math.signum(maximumFileAge) < 0) {
            throw new IllegalArgumentException("The property maximumFileAge must be non-negative");
        }

        if (minimumSize != null && Math.signum(minimumSize) < 0) {
            throw new IllegalArgumentException("The property minimumSize must be non-negative");
        }

        if (maximumSize != null && Math.signum(maximumSize) < 0) {
            throw new IllegalArgumentException("The property maximumSize must be non-negative");
        }

        if (pollingInterval != null && pollingInterval <= 0) {
            throw new IllegalArgumentException("The property pollingInterval must be greater than zero");
        }

        if (numWorkers != null && numWorkers <= 0) {
            throw new IllegalArgumentException("The property numWorkers must be greater than zero");
        }

        if (processedFileSuffix != null && keepFile) {
            throw new IllegalArgumentException(
                    "The property keepFile must be false if the property processedFileSuffix is set");
        }
    }
}
