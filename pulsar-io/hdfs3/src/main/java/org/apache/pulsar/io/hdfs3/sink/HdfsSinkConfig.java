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
package org.apache.pulsar.io.hdfs3.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.io.hdfs3.AbstractHdfsConfig;

/**
 * Configuration object for all HDFS Sink components.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class HdfsSinkConfig extends AbstractHdfsConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The prefix of the files to create inside the HDFS directory, i.e. a value of "topicA"
     * will result in files named topicA-, topicA-, etc being produced
     */
    private String filenamePrefix;

    /**
     * The extension to add to the files written to HDFS, e.g. '.txt', '.seq', etc.
     */
    private String fileExtension;

    /**
     * The character to use to separate records in a text file. If no value is provided
     * then the content from all of the records will be concatenated together in one continuous
     * byte array.
     */
    private char separator;

    /**
     * The interval (in milliseconds) between calls to flush data to HDFS disk.
     */
    private long syncInterval;

    /**
     * The maximum number of records that we hold in memory before acking. Default is Integer.MAX_VALUE.
     * Setting this value to one, results in every record being sent to disk before the record is acked,
     * while setting it to a higher values allows us to buffer records before flushing them all to disk.
     */
    private int maxPendingRecords = Integer.MAX_VALUE;

    public static HdfsSinkConfig load(String yamlFile) throws IOException {
       ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
       return mapper.readValue(new File(yamlFile), HdfsSinkConfig.class);
    }

    public static HdfsSinkConfig load(Map<String, Object> map) throws IOException {
       ObjectMapper mapper = new ObjectMapper();
       return mapper.readValue(new ObjectMapper().writeValueAsString(map), HdfsSinkConfig.class);
    }

    @Override
    public void validate() {
        super.validate();
        if ((StringUtils.isEmpty(fileExtension) && getCompression() == null)
            || StringUtils.isEmpty(filenamePrefix)) {
           throw new IllegalArgumentException("Required property not set.");
        }

        if (syncInterval < 0) {
          throw new IllegalArgumentException("Sync Interval cannot be negative");
        }

        if (maxPendingRecords < 1) {
          throw new IllegalArgumentException("Max Pending Records must be a positive integer");
        }
    }
}
