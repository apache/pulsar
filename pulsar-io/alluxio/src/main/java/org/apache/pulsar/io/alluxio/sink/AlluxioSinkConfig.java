/*
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
package org.apache.pulsar.io.alluxio.sink;

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.alluxio.AlluxioAbstractConfig;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Setter
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
@Accessors(chain = true)
public class AlluxioSinkConfig extends AlluxioAbstractConfig implements Serializable {

    private static final long serialVersionUID = -8917657634001769807L;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The prefix of the files to create in the Alluxio directory (e.g. a value of 'TopicA' results"
            + " in files named topicA-, topicA-, etc being produced)")
    private String filePrefix;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The extension to add to the files written to Alluxio (e.g. '.txt')")
    private String fileExtension;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The character used to separate records in a text file. If no value is provided then the content"
            + " from all of the records is concatenated together in one continuous byte array")
    private char lineSeparator;

    @FieldDoc(
        required = false,
        defaultValue = "10000L",
        help = "The number records of Alluxio file rotation")
    private long rotationRecords = 10000L;

    @FieldDoc(
        required = false,
        defaultValue = "-1L",
        help = "The interval to rotate a Alluxio file (in milliseconds)")
    private long rotationInterval = -1L;

    @FieldDoc(
        required = false,
        defaultValue = "MUST_CACHE",
        help = "Default write type when creating Alluxio files. Valid options are `MUST_CACHE` (write only goes to"
            + " Alluxio and must be stored in Alluxio), `CACHE_THROUGH` (try to cache, write to UnderFS synchronously),"
            + " `THROUGH` (no cache, write to UnderFS synchronously)")
    private String writeType = "MUST_CACHE";

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Sets whether the Sink has to take into account the Schema or if it should simply copy the raw message"
            + " to Alluxio"
    )
    private boolean schemaEnable = false;

    public static AlluxioSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), AlluxioSinkConfig.class);
    }

    public static AlluxioSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), AlluxioSinkConfig.class);
    }

    @Override
    public void validate() {
        super.validate();
        checkArgument(rotationRecords > 0, "rotationRecords must be a positive long.");
        checkArgument(rotationInterval == -1 || rotationInterval > 0,
            "rotationInterval must be either -1 or a positive long.");
    }
}
