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
package org.apache.pulsar.io.influxdb.v1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Configuration class for the InfluxDB Sink Connector.
 */
@Data
@Accessors(chain = true)
public class InfluxDBSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The url of the InfluxDB instance to connect to"
    )
    private String influxdbUrl;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The username used to authenticate to InfluxDB"
    )
    private String username;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "The password used to authenticate to InfluxDB"
    )
    private String password;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The InfluxDB database to write to"
    )
    private String database;

    @FieldDoc(
        required = false,
        defaultValue = "ONE",
        help = "The consistency level for writing data to InfluxDB. Possible values [ALL, ANY, ONE, QUORUM]")
    private String consistencyLevel = "ONE";

    @FieldDoc(
        required = false,
        defaultValue = "NONE",
        help = "The log level for InfluxDB request and response. Possible values [NONE, BASIC, HEADERS, FULL]")
    private String logLevel = "NONE";

    @FieldDoc(
        required = false,
        defaultValue = "autogen",
        help = "The retention policy for the InfluxDB database")
    private String retentionPolicy = "autogen";

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Flag to determine if gzip should be enabled")
    private boolean gzipEnable = false;

    @FieldDoc(
        required = false,
        defaultValue = "1000L",
        help = "The InfluxDB operation time in milliseconds")
    private long batchTimeMs = 1000L;

    @FieldDoc(
        required = false,
        defaultValue = "200",
        help = "The batch size of write to InfluxDB database"
    )
    private int batchSize = 200;

    public static InfluxDBSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), InfluxDBSinkConfig.class);
    }

    public static InfluxDBSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), InfluxDBSinkConfig.class);
    }

    public void validate() {
        Preconditions.checkNotNull(influxdbUrl, "influxdbUrl property not set.");
        Preconditions.checkNotNull(database, "database property not set.");
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
        Preconditions.checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
    }
}
