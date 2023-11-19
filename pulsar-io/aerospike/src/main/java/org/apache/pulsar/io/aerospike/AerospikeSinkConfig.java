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
package org.apache.pulsar.io.aerospike;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class AerospikeSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String seedHosts;
    private String keyspace;
    private String columnName;

    // Optional
    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The username for authentication."
    )
    private String userName;
    @FieldDoc(
            required = false,
            defaultValue = "",
            sensitive = true,
            help = "The password for authentication."
    )
    private String password;
    private String keySet;
    private int maxConcurrentRequests = 100;
    private int timeoutMs = 100;
    private int retries = 1;


    /**
     * @deprecated Use {@link #load(String, SinkContext)} instead.
     */
    @Deprecated
    public static AerospikeSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), AerospikeSinkConfig.class);
    }

    public static AerospikeSinkConfig load(String yamlFile, SinkContext context) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return load(mapper.readValue(new File(yamlFile), new TypeReference<Map<String, Object>>() {}), context);
    }

    /**
     * @deprecated Use {@link #load(Map, SinkContext)} instead.
     */
    @Deprecated
    public static AerospikeSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), AerospikeSinkConfig.class);
    }

    public static AerospikeSinkConfig load(Map<String, Object> map, SinkContext context) {
        return IOConfigUtils.loadWithSecrets(map, AerospikeSinkConfig.class, context);
    }
}