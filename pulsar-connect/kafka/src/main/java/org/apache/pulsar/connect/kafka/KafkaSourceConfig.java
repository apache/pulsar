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

package org.apache.pulsar.connect.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class KafkaSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bootstrapServers;
    private String groupId;
    private Long fetchMinBytes;
    private Long autoCommitIntervalMs;
    private Long sessionTimeoutMs;
    private boolean autoCommitEnabled = true;
    private String topic;
    private String keyDeserializationClass = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializationClass = "org.apache.kafka.common.serialization.StringDeserializer";

    public static KafkaSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KafkaSourceConfig.class);
    }

    public static KafkaSourceConfig load(Map<String, String> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KafkaSourceConfig.class);
    }
}