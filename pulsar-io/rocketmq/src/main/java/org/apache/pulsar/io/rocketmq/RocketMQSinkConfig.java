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

package org.apache.pulsar.io.rocketmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class RocketMQSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help =
                    "A comma-separated list of host and port pairs that are the addresses of "
                            + "the RocketMQ NameServers that a RocketMQ client connects to initially namesrvAddr itself")
    private String namesrvAddr;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help =
                    "A collection of the same type of Producer, which sends the same type of messages with consistent logic")
    private String producerGroup;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help =
                    "The RocketMQ topic that is used for Pulsar moving messages to.")
    private String topic;
    @FieldDoc(
            required = true,
            defaultValue = "*",
            help =
                    "Flags set for messages to distinguish different types of messages under the same topic, functioning as a sub-topic.")
    private String tag;

    public static RocketMQSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), RocketMQSinkConfig.class);
    }

    public static RocketMQSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), RocketMQSinkConfig.class);
    }

    public void validate() {
        Preconditions.checkNotNull(namesrvAddr, "namesrvAddr property not set.");
        Preconditions.checkNotNull(producerGroup, "producerGroup property not set.");
        Preconditions.checkNotNull(topic, "topic property not set.");
    }
}