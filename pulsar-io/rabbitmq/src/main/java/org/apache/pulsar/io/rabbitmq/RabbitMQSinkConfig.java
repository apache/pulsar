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
package org.apache.pulsar.io.rabbitmq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class RabbitMQSinkConfig extends RabbitMQAbstractConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The exchange to publish the messages on")
    private String exchangeName;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The routing key used for publishing the messages")
    private String routingKey;

    @FieldDoc(
        required = false,
        defaultValue = "topic",
        help = "The exchange type to publish the messages on")
    private String exchangeType = "topic";

    /**
     * @deprecated Use {@link #load(String, SinkContext)} instead.
     */
    @Deprecated
    public static RabbitMQSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), RabbitMQSinkConfig.class);
    }

    public static RabbitMQSinkConfig load(String yamlFile, SinkContext context) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return load(mapper.readValue(new File(yamlFile), new TypeReference<Map<String, Object>>() {}), context);
    }

    /**
     * @deprecated Use {@link #load(Map, SinkContext)} instead.
     */
    @Deprecated
    public static RabbitMQSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), RabbitMQSinkConfig.class);
    }

    public static RabbitMQSinkConfig load(Map<String, Object> map, SinkContext context) {
        return IOConfigUtils.loadWithSecrets(map, RabbitMQSinkConfig.class, context);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(exchangeName, "exchangeName property not set.");
    }
}
