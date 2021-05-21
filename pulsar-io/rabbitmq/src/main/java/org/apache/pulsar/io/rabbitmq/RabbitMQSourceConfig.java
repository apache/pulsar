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
package org.apache.pulsar.io.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class RabbitMQSourceConfig extends RabbitMQAbstractConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The RabbitMQ queue name from which messages should be read from or written to")
    private String queueName;

    @FieldDoc(
        required = false,
        defaultValue = "0",
        help = "Maximum number of messages that the server will deliver, 0 for unlimited")
    private int prefetchCount = 0;

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Set true if the settings should be applied to the entire channel rather than each consumer")
    private boolean prefetchGlobal = false;

    @FieldDoc(
            required=false,
            defaultValue = "false",
            help = "Set true if the queue should be declared passively - ie to preserve durability/timeout settings")
    private boolean passive = false;

    public static RabbitMQSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), RabbitMQSourceConfig.class);
    }

    public static RabbitMQSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), RabbitMQSourceConfig.class);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(queueName, "queueName property not set.");
        Preconditions.checkArgument(prefetchCount >= 0, "prefetchCount must be non-negative.");
    }
}
