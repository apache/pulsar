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
package org.apache.pulsar.functions.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Data
public class SingleConsumerPulsarSourceConfig extends PulsarSourceConfig {

    private String topic;
    private ConsumerConfig consumerConfig;

    public static SingleConsumerPulsarSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), SingleConsumerPulsarSourceConfig.class);
    }
}
