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
package org.apache.pulsar.functions.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionConfig;

import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@ToString
public class PulsarSourceConfig {

    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    SubscriptionType subscriptionType;
    private String subscriptionName;
    private Map<String, String> topicSerdeClassNameMap;
    private String topicsPattern;
    private String typeClassName;
    private Long timeoutMs;

    public static PulsarSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), PulsarSourceConfig.class);
    }
}
