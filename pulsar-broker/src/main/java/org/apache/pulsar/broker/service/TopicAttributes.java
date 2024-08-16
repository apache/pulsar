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
package org.apache.pulsar.broker.service;

import io.opentelemetry.api.common.Attributes;
import java.util.Objects;
import lombok.Getter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

@Getter
public class TopicAttributes {

    protected final Attributes commonAttributes;

    public TopicAttributes(TopicName topicName) {
        Objects.requireNonNull(topicName);
        var builder = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, topicName.getDomain().toString())
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicName.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName.getPartitionedTopicName());
        if (topicName.isPartitioned()) {
                builder.put(OpenTelemetryAttributes.PULSAR_PARTITION_INDEX, topicName.getPartitionIndex());
        }
        commonAttributes = builder.build();
    }
}
