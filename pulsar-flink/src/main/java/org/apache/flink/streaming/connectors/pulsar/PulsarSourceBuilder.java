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
package org.apache.flink.streaming.connectors.pulsar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * A class for building a pulsar source.
 */
@PublicEvolving
public class PulsarSourceBuilder<T> {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final long ACKNOWLEDGEMENT_BATCH_SIZE = 100;
    private static final long MAX_ACKNOWLEDGEMENT_BATCH_SIZE = 1000;

    final DeserializationSchema<T> deserializationSchema;
    String serviceUrl = SERVICE_URL;
    final Set<String> topicNames = new TreeSet<>();
    Pattern topicsPattern;
    String subscriptionName = "flink-sub";
    long acknowledgementBatchSize = ACKNOWLEDGEMENT_BATCH_SIZE;

    private PulsarSourceBuilder(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    /**
     * Sets the pulsar service url to connect to. Defaults to pulsar://localhost:6650.
     *
     * @param serviceUrl service url to connect to
     * @return this builder
     */
    public PulsarSourceBuilder<T> serviceUrl(String serviceUrl) {
        Preconditions.checkArgument(StringUtils.isNotBlank(serviceUrl), "serviceUrl cannot be blank");
        this.serviceUrl = serviceUrl;
        return this;
    }

    /**
     * Sets topics to consumer from. This is required.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topics the topic to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topic(String... topics) {
        Preconditions.checkArgument(topics != null && topics.length > 0,
                "topics cannot be blank");
        for (String topic : topics) {
            Preconditions.checkArgument(StringUtils.isNotBlank(topic), "topicNames cannot have blank topic");
        }
        this.topicNames.addAll(Arrays.asList(topics));
        return this;
    }

    /**
     * Sets topics to consumer from. This is required.
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topics the topic to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topics(List<String> topics) {
        Preconditions.checkArgument(topics != null && !topics.isEmpty(), "topics cannot be blank");
        topics.forEach(topicName ->
                Preconditions.checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        this.topicNames.addAll(topics);
        return this;
    }

    /**
     * Use topic pattern to config sets of topics to consumer
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topicsPattern topic pattern to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topicsPattern(Pattern topicsPattern) {
        Preconditions.checkArgument(topicsPattern != null, "Param topicsPattern cannot be null");
        Preconditions.checkArgument(this.topicsPattern == null, "Pattern has already been set.");
        this.topicsPattern = topicsPattern;
        return this;
    }

    /**
     * Use topic pattern to config sets of topics to consumer
     *
     * <p>Topic names (https://pulsar.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Topics)
     * are in the following format:
     * {persistent|non-persistent}://tenant/namespace/topic
     *
     * @param topicsPattern topic pattern string to consumer from
     * @return this builder
     */
    public PulsarSourceBuilder<T> topicsPatternString(String topicsPattern) {
        Preconditions.checkArgument(StringUtils.isNotBlank(topicsPattern), "Topics pattern string cannot be blank");
        Preconditions.checkArgument(this.topicsPattern == null, "Pattern has already been set.");
        this.topicsPattern = Pattern.compile(topicsPattern);
        return this;
    }

    /**
     * Sets the subscription name for the topic consumer. Defaults to flink-sub.
     *
     * @param subscriptionName the subscription name for the topic consumer
     * @return this builder
     */
    public PulsarSourceBuilder<T> subscriptionName(String subscriptionName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(subscriptionName),
                "subscriptionName cannot be blank");
        this.subscriptionName = subscriptionName;
        return this;
    }

    /**
     * Sets the number of messages to receive before acknowledging. This defaults to 100. This
     * value is only used when checkpointing is disabled.
     *
     * @param size number of messages to receive before acknowledging
     * @return this builder
     */
    public PulsarSourceBuilder<T> acknowledgementBatchSize(long size) {
        if (size > 0 && size <= MAX_ACKNOWLEDGEMENT_BATCH_SIZE) {
            acknowledgementBatchSize = size;
            return this;
        }
        throw new IllegalArgumentException("acknowledgementBatchSize can only take values > 0 and <= " + MAX_ACKNOWLEDGEMENT_BATCH_SIZE);
    }

    public SourceFunction<T> build() {
        Preconditions.checkNotNull(serviceUrl, "a service url is required");
        Preconditions.checkArgument((topicNames != null && !topicNames.isEmpty()) || topicsPattern != null,
                "At least one topic or topics pattern is required");
        Preconditions.checkNotNull(subscriptionName, "a subscription name is required");
        return new PulsarConsumerSource<>(this);
    }

    /**
     * Creates a PulsarSourceBuilder.
     *
     * @param deserializationSchema the deserializer used to convert between Pulsar's byte messages and Flink's objects.
     * @return a builder
     */
    public static <T> PulsarSourceBuilder<T> builder(DeserializationSchema<T> deserializationSchema) {
        Preconditions.checkNotNull(deserializationSchema, "deserializationSchema cannot be null");
        return new PulsarSourceBuilder<>(deserializationSchema);
    }
}
