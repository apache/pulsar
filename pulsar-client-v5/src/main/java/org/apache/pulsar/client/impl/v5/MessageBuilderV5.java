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
package org.apache.pulsar.client.impl.v5;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.Transaction;

/**
 * V5 synchronous MessageBuilder implementation.
 * Accumulates metadata and delegates send to ScalableTopicProducer.
 */
final class MessageBuilderV5<T> implements MessageBuilder<T> {

    private final ScalableTopicProducer<T> producer;
    private T value;
    private String key;
    private Map<String, String> properties;
    private Instant eventTime;
    private Long sequenceId;
    private Duration deliverAfter;
    private Instant deliverAt;
    private List<String> replicationClusters;

    MessageBuilderV5(ScalableTopicProducer<T> producer) {
        this.producer = producer;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        return producer.sendInternal(
                key, value, properties, eventTime, sequenceId,
                deliverAfter, deliverAt, replicationClusters);
    }

    @Override
    public MessageBuilderV5<T> value(T value) {
        this.value = value;
        return this;
    }

    @Override
    public MessageBuilderV5<T> key(String key) {
        this.key = key;
        return this;
    }

    @Override
    public MessageBuilderV5<T> transaction(Transaction txn) {
        // TODO: Wire up transaction support
        return this;
    }

    @Override
    public MessageBuilderV5<T> property(String name, String value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
        return this;
    }

    @Override
    public MessageBuilderV5<T> properties(Map<String, String> properties) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.putAll(properties);
        return this;
    }

    @Override
    public MessageBuilderV5<T> eventTime(Instant eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    @Override
    public MessageBuilderV5<T> sequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    @Override
    public MessageBuilderV5<T> deliverAfter(Duration delay) {
        this.deliverAfter = delay;
        return this;
    }

    @Override
    public MessageBuilderV5<T> deliverAt(Instant timestamp) {
        this.deliverAt = timestamp;
        return this;
    }

    @Override
    public MessageBuilderV5<T> replicationClusters(List<String> clusters) {
        this.replicationClusters = clusters;
        return this;
    }
}
