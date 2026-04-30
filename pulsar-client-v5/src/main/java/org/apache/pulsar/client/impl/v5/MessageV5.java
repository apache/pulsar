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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;

/**
 * V5 Message implementation that wraps a v4 Message.
 */
final class MessageV5<T> implements Message<T> {

    private final org.apache.pulsar.client.api.Message<T> v4Message;
    private final MessageIdV5 messageId;

    /**
     * Create with a simple segment ID (for queue consumer, checkpoint consumer, producer).
     */
    MessageV5(org.apache.pulsar.client.api.Message<T> v4Message, long segmentId) {
        this.v4Message = v4Message;
        this.messageId = new MessageIdV5(v4Message.getMessageId(), segmentId);
    }

    /**
     * Create with a pre-built MessageIdV5 that carries a position vector
     * (for stream consumer cumulative ack support).
     */
    MessageV5(org.apache.pulsar.client.api.Message<T> v4Message, MessageIdV5 messageId) {
        this.v4Message = v4Message;
        this.messageId = messageId;
    }

    @Override
    public T value() {
        return v4Message.getValue();
    }

    @Override
    public byte[] data() {
        return v4Message.getData();
    }

    @Override
    public MessageId id() {
        return messageId;
    }

    @Override
    public Optional<String> key() {
        return v4Message.hasKey() ? Optional.of(v4Message.getKey()) : Optional.empty();
    }

    @Override
    public Map<String, String> properties() {
        return v4Message.getProperties();
    }

    @Override
    public Instant publishTime() {
        return Instant.ofEpochMilli(v4Message.getPublishTime());
    }

    @Override
    public Optional<Instant> eventTime() {
        long eventTime = v4Message.getEventTime();
        return eventTime > 0 ? Optional.of(Instant.ofEpochMilli(eventTime)) : Optional.empty();
    }

    @Override
    public long sequenceId() {
        return v4Message.getSequenceId();
    }

    @Override
    public Optional<String> producerName() {
        String name = v4Message.getProducerName();
        return name != null && !name.isEmpty() ? Optional.of(name) : Optional.empty();
    }

    @Override
    public String topic() {
        return v4Message.getTopicName();
    }

    @Override
    public int redeliveryCount() {
        return v4Message.getRedeliveryCount();
    }

    @Override
    public int size() {
        return v4Message.size();
    }

    @Override
    public Optional<String> replicatedFrom() {
        String from = v4Message.getReplicatedFrom();
        return from != null && !from.isEmpty() ? Optional.of(from) : Optional.empty();
    }
}
