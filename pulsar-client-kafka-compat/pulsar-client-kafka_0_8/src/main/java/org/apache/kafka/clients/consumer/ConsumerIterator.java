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
package org.apache.kafka.clients.consumer;

import java.util.Base64;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.pulsar.common.naming.TopicName;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerIterator<K, V> implements Iterator<PulsarMessageAndMetadata<K, V>>, AutoCloseable {

    private final Consumer<byte[]> consumer;
    private final ConcurrentLinkedQueue<Message<byte[]>> receivedMessages;
    private final Optional<Decoder<K>> keyDeSerializer;
    private final Optional<Decoder<V>> valueDeSerializer;
    private final boolean isAutoCommit;
    private volatile MessageId lastConsumedMessageId;
    private static final kafka.serializer.DefaultDecoder DEFAULT_DECODER = new kafka.serializer.DefaultDecoder(null);

    public ConsumerIterator(Consumer<byte[]> consumer, ConcurrentLinkedQueue<Message<byte[]>> receivedMessages,
            Optional<Decoder<K>> keyDeSerializer, Optional<Decoder<V>> valueDeSerializer, boolean isAutoCommit) {
        this.consumer = consumer;
        this.receivedMessages = receivedMessages;
        this.keyDeSerializer = keyDeSerializer;
        this.valueDeSerializer = valueDeSerializer;
        this.isAutoCommit = isAutoCommit;
    }

    @Override
    public boolean hasNext() {
        try {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.MILLISECONDS);
            if (msg != null) {
                receivedMessages.offer(msg);
                return true;
            }
        } catch (PulsarClientException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to receive message for {}-{}, {}", consumer.getTopic(), consumer.getSubscription(),
                        e.getMessage());
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PulsarMessageAndMetadata<K, V> next() {

        Message<byte[]> msg = receivedMessages.poll();
        if (msg == null) {
            try {
                msg = consumer.receive();
            } catch (PulsarClientException e) {
                log.warn("Failed to receive message for {}-{}, {}", consumer.getTopic(), consumer.getSubscription(),
                        e.getMessage(), e);
                throw new RuntimeException(
                        "failed to receive message from " + consumer.getTopic() + "-" + consumer.getSubscription());
            }
        }

        int partition = TopicName.getPartitionIndex(consumer.getTopic());
        long offset = MessageIdUtils.getOffset(msg.getMessageId());
        String key = msg.getKey();
        byte[] value = msg.getValue();

        K desKey = null;
        V desValue = null;

        if (StringUtils.isNotBlank(key)) {
            if (keyDeSerializer.isPresent() && keyDeSerializer.get() instanceof StringDecoder) {
                desKey = (K) key;
            } else {
                byte[] decodedBytes = Base64.getDecoder().decode(key);
                desKey = keyDeSerializer.isPresent() ? keyDeSerializer.get().fromBytes(decodedBytes)
                        : (K) DEFAULT_DECODER.fromBytes(decodedBytes);
            }
        }

        if (value != null) {
            desValue = valueDeSerializer.isPresent() ? valueDeSerializer.get().fromBytes(msg.getData())
                    : (V) DEFAULT_DECODER.fromBytes(msg.getData());
        }

        PulsarMessageAndMetadata<K, V> msgAndMetadata = new PulsarMessageAndMetadata<>(consumer.getTopic(), partition,
                null, offset, keyDeSerializer.orElse(null), valueDeSerializer.orElse(null), desKey, desValue);

        if (isAutoCommit) {
            // Commit the offset of previously dequeued messages
            consumer.acknowledgeCumulativeAsync(msg);
        }

        lastConsumedMessageId = msg.getMessageId();
        return msgAndMetadata;
    }

    protected CompletableFuture<Void> commitOffsets() {
        MessageId msgId = lastConsumedMessageId;
        if (msgId != null) {
            return this.consumer.acknowledgeCumulativeAsync(msgId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
    }
}
