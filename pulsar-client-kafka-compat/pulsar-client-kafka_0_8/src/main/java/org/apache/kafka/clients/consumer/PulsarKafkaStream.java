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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import com.google.common.collect.Queues;

import kafka.serializer.Decoder;

/**
 * We can't extends "kafka.consumer.KafkaStream<K,V>" because it's a scala class which brings ambiguous overriden
 * methods that gives compilation errors.
 *
 * @param <K>
 * @param <V>
 */
public class PulsarKafkaStream<K, V> implements Iterable<PulsarMessageAndMetadata<K, V>>, AutoCloseable {

    private final Optional<Decoder<K>> keyDeSerializer;
    private final Optional<Decoder<V>> valueDeSerializer;
    private final ConsumerIterator<K, V> iterator;

    private final ConcurrentLinkedQueue<Message<byte[]>> receivedMessages = Queues.newConcurrentLinkedQueue();

    public PulsarKafkaStream(Decoder<K> keyDecoder, Decoder<V> valueDecoder, Consumer<byte[]> consumer,
            boolean isAutoCommit, String clientId) {
        this.keyDeSerializer = Optional.ofNullable(keyDecoder);
        this.valueDeSerializer = Optional.ofNullable(valueDecoder);
        this.iterator = new ConsumerIterator<>(consumer, receivedMessages, keyDeSerializer, valueDeSerializer,
                isAutoCommit);
    }

    @Override
    public ConsumerIterator<K, V> iterator() {
        return iterator;
    }

    public CompletableFuture<Void> commitOffsets() {
        return iterator.commitOffsets();
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }
}
