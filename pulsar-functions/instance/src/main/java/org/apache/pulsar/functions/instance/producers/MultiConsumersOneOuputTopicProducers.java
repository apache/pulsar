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
package org.apache.pulsar.functions.instance.producers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

@Slf4j
public class MultiConsumersOneOuputTopicProducers<T> extends AbstractOneOuputTopicProducers<T> {

    @Getter(AccessLevel.PACKAGE)
    // PartitionId -> producer
    private final Map<String, Producer<T>> producers;

    private final Schema<T> schema;


    public MultiConsumersOneOuputTopicProducers(PulsarClient client,
                                                String outputTopic, Schema<T> schema)
            throws PulsarClientException {
        super(client, outputTopic);
        this.producers = new ConcurrentHashMap<>();
        this.schema = schema;
    }

    @Override
    public void initialize() throws PulsarClientException {
        // no-op
    }

    static String makeProducerName(String srcTopicName, String srcTopicPartition) {
        return String.format("%s-%s", srcTopicName, srcTopicPartition);
    }

    @Override
    public synchronized Producer<T> getProducer(String srcPartitionId) throws PulsarClientException {
        Producer<T> producer = producers.get(srcPartitionId);
        if (null == producer) {
            producer = createProducer(outputTopic, srcPartitionId, schema);
            producers.put(srcPartitionId, producer);
        }
        return producer;
    }

    @Override
    public synchronized void closeProducer(String srcPartitionId) {
        Producer<T> producer = producers.get(srcPartitionId);
        if (null != producer) {
            producer.closeAsync();
            producers.remove(srcPartitionId);
        }
    }

    @Override
    public synchronized void close() {
        List<CompletableFuture<Void>> closeFutures = new ArrayList<>(producers.size());
        for (Producer<T> producer: producers.values()) {
            closeFutures.add(producer.closeAsync());
        }
        try {
            FutureUtils.result(FutureUtils.collect(closeFutures));
        } catch (Exception e) {
            log.warn("Fail to close all the producers for output topic {}", outputTopic, e);
        }
    }
}
