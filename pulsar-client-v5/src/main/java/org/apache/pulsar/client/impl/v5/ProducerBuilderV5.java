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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ChunkingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

/**
 * V5 ProducerBuilder implementation.
 * Accumulates configuration and creates a ScalableTopicProducer when create() is called.
 */
final class ProducerBuilderV5<T> implements ProducerBuilder<T> {

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final ProducerConfigurationData conf = new ProducerConfigurationData();

    ProducerBuilderV5(PulsarClientV5 client, Schema<T> v5Schema) {
        this.client = client;
        this.v5Schema = v5Schema;
    }

    @Override
    public Producer<T> create() throws PulsarClientException {
        try {
            return createAsync().join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof PulsarClientException pce) {
                throw pce;
            }
            throw new PulsarClientException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Producer<T>> createAsync() {
        String topicStr = conf.getTopicName();
        if (topicStr == null || topicStr.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Topic name is required"));
        }

        TopicName topicName = V5Utils.asScalableTopicName(topicStr);

        // Create DAG watch client and start the session
        DagWatchClient dagWatch = new DagWatchClient(client.v4Client(), topicName);

        return dagWatch.start()
                .thenApply(initialLayout -> {
                    ScalableTopicProducer<T> producer = new ScalableTopicProducer<>(
                            client, v5Schema, conf, dagWatch, initialLayout);
                    return (Producer<T>) producer;
                });
    }

    @Override
    public ProducerBuilderV5<T> topic(String topicName) {
        conf.setTopicName(topicName);
        return this;
    }

    @Override
    public ProducerBuilderV5<T> producerName(String producerName) {
        conf.setProducerName(producerName);
        return this;
    }

    @Override
    public ProducerBuilderV5<T> accessMode(ProducerAccessMode accessMode) {
        conf.setAccessMode(org.apache.pulsar.client.api.ProducerAccessMode.valueOf(accessMode.name()));
        return this;
    }

    @Override
    public ProducerBuilderV5<T> sendTimeout(Duration timeout) {
        conf.setSendTimeoutMs(timeout.toMillis());
        return this;
    }

    @Override
    public ProducerBuilderV5<T> blockIfQueueFull(boolean blockIfQueueFull) {
        conf.setBlockIfQueueFull(blockIfQueueFull);
        return this;
    }

    @Override
    public ProducerBuilderV5<T> compressionPolicy(CompressionPolicy policy) {
        conf.setCompressionType(
                org.apache.pulsar.client.api.CompressionType.valueOf(policy.type().name()));
        return this;
    }

    @Override
    public ProducerBuilderV5<T> batchingPolicy(BatchingPolicy policy) {
        conf.setBatchingEnabled(policy.enabled());
        conf.setBatchingMaxPublishDelayMicros(
                policy.maxPublishDelay().toNanos() / 1000, TimeUnit.MICROSECONDS);
        conf.setBatchingMaxMessages(policy.maxMessages());
        conf.setBatchingMaxBytes((int) policy.maxSize().bytes());
        return this;
    }

    @Override
    public ProducerBuilderV5<T> chunkingPolicy(ChunkingPolicy policy) {
        conf.setChunkingEnabled(policy.enabled());
        if (policy.chunkSize() > 0) {
            conf.setChunkMaxMessageSize(policy.chunkSize());
        }
        return this;
    }

    @Override
    public ProducerBuilderV5<T> encryptionPolicy(EncryptionPolicy policy) {
        conf.setCryptoKeyReader(CryptoKeyReaderAdapter.wrap(policy.keyReader()));
        conf.setEncryptionKeys(new HashSet<>(policy.keyNames()));
        conf.setCryptoFailureAction(
                org.apache.pulsar.client.api.ProducerCryptoFailureAction.valueOf(
                        policy.failureAction().name()));
        return this;
    }

    @Override
    public ProducerBuilderV5<T> initialSequenceId(long initialSequenceId) {
        conf.setInitialSequenceId(initialSequenceId);
        return this;
    }

    @Override
    public ProducerBuilderV5<T> property(String key, String value) {
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public ProducerBuilderV5<T> properties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }
}
