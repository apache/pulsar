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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;

/**
 * V5 CheckpointConsumerBuilder implementation.
 */
final class CheckpointConsumerBuilderV5<T> implements CheckpointConsumerBuilder<T> {

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private String topicName;
    private Checkpoint startPosition = CheckpointV5.LATEST;
    private String consumerName;
    private EncryptionPolicy encryptionPolicy;

    CheckpointConsumerBuilderV5(PulsarClientV5 client, Schema<T> v5Schema) {
        this.client = client;
        this.v5Schema = v5Schema;
    }

    @Override
    public CheckpointConsumer<T> create() throws PulsarClientException {
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
    public CompletableFuture<CheckpointConsumer<T>> createAsync() {
        if (topicName == null || topicName.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Topic name is required"));
        }

        TopicName topic = V5Utils.asScalableTopicName(topicName);
        DagWatchClient dagWatch = new DagWatchClient(client.v4Client(), topic);

        return dagWatch.start()
                .thenCompose(initialLayout -> ScalableCheckpointConsumer.createAsync(
                        client, v5Schema, dagWatch, initialLayout, startPosition, consumerName));
    }

    @Override
    public CheckpointConsumerBuilderV5<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public CheckpointConsumerBuilderV5<T> startPosition(Checkpoint checkpoint) {
        this.startPosition = checkpoint;
        return this;
    }

    @Override
    public CheckpointConsumerBuilderV5<T> consumerName(String name) {
        this.consumerName = name;
        return this;
    }

    @Override
    public CheckpointConsumerBuilderV5<T> encryptionPolicy(EncryptionPolicy policy) {
        this.encryptionPolicy = policy;
        return this;
    }

    @Override
    public CheckpointConsumerBuilderV5<T> property(String key, String value) {
        // Properties will be passed to readers when created
        return this;
    }

    @Override
    public CheckpointConsumerBuilderV5<T> properties(Map<String, String> properties) {
        return this;
    }
}
