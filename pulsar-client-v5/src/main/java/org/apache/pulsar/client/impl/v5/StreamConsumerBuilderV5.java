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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumerBuilder;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

/**
 * V5 StreamConsumerBuilder implementation.
 */
final class StreamConsumerBuilderV5<T> implements StreamConsumerBuilder<T> {

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final ConsumerConfigurationData<T> conf = new ConsumerConfigurationData<>();
    private String topicName;

    StreamConsumerBuilderV5(PulsarClientV5 client, Schema<T> v5Schema) {
        this.client = client;
        this.v5Schema = v5Schema;
    }

    @Override
    public StreamConsumer<T> subscribe() throws PulsarClientException {
        try {
            return subscribeAsync().join();
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof PulsarClientException pce) {
                throw pce;
            }
            throw new PulsarClientException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<StreamConsumer<T>> subscribeAsync() {
        if (topicName == null || topicName.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Topic name is required"));
        }
        if (conf.getSubscriptionName() == null || conf.getSubscriptionName().isEmpty()) {
            return CompletableFuture.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Subscription name is required"));
        }

        TopicName topic = V5Utils.asScalableTopicName(topicName);
        DagWatchClient dagWatch = new DagWatchClient(client.v4Client(), topic);

        return dagWatch.start()
                .thenCompose(initialLayout -> ScalableStreamConsumer.createAsync(
                        client, v5Schema, conf, dagWatch, initialLayout));
    }

    @Override
    public StreamConsumerBuilderV5<T> topic(String... topicNames) {
        if (topicNames.length > 0) {
            this.topicName = topicNames[0];
        }
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> subscriptionName(String subscriptionName) {
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> seek(MessageId messageId) {
        // Seek will be applied after consumer creation
        // Store for later use
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> seek(Instant timestamp) {
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> subscriptionProperties(Map<String, String> properties) {
        conf.setSubscriptionProperties(properties);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> subscriptionInitialPosition(SubscriptionInitialPosition position) {
        conf.setSubscriptionInitialPosition(switch (position) {
            case LATEST -> org.apache.pulsar.client.api.SubscriptionInitialPosition.Latest;
            case EARLIEST -> org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
        });
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> consumerName(String consumerName) {
        conf.setConsumerName(consumerName);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> acknowledgmentGroupTime(Duration delay) {
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.MICROSECONDS.convert(delay));
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> replicateSubscriptionState(boolean replicate) {
        conf.setReplicateSubscriptionState(replicate);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> encryptionPolicy(EncryptionPolicy policy) {
        conf.setCryptoKeyReader(CryptoKeyReaderAdapter.wrap(policy.keyReader()));
        conf.setCryptoFailureAction(
                org.apache.pulsar.client.api.ConsumerCryptoFailureAction.valueOf(
                        policy.failureAction().name()));
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> property(String key, String value) {
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public StreamConsumerBuilderV5<T> properties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }
}
