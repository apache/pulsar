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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

/**
 * V5 QueueConsumerBuilder implementation.
 */
final class QueueConsumerBuilderV5<T> implements QueueConsumerBuilder<T> {

    private final PulsarClientV5 client;
    private final Schema<T> v5Schema;
    private final ConsumerConfigurationData<T> conf = new ConsumerConfigurationData<>();
    private String topicName;

    QueueConsumerBuilderV5(PulsarClientV5 client, Schema<T> v5Schema) {
        this.client = client;
        this.v5Schema = v5Schema;
    }

    @Override
    public QueueConsumer<T> subscribe() throws PulsarClientException {
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
    public CompletableFuture<QueueConsumer<T>> subscribeAsync() {
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
                .thenCompose(initialLayout -> ScalableQueueConsumer.createAsync(
                        client, v5Schema, conf, dagWatch, initialLayout));
    }

    @Override
    public QueueConsumerBuilderV5<T> topic(String... topicNames) {
        if (topicNames.length > 0) {
            this.topicName = topicNames[0];
        }
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> topics(List<String> topicNames) {
        if (!topicNames.isEmpty()) {
            this.topicName = topicNames.get(0);
        }
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> topicsPattern(Pattern pattern) {
        conf.setTopicsPattern(pattern);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> topicsPattern(String regex) {
        conf.setTopicsPattern(Pattern.compile(regex));
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> subscriptionName(String subscriptionName) {
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> subscriptionProperties(Map<String, String> properties) {
        conf.setSubscriptionProperties(properties);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> subscriptionInitialPosition(SubscriptionInitialPosition position) {
        conf.setSubscriptionInitialPosition(switch (position) {
            case LATEST -> org.apache.pulsar.client.api.SubscriptionInitialPosition.Latest;
            case EARLIEST -> org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
        });
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> consumerName(String consumerName) {
        conf.setConsumerName(consumerName);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> receiverQueueSize(int receiverQueueSize) {
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> priorityLevel(int priorityLevel) {
        conf.setPriorityLevel(priorityLevel);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> ackTimeout(Duration timeout) {
        conf.setAckTimeoutMillis(timeout.toMillis());
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> acknowledgmentGroupTime(Duration delay) {
        conf.setAcknowledgementsGroupTimeMicros(TimeUnit.MICROSECONDS.convert(delay));
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> maxAcknowledgmentGroupSize(int size) {
        conf.setMaxAcknowledgmentGroupSize(size);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> negativeAckRedeliveryBackoff(BackoffPolicy backoff) {
        conf.setNegativeAckRedeliveryBackoff(
                org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(backoff.initialInterval().toMillis())
                        .maxDelayMs(backoff.maxInterval().toMillis())
                        .multiplier(backoff.multiplier())
                        .build());
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> ackTimeoutRedeliveryBackoff(BackoffPolicy backoff) {
        conf.setAckTimeoutRedeliveryBackoff(
                org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(backoff.initialInterval().toMillis())
                        .maxDelayMs(backoff.maxInterval().toMillis())
                        .multiplier(backoff.multiplier())
                        .build());
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> deadLetterPolicy(DeadLetterPolicy policy) {
        var builder = org.apache.pulsar.client.api.DeadLetterPolicy.builder()
                .maxRedeliverCount(policy.maxRedeliverCount());
        if (policy.retryLetterTopic() != null) {
            builder.retryLetterTopic(policy.retryLetterTopic());
        }
        if (policy.deadLetterTopic() != null) {
            builder.deadLetterTopic(policy.deadLetterTopic());
        }
        if (policy.initialSubscriptionName() != null) {
            builder.initialSubscriptionName(policy.initialSubscriptionName());
        }
        conf.setDeadLetterPolicy(builder.build());
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> patternAutoDiscoveryPeriod(Duration interval) {
        conf.setPatternAutoDiscoveryPeriod((int) interval.getSeconds());
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> encryptionPolicy(EncryptionPolicy policy) {
        conf.setCryptoKeyReader(CryptoKeyReaderAdapter.wrap(policy.keyReader()));
        conf.setCryptoFailureAction(
                org.apache.pulsar.client.api.ConsumerCryptoFailureAction.valueOf(
                        policy.failureAction().name()));
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> property(String key, String value) {
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public QueueConsumerBuilderV5<T> properties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }
}
