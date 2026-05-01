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
package org.apache.pulsar.client.api.v5;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.config.EncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.ProcessingTimeoutPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;

/**
 * Builder for configuring and creating a {@link QueueConsumer}.
 *
 * @param <T> the type of message values the consumer will receive
 */
public interface QueueConsumerBuilder<T> {

    /**
     * Subscribe and create the queue consumer, blocking until ready.
     *
     * @return the created {@link QueueConsumer}
     * @throws PulsarClientException if the subscription fails or a connection error occurs
     */
    QueueConsumer<T> subscribe() throws PulsarClientException;

    /**
     * Subscribe and create the queue consumer asynchronously.
     *
     * @return a {@link CompletableFuture} that completes with the created {@link QueueConsumer}
     */
    CompletableFuture<QueueConsumer<T>> subscribeAsync();

    // --- Topic selection ---

    /**
     * The topic(s) to subscribe to.
     *
     * @param topicNames one or more topic names
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> topic(String... topicNames);

    /**
     * The topics to subscribe to.
     *
     * @param topicNames the list of topic names
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * Subscribe to all topics matching a regex pattern.
     *
     * @param pattern the compiled regex pattern to match topic names against
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> topicsPattern(Pattern pattern);

    /**
     * Subscribe to all topics matching a regex pattern (string form).
     *
     * @param regex the regex pattern string to match topic names against
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> topicsPattern(String regex);

    // --- Subscription ---

    /**
     * The subscription name. Required for managed consumers.
     *
     * @param subscriptionName the subscription name
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Properties to attach to the subscription.
     *
     * @param properties the subscription properties
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> subscriptionProperties(Map<String, String> properties);

    /**
     * Initial position when the subscription is first created.
     *
     * @param position the initial position
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition position);

    // --- Consumer identity ---

    /**
     * A custom name for this consumer instance.
     *
     * @param consumerName the consumer name
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> consumerName(String consumerName);

    /**
     * Size of the receiver queue. Controls prefetch depth.
     *
     * @param receiverQueueSize the receiver queue size
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Priority level for this consumer (lower values mean higher priority for
     * message dispatch).
     *
     * @param priorityLevel the priority level
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> priorityLevel(int priorityLevel);

    // --- Acknowledgment ---

    /**
     * Optional safety net for slow / stalled consumers: see
     * {@link ProcessingTimeoutPolicy} for the full semantics. The policy bundles the
     * timeout itself with an optional redelivery backoff. Disabled by default.
     *
     * @param policy timeout + redelivery-backoff configuration
     * @return this builder instance for chaining
     * @see ProcessingTimeoutPolicy#of(Duration)
     * @see ProcessingTimeoutPolicy#of(Duration, BackoffPolicy)
     */
    QueueConsumerBuilder<T> processingTimeout(ProcessingTimeoutPolicy policy);

    /**
     * How frequently acknowledgments are flushed to the broker.
     *
     * @param delay the acknowledgment group time
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> acknowledgmentGroupTime(Duration delay);

    /**
     * Maximum number of acknowledgments to group before flushing.
     *
     * @param size the maximum acknowledgment group size
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> maxAcknowledgmentGroupSize(int size);

    // --- Redelivery ---

    /**
     * Backoff strategy for redelivery after negative acknowledgment.
     *
     * @param backoff the backoff policy to use for negative ack redelivery
     * @return this builder instance for chaining
     * @see BackoffPolicy#fixed(Duration)
     * @see BackoffPolicy#exponential(Duration, Duration)
     */
    QueueConsumerBuilder<T> negativeAckRedeliveryBackoff(BackoffPolicy backoff);

    // --- Dead letter queue ---

    /**
     * Configure the dead letter queue policy.
     *
     * @param policy the dead letter policy
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy policy);

    // --- Pattern subscription ---

    /**
     * How often to re-discover topics matching the pattern.
     *
     * @param interval the auto-discovery interval
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> patternAutoDiscoveryPeriod(Duration interval);

    // --- Encryption ---

    /**
     * Configure end-to-end message encryption for decryption.
     *
     * @param policy the encryption policy to use
     * @return this builder instance for chaining
     * @see EncryptionPolicy#forConsumer
     */
    QueueConsumerBuilder<T> encryptionPolicy(EncryptionPolicy policy);


    // --- Misc ---

    /**
     * Add a single property to the consumer metadata.
     *
     * @param key   the property key
     * @param value the property value
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> property(String key, String value);

    /**
     * Add multiple properties to the consumer metadata.
     *
     * @param properties the properties to add
     * @return this builder instance for chaining
     */
    QueueConsumerBuilder<T> properties(Map<String, String> properties);
}
