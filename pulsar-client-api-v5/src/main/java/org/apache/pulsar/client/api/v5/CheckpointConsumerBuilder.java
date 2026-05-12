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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;

/**
 * Builder for configuring and creating a {@link CheckpointConsumer}.
 *
 * <p>Since this is an unmanaged consumer (no subscription), the terminal method is
 * {@link #create()} rather than {@code subscribe()}.
 *
 * @param <T> the type of message values the consumer will receive
 */
public interface CheckpointConsumerBuilder<T> {

    /**
     * Create the checkpoint consumer, blocking until it is ready.
     *
     * @return the created {@link CheckpointConsumer}
     * @throws PulsarClientException if the creation fails or a connection error occurs
     */
    CheckpointConsumer<T> create() throws PulsarClientException;

    /**
     * Create the checkpoint consumer asynchronously.
     *
     * @return a {@link CompletableFuture} that completes with the created {@link CheckpointConsumer}
     */
    CompletableFuture<CheckpointConsumer<T>> createAsync();

    // --- Required ---

    /**
     * The topic to consume from.
     *
     * @param topicName the topic name
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> topic(String topicName);

    // --- Start position ---

    /**
     * Set the initial position for this consumer.
     *
     * <p>Use {@link Checkpoint#earliest()}, {@link Checkpoint#latest()}, or
     * {@link Checkpoint#fromByteArray} to create the appropriate starting position.
     *
     * <p>Defaults to {@link Checkpoint#latest()} if not specified.
     *
     * @param checkpoint the checkpoint representing the desired start position
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> startPosition(Checkpoint checkpoint);

    // --- Optional ---

    /**
     * A custom name for this consumer instance.
     *
     * @param name the consumer name
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> consumerName(String name);

    /**
     * Join a named consumer group on this scalable topic. All consumers that pass the
     * same {@code group} share the topic's segments via the broker's subscription
     * coordinator: each segment is assigned to exactly one consumer in the group at a
     * time, and segments rebalance automatically as consumers join or leave.
     *
     * <p>When unset (the default), the consumer is unmanaged: it independently reads
     * every segment from {@link #startPosition the configured start position},
     * unaffected by any other consumer. This matches the original reader-style
     * behavior of {@link CheckpointConsumer}.
     *
     * <p>Unlike {@link StreamConsumerBuilder}'s subscription, joining a group does
     * <em>not</em> cause the broker to persist a cursor: each consumer still resumes
     * from the {@code startPosition} (or a checkpoint deserialized from a previous
     * run) it provides at create time. The group only affects how segments are
     * distributed across the live consumer set; once every member of the group goes
     * away, no broker-side state remains for it.
     *
     * @param group the consumer group name
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> consumerGroup(String group);

    /**
     * Configure end-to-end message encryption for decryption.
     *
     * @param policy the encryption policy to use
     * @return this builder instance for chaining
     * @see ConsumerEncryptionPolicy#builder()
     */
    CheckpointConsumerBuilder<T> encryptionPolicy(ConsumerEncryptionPolicy policy);

    // --- Metadata ---

    /**
     * Add a single property to the consumer metadata.
     *
     * @param key   the property key
     * @param value the property value
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> property(String key, String value);

    /**
     * Add multiple properties to the consumer metadata.
     *
     * @param properties the properties to add
     * @return this builder instance for chaining
     */
    CheckpointConsumerBuilder<T> properties(Map<String, String> properties);
}
