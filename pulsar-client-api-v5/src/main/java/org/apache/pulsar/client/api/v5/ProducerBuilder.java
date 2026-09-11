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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ChunkingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.config.ProducerEncryptionPolicy;

/**
 * Builder for configuring and creating a {@link Producer}.
 *
 * @param <T> the type of message values the producer will send
 */
public interface ProducerBuilder<T> {

    /**
     * Create the producer, blocking until it is ready.
     *
     * @return the configured {@link Producer} instance
     * @throws PulsarClientException if the producer cannot be created (e.g. topic does not exist,
     *         authorization failure, or connection error)
     */
    Producer<T> create() throws PulsarClientException;

    /**
     * Create the producer asynchronously.
     *
     * @return a {@link CompletableFuture} that completes with the configured {@link Producer},
     *         or completes exceptionally with {@link PulsarClientException} on failure
     */
    CompletableFuture<Producer<T>> createAsync();

    // --- Required ---

    /**
     * The topic to produce to. This is required and must be set before calling {@link #create()}.
     *
     * @param topicName the fully qualified topic name (e.g. {@code topic://tenant/namespace/my-topic})
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> topic(String topicName);

    // --- Optional ---

    /**
     * Set a custom producer name. If not set, the broker assigns a unique name.
     * The producer name is used for message deduplication and appears in broker logs.
     *
     * @param producerName the producer name
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> producerName(String producerName);

    /**
     * Access mode for this producer on the topic.
     *
     * @param accessMode the access mode (e.g. {@link ProducerAccessMode#SHARED},
     *        {@link ProducerAccessMode#EXCLUSIVE})
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> accessMode(ProducerAccessMode accessMode);

    /**
     * Timeout for a send operation. If the message is not acknowledged by the broker within
     * this duration, the send future completes exceptionally. A value of {@link Duration#ZERO}
     * disables the timeout.
     *
     * @param timeout the send timeout duration
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> sendTimeout(Duration timeout);

    /**
     * Whether the producer should block when the pending message queue is full,
     * rather than failing immediately. Default is {@code true}.
     *
     * @param blockIfQueueFull {@code true} to block, {@code false} to fail immediately
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull);

    /**
     * Configure compression for message payloads.
     *
     * @param policy the compression policy
     * @return this builder instance for chaining
     * @see CompressionPolicy#of(org.apache.pulsar.client.api.v5.config.CompressionType)
     * @see CompressionPolicy#disabled()
     */
    ProducerBuilder<T> compressionPolicy(CompressionPolicy policy);

    /**
     * Configure message batching. When enabled, the producer groups multiple messages
     * into a single broker request to improve throughput.
     *
     * @param policy the batching policy
     * @return this builder instance for chaining
     * @see BatchingPolicy#ofDefault()
     * @see BatchingPolicy#ofDisabled()
     * @see BatchingPolicy#of(Duration, int, int)
     */
    ProducerBuilder<T> batchingPolicy(BatchingPolicy policy);

    /**
     * Enable chunking for large messages that exceed the broker's max message size.
     *
     * @param policy the chunking policy
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> chunkingPolicy(ChunkingPolicy policy);

    /**
     * Configure end-to-end message encryption.
     *
     * @param policy the producer-side encryption policy
     * @return this builder instance for chaining
     * @see ProducerEncryptionPolicy#builder()
     */
    ProducerBuilder<T> encryptionPolicy(ProducerEncryptionPolicy policy);

    /**
     * Set the initial sequence ID for producer message deduplication. Subsequent messages
     * are assigned incrementing sequence IDs starting from this value.
     *
     * @param initialSequenceId the starting sequence ID
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> initialSequenceId(long initialSequenceId);

    // --- Metadata ---

    /**
     * Add a single property to the producer metadata. Properties are sent to the broker
     * and can be used for filtering and identification.
     *
     * @param key   the property key
     * @param value the property value
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> property(String key, String value);

    /**
     * Add multiple properties to the producer metadata.
     *
     * @param properties a map of property key-value pairs
     * @return this builder instance for chaining
     */
    ProducerBuilder<T> properties(Map<String, String> properties);
}
