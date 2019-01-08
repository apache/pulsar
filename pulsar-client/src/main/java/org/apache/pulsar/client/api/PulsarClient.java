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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.impl.ClientBuilderImpl;

/**
 * Class that provides a client interface to Pulsar.
 * <p>
 * Client instances are thread-safe and can be reused for managing multiple {@link Producer}, {@link Consumer} and
 * {@link Reader} instances.
 */
public interface PulsarClient extends Closeable {

    /**
     * Get a new builder instance that can used to configure and build a {@link PulsarClient} instance.
     *
     * @return the {@link ClientBuilder}
     *
     * @since 2.0.0
     */
    public static ClientBuilder builder() {
        return new ClientBuilderImpl();
    }

    /**
     * Create a producer with default for publishing on a specific topic
     * <p>
     * Example:
     *
     * <code>
     * Producer producer = client.newProducer().topic(myTopic).create();
     * </code>
     *
     * @return a {@link ProducerBuilder} object to configure and construct the {@link Producer} instance
     *
     * @since 2.0.0
     */
    ProducerBuilder<byte[]> newProducer();

    /**
     * Create a producer with default for publishing on a specific topic
     * <p>
     * Example:
     *
     * <code>
     * Producer producer = client.newProducer(mySchema).topic(myTopic).create();
     * </code>
     *
     * @param schema
     *          provide a way to convert between serialized data and domain objects
     *
     * @return a {@link ProducerBuilder} object to configure and construct the {@link Producer} instance
     *
     * @since 2.0.0
     */
    <T> ProducerBuilder<T> newProducer(Schema<T> schema);

    /**
     * Create a consumer with default for subscribing on a specific topic
     *
     * @return a {@link ConsumerBuilder} object to configure and construct the {@link Consumer} instance
     *
     * @since 2.0.0
     */
    ConsumerBuilder<byte[]> newConsumer();

    /**
     * Create a consumer with default for subscribing on a specific topic
     *
     * Since 2.2, if you are creating a consumer with non-bytes schema on a non-existence topic, it will
     * automatically create the topic with the provided schema.
     *
     * @param schema
     *          provide a way to convert between serialized data and domain objects
     * @return a {@link ConsumerBuilder} object to configure and construct the {@link Consumer} instance
     *
     * @since 2.0.0
     */
    <T> ConsumerBuilder<T> newConsumer(Schema<T> schema);

    /**
     * Create a topic reader for reading messages from the specified topic.
     * <p>
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. Reader can only work on non-partitioned topics.
     *
     * @return a {@link ReaderBuilder} that can be used to configure and construct a {@link Reader} instance
     *
     * @since 2.0.0
     */
    ReaderBuilder<byte[]> newReader();

    /**
     * Create a topic reader for reading messages from the specified topic.
     * <p>
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. Reader can only work on non-partitioned topics.
     *
     * @param schema
     *          provide a way to convert between serialized data and domain objects
     *
     * @return a {@link ReaderBuilder} that can be used to configure and construct a {@link Reader} instance
     *
     * @since 2.0.0
     */
    <T> ReaderBuilder<T> newReader(Schema<T> schema);

    /**
     * Update the service URL this client is using.
     *
     * This will force the client close all existing connections and to restart service discovery to the new service
     * endpoint.
     *
     * @param serviceUrl
     *            the new service URL this client should connect to
     * @throws PulsarClientException
     *             in case the serviceUrl is not valid
     */
    void updateServiceUrl(String serviceUrl) throws PulsarClientException;

    /**
     * Get the list of partitions for a given topic.
     *
     * If the topic is partitioned, this will return a list of partition names. If the topic is not partitioned, the
     * returned list will contain the topic name itself.
     *
     * This can be used to discover the partitions and create {@link Reader}, {@link Consumer} or {@link Producer}
     * instances directly on a particular partition.
     *
     * @param topic
     *            the topic name
     * @return a future that will yield a list of the topic partitions
     * @since 2.3.0
     */
    CompletableFuture<List<String>> getPartitionsForTopic(String topic);

    /**
     * Close the PulsarClient and release all the resources.
     *
     * All the producers and consumers will be orderly closed. Waits until all pending write request are persisted.
     *
     * @throws PulsarClientException
     *             if the close operation fails
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Asynchronously close the PulsarClient and release all the resources.
     *
     * All the producers and consumers will be orderly closed. Waits until all pending write request are persisted.
     *
     * @throws PulsarClientException
     *             if the close operation fails
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Perform immediate shutdown of PulsarClient.
     *
     * Release all the resources and close all the producers without waiting for ongoing operations to complete.
     *
     * @throws PulsarClientException
     *             if the forceful shutdown fails
     */
    void shutdown() throws PulsarClientException;
}
