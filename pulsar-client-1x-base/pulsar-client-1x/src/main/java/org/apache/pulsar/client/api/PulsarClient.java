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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.impl.v1.PulsarClientV1Impl;

/**
 * Class that provides a client interface to Pulsar.
 * <p>
 * Client instances are thread-safe and can be reused for managing multiple {@link Producer}, {@link Consumer} and
 * {@link Reader} instances.
 */
public interface PulsarClient extends Closeable {

    /**
     * Create a new PulsarClient object using default client configuration.
     *
     * @param serviceUrl
     *            the url of the Pulsar endpoint to be used
     * @return a new pulsar client object
     * @throws PulsarClientException.InvalidServiceURL
     *             if the serviceUrl is invalid
     * @deprecated use {@link #builder()} to construct a client instance
     */
    @Deprecated
     static PulsarClient create(String serviceUrl) throws PulsarClientException {
        return create(serviceUrl, new ClientConfiguration());
    }

    /**
     * Create a new PulsarClient object.
     *
     * @param serviceUrl
     *            the url of the Pulsar endpoint to be used
     * @param conf
     *            the client configuration
     * @return a new pulsar client object
     * @throws PulsarClientException.InvalidServiceURL
     *             if the serviceUrl is invalid
     * @deprecated use {@link #builder()} to construct a client instance
     */
    @Deprecated
    static PulsarClient create(String serviceUrl, ClientConfiguration conf) throws PulsarClientException {
        return new PulsarClientV1Impl(serviceUrl, conf);
    }

    /**
     * Create a producer with default {@link ProducerConfiguration} for publishing on a specific topic.
     *
     * @param topic
     *            The name of the topic where to produce
     * @return The producer object
     * @throws PulsarClientException.AlreadyClosedException
     *             if the client was already closed
     * @throws PulsarClientException.InvalidTopicNameException
     *             if the topic name is not valid
     * @throws PulsarClientException.AuthenticationException
     *             if there was an error with the supplied credentials
     * @throws PulsarClientException.AuthorizationException
     *             if the authorization to publish on topic was denied
     * @deprecated use {@link #newProducer()} to build a new producer
     */
    @Deprecated
    Producer createProducer(String topic) throws PulsarClientException;

    /**
     * Asynchronously create a producer with default {@link ProducerConfiguration} for publishing on a specific topic.
     *
     * @param topic
     *            The name of the topic where to produce
     * @return Future of the asynchronously created producer object
     * @deprecated use {@link #newProducer()} to build a new producer
     */
    @Deprecated
    CompletableFuture<Producer> createProducerAsync(String topic);

    /**
     * Create a producer with given {@code ProducerConfiguration} for publishing on a specific topic.
     *
     * @param topic
     *            The name of the topic where to produce
     * @param conf
     *            The {@code ProducerConfiguration} object
     * @return The producer object
     * @throws PulsarClientException
     *             if it was not possible to create the producer
     * @throws InterruptedException
     * @deprecated use {@link #newProducer()} to build a new producer
     */
    @Deprecated
    Producer createProducer(String topic, ProducerConfiguration conf) throws PulsarClientException;

    /**
     * Asynchronously create a producer with given {@code ProducerConfiguration} for publishing on a specific topic.
     *
     * @param topic
     *            The name of the topic where to produce
     * @param conf
     *            The {@code ProducerConfiguration} object
     * @return Future of the asynchronously created producer object
     * @deprecated use {@link #newProducer()} to build a new producer
     */
    @Deprecated
    CompletableFuture<Producer> createProducerAsync(String topic, ProducerConfiguration conf);

    /**
     * Subscribe to the given topic and subscription combination with default {@code ConsumerConfiguration}.
     *
     * @param topic
     *            The name of the topic
     * @param subscription
     *            The name of the subscription
     * @return The {@code Consumer} object
     * @throws PulsarClientException
     * @throws InterruptedException
     *
     * @deprecated Use {@link #newConsumer()} to build a new consumer
     */
    @Deprecated
    Consumer subscribe(String topic, String subscription) throws PulsarClientException;

    /**
     * Asynchronously subscribe to the given topic and subscription combination using default.
     * {@code ConsumerConfiguration}
     *
     * @param topic
     *            The topic name
     * @param subscription
     *            The subscription name
     * @return Future of the {@code Consumer} object
     * @deprecated Use {@link #newConsumer()} to build a new consumer
     */
    @Deprecated
    CompletableFuture<Consumer> subscribeAsync(String topic, String subscription);

    /**
     * Subscribe to the given topic and subscription combination with given {@code ConsumerConfiguration}.
     *
     * @param topic
     *            The name of the topic
     * @param subscription
     *            The name of the subscription
     * @param conf
     *            The {@code ConsumerConfiguration} object
     * @return The {@code Consumer} object
     * @throws PulsarClientException
     * @deprecated Use {@link #newConsumer()} to build a new consumer
     */
    @Deprecated
    Consumer subscribe(String topic, String subscription, ConsumerConfiguration conf) throws PulsarClientException;

    /**
     * Asynchronously subscribe to the given topic and subscription combination using given.
     * {@code ConsumerConfiguration}
     *
     * @param topic
     *            The name of the topic
     * @param subscription
     *            The name of the subscription
     * @param conf
     *            The {@code ConsumerConfiguration} object
     * @return Future of the {@code Consumer} object
     * @deprecated Use {@link #newConsumer()} to build a new consumer
     */
    @Deprecated
    CompletableFuture<Consumer> subscribeAsync(String topic, String subscription, ConsumerConfiguration conf);

    /**
     * Create a topic reader with given {@code ReaderConfiguration} for reading messages from the specified topic.
     * <p>
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. Reader can only work on non-partitioned topics.
     * <p>
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li><code>MessageId.earliest</code> : Start reading from the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Start reading from the end topic, only getting messages published after the
     * reader was created
     * <li><code>MessageId</code> : When passing a particular message id, the reader will position itself on that
     * specific position. The first message to be read will be the message next to the specified messageId.
     * </ul>
     *
     * @param topic
     *            The name of the topic where to read
     * @param startMessageId
     *            The message id where the reader will position itself. The first message returned will be the one after
     *            the specified startMessageId
     * @param conf
     *            The {@code ReaderConfiguration} object
     * @return The {@code Reader} object
     * @deprecated Use {@link #newReader()} to build a new reader
     */
    @Deprecated
    Reader createReader(String topic, MessageId startMessageId, ReaderConfiguration conf) throws PulsarClientException;

    /**
     * Asynchronously create a topic reader with given {@code ReaderConfiguration} for reading messages from the
     * specified topic.
     * <p>
     * The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. Reader can only work on non-partitioned topics.
     * <p>
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li><code>MessageId.earliest</code> : Start reading from the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Start reading from the end topic, only getting messages published after the
     * reader was created
     * <li><code>MessageId</code> : When passing a particular message id, the reader will position itself on that
     * specific position. The first message to be read will be the message next to the specified messageId.
     * </ul>
     *
     * @param topic
     *            The name of the topic where to read
     * @param startMessageId
     *            The message id where the reader will position itself. The first message returned will be the one after
     *            the specified startMessageId
     * @param conf
     *            The {@code ReaderConfiguration} object
     * @return Future of the asynchronously created producer object
     * @deprecated Use {@link #newReader()} to build a new reader
     */
    @Deprecated
    CompletableFuture<Reader> createReaderAsync(String topic, MessageId startMessageId, ReaderConfiguration conf);

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
