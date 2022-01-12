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
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Class that provides a client interface to Pulsar.
 *
 * <p>Client instances are thread-safe and can be reused for managing multiple {@link Producer}, {@link Consumer} and
 * {@link Reader} instances.
 *
 * <p>Example of constructing a client:
 *
 * <pre>{@code
 * PulsarClient client = PulsarClient.builder()
 *                              .serviceUrl("pulsar://broker:6650")
 *                              .build();
 * }</pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PulsarClient extends Closeable {

    /**
     * Get a new builder instance that can used to configure and build a {@link PulsarClient} instance.
     *
     * @return the {@link ClientBuilder}
     *
     * @since 2.0.0
     */
    static ClientBuilder builder() {
        return DefaultImplementation.getDefaultImplementation().newClientBuilder();
    }

    /**
     * Create a producer builder that can be used to configure
     * and construct a producer with default {@link Schema#BYTES}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Producer<byte[]> producer = client.newProducer()
     *                  .topic("my-topic")
     *                  .create();
     * producer.send("test".getBytes());
     * }</pre>
     *
     * @return a {@link ProducerBuilder} object to configure and construct the {@link Producer} instance
     *
     * @since 2.0.0
     */
    ProducerBuilder<byte[]> newProducer();

    /**
     * Create a producer builder that can be used to configure
     * and construct a producer with the specified schema.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Producer<String> producer = client.newProducer(Schema.STRING)
     *                  .topic("my-topic")
     *                  .create();
     * producer.send("test");
     * }</pre>
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
     * Create a consumer builder with no schema ({@link Schema#BYTES}) for subscribing to
     * one or more topics.
     *
     * <pre>{@code
     * Consumer<byte[]> consumer = client.newConsumer()
     *        .topic("my-topic")
     *        .subscriptionName("my-subscription-name")
     *        .subscribe();
     *
     * while (true) {
     *     Message<byte[]> message = consumer.receive();
     *     System.out.println("Got message: " + message.getValue());
     *     consumer.acknowledge(message);
     * }
     * }</pre>
     *
     * @return a {@link ConsumerBuilder} object to configure and construct the {@link Consumer} instance
     *
     * @since 2.0.0
     */
    ConsumerBuilder<byte[]> newConsumer();

    /**
     * Create a consumer builder with a specific schema for subscribing on a specific topic
     *
     * <p>Since 2.2, if you are creating a consumer with non-bytes schema on a non-existence topic, it will
     * automatically create the topic with the provided schema.
     *
     * <pre>{@code
     * Consumer<String> consumer = client.newConsumer(Schema.STRING)
     *        .topic("my-topic")
     *        .subscriptionName("my-subscription-name")
     *        .subscribe();
     *
     * while (true) {
     *     Message<String> message = consumer.receive();
     *     System.out.println("Got message: " + message.getValue());
     *     consumer.acknowledge(message);
     * }
     * }</pre>
     *
     * @param schema
     *          provide a way to convert between serialized data and domain objects
     * @return a {@link ConsumerBuilder} object to configure and construct the {@link Consumer} instance
     *
     * @since 2.0.0
     */
    <T> ConsumerBuilder<T> newConsumer(Schema<T> schema);

    /**
     * Create a topic reader builder with no schema ({@link Schema#BYTES}) to read from the specified topic.
     *
     * <p>The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. A reader needs to be specified a {@link ReaderBuilder#startMessageId(MessageId)}
     * that can either be:
     * <ul>
     * <li>{@link MessageId#earliest}: Start reading from the earliest message available in the topic</li>
     * <li>{@link MessageId#latest}: Start reading from end of the topic. The first message read will be the one
     * published <b>*after*</b> the creation of the builder</li>
     * <li>{@link MessageId}: Position the reader on a particular message. The first message read will be the one
     * immediately <b>*after*</b> the specified message</li>
     * </ul>
     *
     * <p>A Reader can only from non-partitioned topics. In case of partitioned topics, one can create the readers
     * directly on the individual partitions. See {@link #getPartitionsForTopic(String)} for how to get the
     * topic partitions names.
     *
     * <p>Example of usage of Reader:
     * <pre>{@code
     * Reader<byte[]> reader = client.newReader()
     *        .topic("my-topic")
     *        .startMessageId(MessageId.earliest)
     *        .create();
     *
     * while (true) {
     *     Message<byte[]> message = reader.readNext();
     *     System.out.println("Got message: " + message.getValue());
     *     // Reader doesn't need acknowledgments
     * }
     * }</pre>
     *
     * @return a {@link ReaderBuilder} that can be used to configure and construct a {@link Reader} instance
     * @since 2.0.0
     */
    ReaderBuilder<byte[]> newReader();

    /**
     * Create a topic reader builder with a specific {@link Schema}) to read from the specified topic.
     *
     * <p>The Reader provides a low-level abstraction that allows for manual positioning in the topic, without using a
     * subscription. A reader needs to be specified a {@link ReaderBuilder#startMessageId(MessageId)} that can either
     * be:
     * <ul>
     * <li>{@link MessageId#earliest}: Start reading from the earliest message available in the topic</li>
     * <li>{@link MessageId#latest}: Start reading from end of the topic. The first message read will be the one
     * published <b>*after*</b> the creation of the builder</li>
     * <li>{@link MessageId}: Position the reader on a particular message. The first message read will be the one
     * immediately <b>*after*</b> the specified message</li>
     * </ul>
     *
     * <p>A Reader can only from non-partitioned topics. In case of partitioned topics, one can create the readers
     * directly on the individual partitions. See {@link #getPartitionsForTopic(String)} for how to get the
     * topic partitions names.
     *
     * <p>Example of usage of Reader:
     * <pre>
     * {@code
     * Reader<String> reader = client.newReader(Schema.STRING)
     *        .topic("my-topic")
     *        .startMessageId(MessageId.earliest)
     *        .create();
     *
     * while (true) {
     *     Message<String> message = reader.readNext();
     *     System.out.println("Got message: " + message.getValue());
     *     // Reader doesn't need acknowledgments
     * }
     * }</pre>
     *
     * @return a {@link ReaderBuilder} that can be used to configure and construct a {@link Reader} instance
     *
     * @since 2.0.0
     */
    <T> ReaderBuilder<T> newReader(Schema<T> schema);

    /**
     * Create a table view builder with a specific schema for subscribing on a specific topic.
     *
     * <p>The TableView provides a key-value map view of a compacted topic. Messages without keys will
     * be ignored.
     *
     * <p>Example:
     * <pre>{@code
     *  TableView<byte[]> tableView = client.newTableView(Schema.BYTES)
     *            .topic("my-topic")
     *            .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
     *            .create();
     *
     *  tableView.forEach((k, v) -> System.out.println(k + ":" + v));
     * }</pre>
     *
     * @param schema provide a way to convert between serialized data and domain objects
     * @return a {@link TableViewBuilder} object to configure and construct the {@link TableView} instance
     */
    <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema);

    /**
     * Update the service URL this client is using.
     *
     * <p>This will force the client close all existing connections and to restart service discovery to the new service
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
     * <p>If the topic is partitioned, this will return a list of partition names. If the topic is not partitioned, the
     * returned list will contain the topic name itself.
     *
     * <p>This can be used to discover the partitions and create {@link Reader}, {@link Consumer} or {@link Producer}
     * instances directly on a particular partition.
     *
     * @param topic
     *            the topic name
     * @return a future that will yield a list of the topic partitions or {@link PulsarClientException} if there was any
     *         error in the operation.
     * @since 2.3.0
     */
    CompletableFuture<List<String>> getPartitionsForTopic(String topic);

    /**
     * Close the PulsarClient and release all the resources.
     *
     * <p>This operation will trigger a graceful close of all producer, consumer and reader instances that
     * this client has currently active. That implies that close will block and wait until all pending producer
     * send requests are persisted.
     *
     * @throws PulsarClientException
     *             if the close operation fails
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Asynchronously close the PulsarClient and release all the resources.
     *
     * <p>This operation will trigger a graceful close of all producer, consumer and reader instances that
     * this client has currently active. That implies that close and wait, asynchronously, until all pending producer
     * send requests are persisted.
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Perform immediate shutdown of PulsarClient.
     *
     * <p>Release all the resources and close all the producer, consumer and reader instances without waiting
     * for ongoing operations to complete.
     *
     * @throws PulsarClientException
     *             if the forceful shutdown fails
     */
    void shutdown() throws PulsarClientException;

    /**
     * Return internal state of the client. Useful if you want to check that current client is valid.
     * @return true is the client has been closed
     * @see #shutdown()
     * @see #close()
     * @see #closeAsync()
     */
    boolean isClosed();

    /**
     * Create a transaction builder that can be used to configure
     * and construct a transaction.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Transaction txn = client.newTransaction()
     *                         .withTransactionTimeout(1, TimeUnit.MINUTES)
     *                         .build().get();
     * }</pre>
     *
     * @return a {@link TransactionBuilder} object to configure and construct
     * the {@link org.apache.pulsar.client.api.transaction.Transaction} instance
     *
     * @throws PulsarClientException
     *             if transactions are not enabled
     * @since 2.7.0
     */
    TransactionBuilder newTransaction() throws PulsarClientException;
}
