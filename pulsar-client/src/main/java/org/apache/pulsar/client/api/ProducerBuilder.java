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

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError;

/**
 * {@link ProducerBuilder} is used to configure and create instances of {@link Producer}.
 *
 * @see PulsarClient#newProducer()
 */
public interface ProducerBuilder extends Serializable, Cloneable {

    /**
     * Finalize the creation of the {@link Producer} instance.
     * <p>
     * This method will block until the producer is created successfully.
     *
     * @return the producer instance
     * @throws PulsarClientException.ProducerBusyException
     *             if a producer with the same "producer name" is already connected to the topic
     * @throws PulsarClientException
     *             if the producer creation fails
     */
    Producer create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link Producer} instance in asynchronous mode.
     * <p>
     * This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
     *
     * @return a future that will yield the created producer instance
     * @throws PulsarClientException.ProducerBusyException
     *             if a producer with the same "producer name" is already connected to the topic
     * @throws PulsarClientException
     *             if the producer creation fails
     */
    CompletableFuture<Producer> createAsync();

    /**
     * Create a copy of the current {@link ProducerBuilder}.
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * ProducerBuilder builder = client.newProducer().sendTimeout(10, TimeUnit.SECONDS).blockIfQueueFull(true);
     *
     * Producer producer1 = builder.clone().topic(TOPIC_1).create();
     * Producer producer2 = builder.clone().topic(TOPIC_2).create();
     * </pre>
     */
    ProducerBuilder clone();

    /**
     * Specify the topic this producer will be publishing on.
     * <p>
     * This argument is required when constructing the produce.
     *
     * @param topicName
     */
    ProducerBuilder topic(String topicName);

    /**
     * Specify a name for the producer
     * <p>
     * If not assigned, the system will generate a globally unique name which can be access with
     * {@link Producer#getProducerName()}.
     * <p>
     * When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
     * across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
     * a topic.
     *
     * @param producerName
     *            the custom name to use for the producer
     */
    ProducerBuilder producerName(String producerName);

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * @param sendTimeout
     *            the send timeout
     * @param unit
     *            the time unit of the {@code sendTimeout}
     */
    ProducerBuilder sendTimeout(int sendTimeout, TimeUnit unit);

    /**
     * Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
     * <p>
     * When the queue is full, by default, all calls to {@link Producer#send} and {@link Producer#sendAsync} will fail
     * unless blockIfQueueFull is set to true. Use {@link #setBlockIfQueueFull} to change the blocking behavior.
     *
     * @param maxPendingMessages
     * @return
     */
    ProducerBuilder maxPendingMessages(int maxPendingMessages);

    /**
     * Set the number of max pending messages across all the partitions
     * <p>
     * This setting will be used to lower the max pending messages for each partition
     * ({@link #maxPendingMessages(int)}), if the total exceeds the configured value.
     *
     * @param maxPendingMessagesAcrossPartitions
     */
    ProducerBuilder maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions);

    /**
     * Set whether the {@link Producer#send} and {@link Producer#sendAsync} operations should block when the outgoing
     * message queue is full.
     * <p>
     * Default is <code>false</code>. If set to <code>false</code>, send operations will immediately fail with
     * {@link ProducerQueueIsFullError} when there is no space left in pending queue.
     *
     * @param blockIfQueueFull
     *            whether to block {@link Producer#send} and {@link Producer#sendAsync} operations on queue full
     * @return
     */
    ProducerBuilder blockIfQueueFull(boolean blockIfQueueFull);

    /**
     * Set the message routing mode for the partitioned producer
     *
     * @param mode
     * @return
     */
    ProducerBuilder messageRoutingMode(MessageRoutingMode messageRouteMode);

    /**
     * Set the compression type for the producer.
     * <p>
     * By default, message payloads are not compressed. Supported compression types are:
     * <ul>
     * <li><code>CompressionType.LZ4</code></li>
     * <li><code>CompressionType.ZLIB</code></li>
     * </ul>
     *
     * @param compressionType
     * @return
     */
    ProducerBuilder compressionType(CompressionType compressionType);

    /**
     * Set a custom message routing policy by passing an implementation of MessageRouter
     *
     *
     * @param messageRouter
     */
    ProducerBuilder messageRouter(MessageRouter messageRouter);

    /**
     * Control whether automatic batching of messages is enabled for the producer. <i>default: false [No batching]</i>
     *
     * When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
     * broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
     * messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
     * contents.
     *
     * When enabled default batch delay is set to 10 ms and default batch size is 1000 messages
     *
     * @see #batchingMaxPublishDelay(long, TimeUnit)
     */
    ProducerBuilder enableBatching(boolean enableBatching);

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    ProducerBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Add public encryption key, used by producer to encrypt the data key.
     *
     * At the time of producer creation, Pulsar client checks if there are keys added to encryptionKeys. If keys are
     * found, a callback getKey(String keyName) is invoked against each key to load the values of the key. Application
     * should implement this callback to return the key in pkcs8 format. If compression is enabled, message is encrypted
     * after compression. If batch messaging is enabled, the batched message is encrypted.
     *
     */
    ProducerBuilder addEncryptionKey(String key);

    /**
     * Sets the ProducerCryptoFailureAction to the value specified
     *
     * @param The
     *            producer action
     */
    ProducerBuilder cryptoFailureAction(ProducerCryptoFailureAction action);

    /**
     * Set the time period within which the messages sent will be batched <i>default: 10ms</i> if batch messages are
     * enabled. If set to a non zero value, messages will be queued until this time interval or until
     *
     * @see ProducerConfiguration#batchingMaxMessages threshold is reached; all messages will be published as a single
     *      batch message. The consumer will be delivered individual messages in the batch in the same order they were
     *      enqueued
     * @param batchDelay
     *            the batch delay
     * @param timeUnit
     *            the time unit of the {@code batchDelay}
     * @return
     */
    ProducerBuilder batchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit);

    /**
     * Set the maximum number of messages permitted in a batch. <i>default: 1000</i> If set to a value greater than 1,
     * messages will be queued until this threshold is reached or batch interval has elapsed
     *
     * @see ProducerConfiguration#setBatchingMaxPublishDelay(long, TimeUnit) All messages in batch will be published as
     *      a single batch message. The consumer will be delivered individual messages in the batch in the same order
     *      they were enqueued
     * @param batchMessagesMaxMessagesPerBatch
     *            maximum number of messages in a batch
     * @return
     */
    ProducerBuilder batchingMaxMessages(int batchMessagesMaxMessagesPerBatch);

    /**
     * Set the baseline for the sequence ids for messages published by the producer.
     * <p>
     * First message will be using (initialSequenceId + 1) as its sequence id and subsequent messages will be assigned
     * incremental sequence ids, if not otherwise specified.
     *
     * @param initialSequenceId
     * @return
     */
    ProducerBuilder initialSequenceId(long initialSequenceId);

    /**
     * Set a name/value property with this producer.
     *
     * @param key
     * @param value
     * @return
     */
    ProducerBuilder property(String key, String value);

    /**
     * Add all the properties in the provided map
     *
     * @param properties
     * @return
     */
    ProducerBuilder properties(Map<String, String> properties);
}
