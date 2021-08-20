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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ReaderBuilder} is used to configure and create instances of {@link Reader}.
 *
 * @see PulsarClient#newReader()
 *
 * @since 2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ReaderBuilder<T> extends Cloneable {

    /**
     * Finalize the creation of the {@link Reader} instance.
     *
     * <p>This method will block until the reader is created successfully or an exception is thrown.
     *
     * @return the reader instance
     * @throws PulsarClientException
     *             if the reader creation fails
     */
    Reader<T> create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link Reader} instance in asynchronous mode.
     *
     * <p>This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
     *
     * @return the reader instance
     * @throws PulsarClientException
     *             if the reader creation fails
     */
    CompletableFuture<Reader<T>> createAsync();

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("topicName", "test-topic");
     * config.put("receiverQueueSize", 2000);
     *
     * ReaderBuilder<byte[]> builder = ...;
     * builder = builder.loadConf(config);
     *
     * Reader<byte[]> reader = builder.create();
     * }</pre>
     *
     * @param config
     *            configuration to load
     * @return the reader builder instance
     */
    ReaderBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current {@link ReaderBuilder}.
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>{@code
     * ReaderBuilder<String> builder = client.newReader(Schema.STRING)
     *             .readerName("my-reader")
     *             .receiverQueueSize(10);
     *
     * Reader<String> reader1 = builder.clone().topic("topic-1").create();
     * Reader<String> reader2 = builder.clone().topic("topic-2").create();
     * }</pre>
     *
     * @return a clone of the reader builder instance
     */
    ReaderBuilder<T> clone();

    /**
     * Specify the topic this reader will read from.
     *
     * <p>This argument is required when constructing the reader.
     *
     * @param topicName
     *            the name of the topic
     * @return the reader builder instance
     */
    ReaderBuilder<T> topic(String topicName);

    /**
     * Specify topics this reader will read from.
     * @param topicNames
     * @return
     */
    ReaderBuilder<T> topics(List<String> topicNames);

    /**
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li>{@link MessageId#earliest}: Start reading from the earliest message available in the topic</li>
     * <li>{@link MessageId#latest}: Start reading from end of the topic. The first message read will be the one
     * published <b>*after*</b> the creation of the builder</li>
     * <li>{@link MessageId}: Position the reader on a particular message. The first message read will be the one
     * immediately <b>*after*</b> the specified message</li>
     * </ul>
     *
     * <p>If the first message <b>*after*</b> the specified message is not the desired behaviour, use
     * {@link ReaderBuilder#startMessageIdInclusive()}.
     *
     * @param startMessageId the message id where the reader will be initially positioned on
     * @return the reader builder instance
     */
    ReaderBuilder<T> startMessageId(MessageId startMessageId);

    /**
     * The initial reader positioning can be set at specific timestamp by providing total rollback duration. so, broker
     * can find a latest message that was published before given duration. <br/>
     * eg: rollbackDuration in minute = 5 suggests broker to find message which was published 5 mins back and set the
     * inital position on that messageId.
     *
     * @param rollbackDuration
     *            duration which position should be rolled back.
     * @return
     */
    ReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit);

    /**
     * Set the reader to include the given position of {@link ReaderBuilder#startMessageId(MessageId)}
     *
     * <p>This configuration option also applies for any cursor reset operation like {@link Reader#seek(MessageId)}.
     *
     * @return the reader builder instance
     */
    ReaderBuilder<T> startMessageIdInclusive();

    /**
     * Sets a {@link ReaderListener} for the reader.
     *
     * <p>When a {@link ReaderListener} is set, application will receive messages through it. Calls to
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     * @return the reader builder instance
     */
    ReaderBuilder<T> readerListener(ReaderListener<T> readerListener);

    /**
     * Sets a {@link CryptoKeyReader} to decrypt the message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the reader builder instance
     */
    ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKey
     *            the private key that is always used to decrypt message payloads.
     * @return the reader builder instance
     * @since 2.8.0
     */
    ReaderBuilder<T> defaultCryptoKeyReader(String privateKey);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKeys
     *            the map of private key names and their URIs used to decrypt message payloads.
     * @return the reader builder instance
     * @since 2.8.0
     */
    ReaderBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys);

    /**
     * Sets the {@link ConsumerCryptoFailureAction} to specify.
     *
     * @param action
     *            The action to take when the decoding fails
     * @return the reader builder instance
     */
    ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
     *
     * <p>The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p>Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     * @return the reader builder instance
     */
    ReaderBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Specify a reader name.
     *
     * <p>The reader name is purely informational and can used to track a particular reader in the reported stats.
     * By default a randomly generated name is used.
     *
     * @param readerName
     *            the name to use for the reader
     * @return the reader builder instance
     */
    ReaderBuilder<T> readerName(String readerName);

    /**
     * Set the subscription role prefix. The default prefix is "reader".
     *
     * @param subscriptionRolePrefix
     * @return the reader builder instance
     */
    ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix);

    /**
     * Set the subscription name.
     * <p>If subscriptionRolePrefix is set at the same time, this configuration will prevail
     *
     * @param subscriptionName
     * @return the reader builder instance
     */
    ReaderBuilder<T> subscriptionName(String subscriptionName);

    /**
     * If enabled, the reader will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the reader will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * <p>readCompacted can only be enabled when reading from a persistent topic. Attempting to enable it
     * on non-persistent topics will lead to the reader create call throwing a {@link PulsarClientException}.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     * @return the reader builder instance
     */
    ReaderBuilder<T> readCompacted(boolean readCompacted);

    /**
     * Set key hash range of the reader, broker will only dispatch messages which hash of the message key contains by
     * the specified key hash range. Multiple key hash ranges can be specified on a reader.
     *
     * <p>Total hash range size is 65536, so the max end of the range should be less than or equal to 65535.
     *
     * @param ranges
     *            key hash ranges for a reader
     * @return the reader builder instance
     */
    ReaderBuilder<T> keyHashRange(Range... ranges);

    /**
     * Enable pooling of messages and the underlying data buffers.
     * <p/>
     * When pooling is enabled, the application is responsible for calling Message.release() after the handling of every
     * received message. If “release()” is not called on a received message, there will be a memory leak. If an
     * application attempts to use and already “released” message, it might experience undefined behavior (for example, memory
     * corruption, deserialization error, etc.).
     */
    ReaderBuilder<T> poolMessages(boolean poolMessages);
}
