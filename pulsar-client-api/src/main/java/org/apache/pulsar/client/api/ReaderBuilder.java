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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * {@link ReaderBuilder} is used to configure and create instances of {@link Reader}.
 *
 * @see PulsarClient#newReader()
 *
 * @since 2.0.0
 */
public interface ReaderBuilder<T> extends Cloneable {

    /**
     * Finalize the creation of the {@link Reader} instance.
     *
     * <p>
     * This method will block until the reader is created successfully.
     *
     * @return the reader instance
     * @throws PulsarClientException
     *             if the reader creation fails
     */
    Reader<T> create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link Reader} instance in asynchronous mode.
     *
     * <p>
     * This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
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
     * <pre>
     * Map&lt;String, Object&gt; config = new HashMap&lt;&gt;();
     * config.put("topicName", "test-topic");
     * config.put("receiverQueueSize", 2000);
     *
     * ReaderBuilder&lt;byte[]&gt; builder = ...;
     * builder = builder.loadConf(config);
     *
     * Reader&lt;byte[]&gt; reader = builder.create();
     * </pre>
     *
     * @param config configuration to load
     * @return reader builder instance
     */
    ReaderBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current {@link ReaderBuilder}.
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * ReaderBuilder builder = client.newReader().readerName("my-reader").receiverQueueSize(10);
     *
     * Reader reader1 = builder.clone().topic(TOPIC_1).create();
     * Reader reader2 = builder.clone().topic(TOPIC_2).create();
     * </pre>
     */
    ReaderBuilder<T> clone();

    /**
     * Specify the topic this consumer will subscribe on.
     * <p>
     * This argument is required when constructing the consumer.
     *
     * @param topicName
     */
    ReaderBuilder<T> topic(String topicName);

    /**
     * The initial reader positioning is done by specifying a message id. The options are:
     * <ul>
     * <li><code>MessageId.earliest</code> : Start reading from the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Start reading from the end topic, only getting messages published after the
     * reader was created
     * <li><code>MessageId</code> : When passing a particular message id, the reader will position itself on that
     * specific position. The first message to be read will be the message next to the specified messageId.
     * </ul>
     */
    ReaderBuilder<T> startMessageId(MessageId startMessageId);

    /**
     * Sets a {@link ReaderListener} for the reader
     * <p>
     * When a {@link ReaderListener} is set, application will receive messages through it. Calls to
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     */
    ReaderBuilder<T> readerListener(ReaderListener<T> readerListener);

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            The action to take when the decoding fails
     */
    ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     * </p>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     */
    ReaderBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Set the reader name.
     *
     * @param readerName
     */
    ReaderBuilder<T> readerName(String readerName);

    /**
     * Set the subscription role prefix. The default prefix is "reader".
     *
     * @param subscriptionRolePrefix
     */
    ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix);

    /**
     * If enabled, the reader will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the reader will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * readCompacted can only be enabled when reading from a persistent topic. Attempting to enable it on non-persistent
     * topics will lead to the reader create call throwing a PulsarClientException.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     */
    ReaderBuilder<T> readCompacted(boolean readCompacted);
}
