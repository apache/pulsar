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
import java.util.concurrent.CompletableFuture;

/**
 * {@link ReaderBuilder} is used to configure and create instances of {@link Reader}.
 *
 * @see PulsarClient#newReader()
 *
 * @since 2.0.0
 */
public interface ReaderBuilder extends Serializable, Cloneable {

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
    Reader create() throws PulsarClientException;

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
    CompletableFuture<Reader> createAsync();

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
    ReaderBuilder clone();

    /**
     * Specify the topic this consumer will subscribe on.
     * <p>
     * This argument is required when constructing the consumer.
     *
     * @param topicName
     */
    ReaderBuilder topic(String topicName);

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
    ReaderBuilder startMessageId(MessageId startMessageId);

    /**
     * Sets a {@link ReaderListener} for the reader
     * <p>
     * When a {@link ReaderListener} is set, application will receive messages through it. Calls to
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     */
    ReaderBuilder readerListener(ReaderListener readerListener);

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    ReaderBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            The action to take when the decoding fails
     */
    ReaderBuilder cryptoFailureAction(ConsumerCryptoFailureAction action);

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
    ReaderBuilder receiverQueueSize(int receiverQueueSize);

    /**
     * Set the reader name.
     *
     * @param readerName
     */
    ReaderBuilder readerName(String readerName);
}
