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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Message builder that constructs a message to be published through a producer.
 *
 * Usage example:
 *
 * <pre><code>
 * producer.newMessage().key(myKey).value(myValue).send();
 * </code></pre>
 *
 */
public interface TypedMessageBuilder<T> extends Serializable {

    /**
     * Send a message synchronously.
     * <p>
     * This method will block until the message is successfully published and returns the
     * {@link MessageId} assigned by the broker to the published message.
     * <p>
     * Example:
     *
     * <pre>
     * <code>MessageId msgId = producer.newMessage().key(myKey).value(myValue).send();
     * System.out.println("Published message: " + msgId);
     * </code>
     * </pre>
     *
     * @return the {@link MessageId} assigned by the broker to the published message.
     */
    MessageId send() throws PulsarClientException;

    /**
     * Send a message asynchronously
     * <p>
     * This method returns a future that can be used to track the completion of the send operation and yields the
     * {@link MessageId} assigned by the broker to the published message.
     * <p>
     * Example:
     *
     * <pre>
     * <code>producer.newMessage().value(myValue).sendAsync().thenAccept(messageId -> {
     *    System.out.println("Published message: " + messageId);
     * }).exceptionally(e -> {
     *    System.out.println("Failed to publish " + e);
     *    return null;
     * });</code>
     * </pre>
     * <p>
     * When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     * <p>
     * See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * @param message
     *            a message
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync();

    /**
     * Sets the key of the message for routing policy
     *
     * @param key
     */
    TypedMessageBuilder<T> key(String key);

    /**
     * Sets the bytes of the key of the message for routing policy.
     * Internally the bytes will be base64 encoded.
     *
     * @param key routing key for message, in byte array form
     */
    TypedMessageBuilder<T> keyBytes(byte[] key);

    /**
     * Set a domain object on the message
     *
     * @param value
     *            the domain object
     */
    TypedMessageBuilder<T> value(T value);

    /**
     * Sets a new property on a message.
     *
     * @param name
     *            the name of the property
     * @param value
     *            the associated value
     */
    TypedMessageBuilder<T> property(String name, String value);

    /**
     * Add all the properties in the provided map
     */
    TypedMessageBuilder<T> properties(Map<String, String> properties);

    /**
     * Set the event time for a given message.
     *
     * <p>
     * Applications can retrieve the event time by calling {@link Message#getEventTime()}.
     *
     * <p>
     * Note: currently pulsar doesn't support event-time based index. so the subscribers can't seek the messages by
     * event time.
     */
    TypedMessageBuilder<T> eventTime(long timestamp);

    /**
     * Specify a custom sequence id for the message being published.
     * <p>
     * The sequence id can be used for deduplication purposes and it needs to follow these rules:
     * <ol>
     * <li><code>sequenceId >= 0</code>
     * <li>Sequence id for a message needs to be greater than sequence id for earlier messages:
     * <code>sequenceId(N+1) > sequenceId(N)</code>
     * <li>It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
     * <code>sequenceId</code> could represent an offset or a cumulative size.
     * </ol>
     *
     * @param sequenceId
     *            the sequence id to assign to the current message
     */
    TypedMessageBuilder<T> sequenceId(long sequenceId);

    /**
     * Override the replication clusters for this message.
     *
     * @param clusters
     */
    TypedMessageBuilder<T> replicationClusters(List<String> clusters);

    /**
     * Disable replication for this message.
     */
    TypedMessageBuilder<T> disableReplication();
}
