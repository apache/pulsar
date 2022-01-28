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
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Message builder that constructs a message to be published through a producer.
 *
 * <p>Usage example:
 * <pre><code>
 * producer.newMessage().key(myKey).value(myValue).send();
 * </code></pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface TypedMessageBuilder<T> extends Serializable {

    /**
     * Send a message synchronously.
     *
     * <p>This method will block until the message is successfully published and returns the
     * {@link MessageId} assigned by the broker to the published message.
     *
     * <p>Example:
     *
     * <pre>{@code
     * MessageId msgId = producer.newMessage()
     *                  .key(myKey)
     *                  .value(myValue)
     *                  .send();
     * System.out.println("Published message: " + msgId);
     * }</pre>
     *
     * @return the {@link MessageId} assigned by the broker to the published message.
     */
    MessageId send() throws PulsarClientException;

    /**
     * Send a message asynchronously
     *
     * <p>This method returns a future that can be used to track the completion of the send operation and yields the
     * {@link MessageId} assigned by the broker to the published message.
     *
     * <p>Example:
     *
     * <pre>
     * <code>producer.newMessage()
     *                  .value(myValue)
     *                  .sendAsync().thenAccept(messageId -> {
     *    System.out.println("Published message: " + messageId);
     * }).exceptionally(e -> {
     *    System.out.println("Failed to publish " + e);
     *    return null;
     * });</code>
     * </pre>
     *
     * <p>When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     *
     * <p>See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * @return a future that can be used to track when the message will have been safely persisted
     */
    CompletableFuture<MessageId> sendAsync();

    /**
     * Sets the key of the message for routing policy.
     *
     * @param key the partitioning key for the message
     * @return the message builder instance
     */
    TypedMessageBuilder<T> key(String key);

    /**
     * Sets the bytes of the key of the message for routing policy.
     * Internally the bytes will be base64 encoded.
     *
     * @param key routing key for message, in byte array form
     * @return the message builder instance
     */
    TypedMessageBuilder<T> keyBytes(byte[] key);

    /**
     * Sets the ordering key of the message for message dispatch in {@link SubscriptionType#Key_Shared} mode.
     * Partition key Will be used if ordering key not specified.
     *
     * @param orderingKey the ordering key for the message
     * @return the message builder instance
     */
    TypedMessageBuilder<T> orderingKey(byte[] orderingKey);

    /**
     * Set a domain object on the message.
     *
     * @param value
     *            the domain object
     * @return the message builder instance
     */
    TypedMessageBuilder<T> value(T value);

    /**
     * Sets a new property on a message.
     *
     * @param name
     *            the name of the property
     * @param value
     *            the associated value
     * @return the message builder instance
     */
    TypedMessageBuilder<T> property(String name, String value);

    /**
     * Add all the properties in the provided map.
     * @return the message builder instance
     */
    TypedMessageBuilder<T> properties(Map<String, String> properties);

    /**
     * Set the event time for a given message.
     *
     * <p>Applications can retrieve the event time by calling {@link Message#getEventTime()}.
     *
     * <p>Note: currently pulsar doesn't support event-time based index. so the subscribers
     * can't seek the messages by event time.
     * @return the message builder instance
     */
    TypedMessageBuilder<T> eventTime(long timestamp);

    /**
     * Specify a custom sequence id for the message being published.
     *
     * <p>The sequence id can be used for deduplication purposes and it needs to follow these rules:
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
     * @return the message builder instance
     */
    TypedMessageBuilder<T> sequenceId(long sequenceId);

    /**
     * Override the geo-replication clusters for this message.
     *
     * @param clusters the list of clusters.
     * @return the message builder instance
     */
    TypedMessageBuilder<T> replicationClusters(List<String> clusters);

    /**
     * Disable geo-replication for this message.
     *
     * @return the message builder instance
     */
    TypedMessageBuilder<T> disableReplication();

    /**
     * Deliver the message only at or after the specified absolute timestamp.
     *
     * <p>The timestamp is milliseconds and based on UTC (eg: {@link System#currentTimeMillis()}.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param timestamp
     *            absolute timestamp indicating when the message should be delivered to consumers
     * @return the message builder instance
     */
    TypedMessageBuilder<T> deliverAt(long timestamp);

    /**
     * Request to deliver the message only after the specified relative delay.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param delay
     *            the amount of delay before the message will be delivered
     * @param unit
     *            the time unit for the delay
     * @return the message builder instance
     */
    TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit);

    /**
     * Configure the {@link TypedMessageBuilder} from a config map, as an alternative compared
     * to call the individual builder methods.
     *
     * <p>The "value" of the message itself cannot be set on the config map.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Map<String, Object> conf = new HashMap<>();
     * conf.put("key", "my-key");
     * conf.put("eventTime", System.currentTimeMillis());
     *
     * producer.newMessage()
     *             .value("my-message")
     *             .loadConf(conf)
     *             .send();
     * }</pre>
     *
     * <p>The available options are:
     * <table border="1">
     *  <tr>
     *    <th>Constant</th>
     *    <th>Name</th>
     *    <th>Type</th>
     *    <th>Doc</th>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_KEY}</td>
     *    <td>{@code key}</td>
     *    <td>{@code String}</td>
     *    <td>{@link #key(String)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_PROPERTIES}</td>
     *    <td>{@code properties}</td>
     *    <td>{@code Map<String,String>}</td>
     *    <td>{@link #properties(Map)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_EVENT_TIME}</td>
     *    <td>{@code eventTime}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #eventTime(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_SEQUENCE_ID}</td>
     *    <td>{@code sequenceId}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #sequenceId(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_REPLICATION_CLUSTERS}</td>
     *    <td>{@code replicationClusters}</td>
     *    <td>{@code List<String>}</td>
     *    <td>{@link #replicationClusters(List)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_DISABLE_REPLICATION}</td>
     *    <td>{@code disableReplication}</td>
     *    <td>{@code boolean}</td>
     *    <td>{@link #disableReplication()}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_DELIVERY_AFTER_SECONDS}</td>
     *    <td>{@code deliverAfterSeconds}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #deliverAfter(long, TimeUnit)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_DELIVERY_AT}</td>
     *    <td>{@code deliverAt}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #deliverAt(long)}</td>
     *  </tr>
     * </table>
     *
     * @param config a map with the configuration options for the message
     * @return the message builder instance
     */
    TypedMessageBuilder<T> loadConf(Map<String, Object> config);

    String CONF_KEY = "key";
    String CONF_PROPERTIES = "properties";
    String CONF_EVENT_TIME = "eventTime";
    String CONF_SEQUENCE_ID = "sequenceId";
    String CONF_REPLICATION_CLUSTERS = "replicationClusters";
    String CONF_DISABLE_REPLICATION = "disableReplication";
    String CONF_DELIVERY_AFTER_SECONDS = "deliverAfterSeconds";
    String CONF_DELIVERY_AT = "deliverAt";
}
