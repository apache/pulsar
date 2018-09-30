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
import java.util.regex.Pattern;

import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;

/**
 * {@link ConsumerBuilder} is used to configure and create instances of {@link Consumer}.
 *
 * @see PulsarClient#newConsumer()
 *
 * @since 2.0.0
 */
public interface ConsumerBuilder<T> extends Cloneable {

    /**
     * Create a copy of the current consumer builder.
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * ConsumerBuilder builder = client.newConsumer() //
     *         .subscriptionName("my-subscription-name") //
     *         .subscriptionType(SubscriptionType.Shared) //
     *         .receiverQueueSize(10);
     *
     * Consumer consumer1 = builder.clone().topic(TOPIC_1).subscribe();
     * Consumer consumer2 = builder.clone().topic(TOPIC_2).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> clone();

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>
     * Map&lt;String, Object&gt; config = new HashMap&lt;&gt;();
     * config.put("ackTimeoutMillis", 1000);
     * config.put("receiverQueueSize", 2000);
     *
     * ConsumerBuilder&lt;byte[]&gt; builder = ...;
     * builder = builder.loadConf(config);
     *
     * Consumer&lt;byte[]&gt; consumer = builder.subscribe();
     * </pre>
     *
     * @param config configuration to load
     * @return consumer builder instance
     */
    ConsumerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Finalize the {@link Consumer} creation by subscribing to the topic.
     *
     * <p>
     * If the subscription does not exist, a new subscription will be created and all messages published after the
     * creation will be retained until acknowledged, even if the consumer is not connected.
     *
     * @return the {@link Consumer} instance
     * @throws PulsarClientException
     *             if the the subscribe operation fails
     */
    Consumer<T> subscribe() throws PulsarClientException;

    /**
     * Finalize the {@link Consumer} creation by subscribing to the topic in asynchronous mode.
     *
     * <p>
     * If the subscription does not exist, a new subscription will be created and all messages published after the
     * creation will be retained until acknowledged, even if the consumer is not connected.
     *
     * @return a future that will yield a {@link Consumer} instance
     * @throws PulsarClientException
     *             if the the subscribe operation fails
     */
    CompletableFuture<Consumer<T>> subscribeAsync();

    /**
     * Specify the topics this consumer will subscribe on.
     * <p>
     *
     * @param topicNames
     */
    ConsumerBuilder<T> topic(String... topicNames);

    /**
     * Specify a list of topics that this consumer will subscribe on.
     * <p>
     *
     * @param topicNames
     */
    ConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     * <p>
     *
     * @param topicsPattern
     */
    ConsumerBuilder<T> topicsPattern(Pattern topicsPattern);

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     * It accepts regular expression and will be compiled into a pattern internally.
     * Eg. "persistent://prop/use/ns-abc/pattern-topic-.*"
     * <p>
     *
     * @param topicsPattern
     *            given regular expression for topics pattern
     */
    ConsumerBuilder<T> topicsPattern(String topicsPattern);

    /**
     * Specify the subscription name for this consumer.
     * <p>
     * This argument is required when constructing the consumer.
     *
     * @param subscriptionName
     */
    ConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Set the timeout for unacked messages, truncated to the nearest millisecond. The timeout needs to be greater than
     * 10 seconds.
     *
     * @param ackTimeout
     *            for unacked messages.
     * @param timeUnit
     *            unit in which the timeout is provided.
     */
    ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit);

    /**
     * Select the subscription type to be used when subscribing to the topic.
     * <p>
     * Default is {@link SubscriptionType#Exclusive}
     *
     * @param subscriptionType
     *            the subscription type value
     */
    ConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);

    /**
     * Sets a {@link MessageListener} for the consumer
     * <p>
     * When a {@link MessageListener} is set, application will receive messages through it. Calls to
     * {@link Consumer#receive()} will not be allowed.
     *
     * @param messageListener
     *            the listener object
     */
    ConsumerBuilder<T> messageListener(MessageListener<T> messageListener);

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    ConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            The consumer action
     */
    ConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     * </p>
     * <p>
     * <b>Setting the consumer queue size as zero</b>
     * <ul>
     * <li>Decreases the throughput of the consumer, by disabling pre-fetching of messages. This approach improves the
     * message distribution on shared subscription, by pushing messages only to the consumers that are ready to process
     * them. Neither {@link Consumer#receive(int, TimeUnit)} nor Partitioned Topics can be used if the consumer queue
     * size is zero. {@link Consumer#receive()} function call should not be interrupted when the consumer queue size is
     * zero.</li>
     * <li>Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer connection with
     * broker and {@link Consumer#receive()} call will remain blocked while {@link Consumer#receiveAsync()} receives
     * exception in callback. <b> consumer will not be able receive any further message unless batch-message in pipeline
     * is removed</b></li>
     * </ul>
     * </p>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     */
    ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Group the consumer acknowledgments for the specified time.
     * <p>
     * By default, the consumer will use a 100 ms grouping time to send out the acknowledgments to the broker.
     * <p>
     * Setting a group time of 0, will send out the acknowledgments immediately.
     *
     * @param delay
     *            the max amount of time an acknowledgemnt can be delayed
     * @param unit
     *            the time unit for the delay
     */
    ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);

    /**
     * Set the max total receiver queue size across partitons.
     * <p>
     * This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000).
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     */
    ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * Set the consumer name.
     *
     * @param consumerName
     */
    ConsumerBuilder<T> consumerName(String consumerName);

    /**
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>
     * The consumer group listener is used for receiving consumer state change in a consumer group for failover
     * subscription. Application can then react to the consumer state changes.
     *
     * @param consumerEventListener
     *            the consumer group listener object
     */
    ConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);

    /**
     * If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * readCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer (i.e.
     * failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics or on a
     * shared subscription, will lead to the subscription call throwing a PulsarClientException.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     */
    ConsumerBuilder<T> readCompacted(boolean readCompacted);

    /**
     * Set topics auto discovery period when using a pattern for topics consumer.
     * The period is in minute, and default and minimum value is 1 minute.
     *
     * @param periodInMinutes
     *            number of minutes between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     */
    ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes);

    /**
     * Sets priority level for the shared subscription consumers to which broker gives more priority while dispatching
     * messages. Here, broker follows descending priorities. (eg: 0=max-priority, 1, 2,..) </br>
     * In Shared subscription mode, broker will first dispatch messages to max priority-level consumers if they have
     * permits, else broker will consider next priority level consumers. </br>
     * If subscription has consumer-A with priorityLevel 0 and Consumer-B with priorityLevel 1 then broker will dispatch
     * messages to only consumer-A until it runs out permit and then broker starts dispatching messages to Consumer-B.
     *
     * <pre>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Order in which broker dispatches messages to consumers: C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * @param priorityLevel
     */
    ConsumerBuilder<T> priorityLevel(int priorityLevel);

    /**
     * Set a name/value property with this consumer.
     *
     * @param key
     * @param value
     */
    ConsumerBuilder<T> property(String key, String value);

    /**
     * Add all the properties in the provided map
     *
     * @param properties
     */
    ConsumerBuilder<T> properties(Map<String, String> properties);

    /**
     * Set subscriptionInitialPosition for the consumer
    */
    ConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);

    /**
     * Determines to which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both.
     * Only used with pattern subscriptions.
     *
     * @param mode Pattern subscription mode
     */
    ConsumerBuilder<T> subscriptionTopicsMode(Mode mode);

    /**
     * Intercept {@link Consumer}.
     *
     * @param interceptors the list of interceptors to intercept the consumer created by this builder.
     */
    ConsumerBuilder<T> intercept(ConsumerInterceptor<T> ...interceptors);

    /**
     * Set dead letter policy for consumer
     *
     * By default some message will redelivery so many times possible, even to the extent that it can be never stop.
     * By using dead letter mechanism messages will has the max redelivery count, when message exceeding the maximum
     * number of redeliveries, message will send to the Dead Letter Topic and acknowledged automatic.
     *
     * You can enable the dead letter mechanism by setting dead letter policy.
     * example:
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
     *          .subscribe();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
     * To setting a custom dead letter topic name
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).deadLetterTopic("your-topic-name").build())
     *          .subscribe();
     * </pre>
     */
    ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);
}
