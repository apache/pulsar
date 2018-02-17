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

/**
 * {@link ConsumerBuilder} is used to configure and create instances of {@link Consumer}.
 *
 * @see PulsarClient#newConsumer()
 *
 * @since 2.0.0
 */
public interface ConsumerBuilder extends Serializable, Cloneable {

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
    ConsumerBuilder clone();

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
    Consumer subscribe() throws PulsarClientException;

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
    CompletableFuture<Consumer> subscribeAsync();

    /**
     * Specify the topic this consumer will subscribe on.
     * <p>
     * This argument is required when constructing the consumer.
     *
     * @param topicName
     */
    ConsumerBuilder topic(String topicName);

    /**
     * Specify the subscription name for this consumer.
     * <p>
     * This argument is required when constructing the consumer.
     *
     * @param subscriptionName
     */
    ConsumerBuilder subscriptionName(String subscriptionName);

    /**
     * Set the timeout for unacked messages, truncated to the nearest millisecond. The timeout needs to be greater than
     * 10 seconds.
     *
     * @param ackTimeout
     *            for unacked messages.
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return {@link ConsumerConfiguration}
     */
    ConsumerBuilder ackTimeout(long ackTimeout, TimeUnit timeUnit);

    /**
     * Select the subscription type to be used when subscribing to the topic.
     * <p>
     * Default is {@link SubscriptionType#Exclusive}
     *
     * @param subscriptionType
     *            the subscription type value
     */
    ConsumerBuilder subscriptionType(SubscriptionType subscriptionType);

    /**
     * Sets a {@link MessageListener} for the consumer
     * <p>
     * When a {@link MessageListener} is set, application will receive messages through it. Calls to
     * {@link Consumer#receive()} will not be allowed.
     *
     * @param messageListener
     *            the listener object
     */
    ConsumerBuilder messageListener(MessageListener messageListener);

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    ConsumerBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            The consumer action
     */
    ConsumerBuilder cryptoFailureAction(ConsumerCryptoFailureAction action);

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
    ConsumerBuilder receiverQueueSize(int receiverQueueSize);

    /**
     * Set the max total receiver queue size across partitons.
     * <p>
     * This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000).
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     */
    ConsumerBuilder maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * Set the consumer name.
     *
     * @param consumerName
     */
    ConsumerBuilder consumerName(String consumerName);

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
    ConsumerBuilder readCompacted(boolean readCompacted);

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
    ConsumerBuilder priorityLevel(int priorityLevel);

    /**
     * Set a name/value property with this consumer.
     *
     * @param key
     * @param value
     * @return
     */
    ConsumerBuilder property(String key, String value);

    /**
     * Add all the properties in the provided map
     *
     * @param properties
     * @return
     */
    ConsumerBuilder properties(Map<String, String> properties);

}
