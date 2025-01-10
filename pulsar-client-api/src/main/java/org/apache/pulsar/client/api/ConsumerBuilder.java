/*
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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ConsumerBuilder} is used to configure and create instances of {@link Consumer}.
 *
 * @see PulsarClient#newConsumer()
 *
 * @since 2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ConsumerBuilder<T> extends Cloneable {

    /**
     * Create a copy of the current consumer builder.
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     * <pre>{@code
     * ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING)
     *         .subscriptionName("my-subscription-name")
     *         .subscriptionType(SubscriptionType.Shared)
     *         .receiverQueueSize(10);
     *
     * Consumer<String> consumer1 = builder.clone().topic("my-topic-1").subscribe();
     * Consumer<String> consumer2 = builder.clone().topic("my-topic-2").subscribe();
     * }</pre>
     *
     * @return a cloned consumer builder object
     */
    ConsumerBuilder<T> clone();

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("ackTimeoutMillis", 1000);
     * config.put("receiverQueueSize", 2000);
     *
     * Consumer<byte[]> builder = client.newConsumer()
     *              .loadConf(config)
     *              .subscribe();
     *
     * Consumer<byte[]> consumer = builder.subscribe();
     * }</pre>
     *
     * @param config configuration to load
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Finalize the {@link Consumer} creation by subscribing to the topic.
     *
     * <p>If the subscription does not exist, a new subscription is created. By default, the subscription
     * is created at the end of the topic. See {@link #subscriptionInitialPosition(SubscriptionInitialPosition)}
     * to configure the initial position behavior.
     *
     * <p>Once a subscription is created, it retains the data and the subscription cursor even if the consumer
     * is not connected.
     *
     * @return the consumer builder instance
     * @throws PulsarClientException
     *             if the subscribe operation fails
     */
    Consumer<T> subscribe() throws PulsarClientException;

    /**
     * Finalize the {@link Consumer} creation by subscribing to the topic in asynchronous mode.
     *
     * <p>If the subscription does not exist, a new subscription is created. By default, the subscription
     * is created at the end of the topic. See {@link #subscriptionInitialPosition(SubscriptionInitialPosition)}
     * to configure the initial position behavior.
     *
     * <p>Once a subscription is created, it retains the data and the subscription cursor even
     * if the consumer is not connected.
     *
     * @return a future that yields a {@link Consumer} instance
     * @throws PulsarClientException
     *             if the subscribe operation fails
     */
    CompletableFuture<Consumer<T>> subscribeAsync();

    /**
     * Specify the topics this consumer subscribes to.
     *
     * @param topicNames a set of topics that the consumer subscribes to
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topic(String... topicNames);

    /**
     * Specify a list of topics that this consumer subscribes to.
     *
     * @param topicNames a list of topics that the consumer subscribes to
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * Specify a pattern for topics(not contains the partition suffix) that this consumer subscribes to.
     *
     * <p>The pattern is applied to subscribe to all topics, within a single namespace, that match the
     * pattern.
     *
     * <p>The consumer automatically subscribes to topics created after itself.
     *
     * @param topicsPattern
     *            a regular expression to select a list of topics(not contains the partition suffix) to subscribe to
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topicsPattern(Pattern topicsPattern);

    /**
     * Specify a pattern for topics(not contains the partition suffix) that this consumer subscribes to.
     *
     * <p>It accepts a regular expression that is compiled into a pattern internally. E.g.,
     * "persistent://public/default/pattern-topic-.*"
     *
     * <p>The pattern is applied to subscribe to all topics, within a single namespace, that match the
     * pattern.
     *
     * <p>The consumer automatically subscribes to topics created after itself.
     *
     * @param topicsPattern
     *            given regular expression for topics(not contains the partition suffix) pattern
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topicsPattern(String topicsPattern);

    /**
     * Specify the subscription name for this consumer.
     *
     * <p>This argument is required when constructing the consumer.
     *
     * @param subscriptionName the name of the subscription that this consumer should attach to
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Specify the subscription properties for this subscription.
     * Properties are immutable, and consumers under the same subscription will fail to create a subscription
     * if they use different properties.
     * @param subscriptionProperties the properties of the subscription
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionProperties(Map<String, String> subscriptionProperties);


    /**
     * Sets the timeout for unacknowledged messages, truncated to the nearest millisecond. The timeout must be
     * greater than 1 second.
     *
     * <p>By default, the acknowledgment timeout is disabled (set to `0`, which means infinite).
     * When a consumer with an infinite acknowledgment timeout terminates, any unacknowledged
     * messages that it receives are re-delivered to another consumer.
     * <p>Since 2.3.0, when a dead letter policy is specified and no ackTimeoutMillis is specified,
     * the acknowledgment timeout is set to 30 seconds.
     *
     * <p>When enabling acknowledgment timeout, if a message is not acknowledged within the specified timeout,
     * it is re-delivered to the consumer (possibly to a different consumer, in the case of
     * a shared subscription).
     *
     * @param ackTimeout
     *            for unacked messages.
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit);

    /**
     * Enables or disables the acknowledgment receipt feature.
     *
     * <p>When this feature is enabled, the consumer ensures that acknowledgments are processed by the broker by
     * waiting for a receipt from the broker. Even when the broker returns a receipt, it doesn't guarantee that the
     * message won't be redelivered later due to certain implementation details.
     * It is recommended to use the asynchronous {@link Consumer#acknowledgeAsync(Message)} method for acknowledgment
     * when this feature is enabled. This is because using the synchronous {@link Consumer#acknowledge(Message)} method
     * with acknowledgment receipt can cause performance issues due to the round trip to the server, which prevents
     * pipelining (having multiple messages in-flight). With the asynchronous method, the consumer can continue
     * consuming other messages while waiting for the acknowledgment receipts.
     *
     * @param isAckReceiptEnabled {@code true} to enable acknowledgment receipt, {@code false} to disable it
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> isAckReceiptEnabled(boolean isAckReceiptEnabled);
    /**
     * Define the granularity of the ack-timeout redelivery.
     *
     * <p>By default, the tick time is set to 1 second. Using a higher tick time
     * reduces the memory overhead to track messages when the ack-timeout is set to
     * bigger values (e.g., 1 hour).
     *
     * @param tickTime
     *            the min precision for the acknowledgment timeout messages tracker
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit);

    /**
     * Sets the delay to wait before re-delivering messages that have failed to be processed.
     *
     * <p>When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message
     * is redelivered after a fixed timeout. The default is 1 min.
     *
     * @param redeliveryDelay
     *            redelivery delay for failed messages
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     * @see Consumer#negativeAcknowledge(Message)
     */
    ConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit);

    /**
     * Select the subscription type to be used when subscribing to a topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionType#Exclusive} (Default)</li>
     *  <li>{@link SubscriptionType#Failover}</li>
     *  <li>{@link SubscriptionType#Shared}</li>
     * </ul>
     *
     * @param subscriptionType
     *            the subscription type value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);

    /**
     * Selects the subscription mode to be used when subscribing to a topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionMode#Durable} (Default)</li>
     *  <li>{@link SubscriptionMode#NonDurable}</li>
     * </ul>
     *
     * @param subscriptionMode
     *            the subscription mode value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode);

    /**
     * Sets a {@link MessageListener} for the consumer.
     *
     * <p>The application receives messages through the message listener,
     * and calls to {@link Consumer#receive()} are not allowed.
     *
     * @param messageListener
     *            the listener object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> messageListener(MessageListener<T> messageListener);

    /**
     * Set the {@link MessageListenerExecutor} to be used for message listeners of <b>current consumer</b>.
     * <i>(default: use executor from PulsarClient,
     * {@link org.apache.pulsar.client.impl.PulsarClientImpl#externalExecutorProvider})</i>.
     *
     * <p>The listener thread pool is exclusively owned by current consumer
     * that are using a "listener" model to get messages. For a given internal consumer,
     * the listener will always be invoked from the same thread, to ensure ordering.
     *
     * <p> The caller need to shut down the thread pool after closing the consumer to avoid leaks.
     * @param messageListenerExecutor the executor of the consumer message listener
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> messageListenerExecutor(MessageListenerExecutor messageListenerExecutor);

    /**
     * Sets a {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param privateKey
     *            the private key that is always used to decrypt message payloads.
     * @return the consumer builder instance
     * @since 2.8.0
     */
    ConsumerBuilder<T> defaultCryptoKeyReader(String privateKey);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKeys
     *            the map of private key names and their URIs used to decrypt message payloads.
     * @return the consumer builder instance
     * @since 2.8.0
     */
    ConsumerBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys);

    /**
     * Sets a {@link MessageCrypto}.
     *
     * <p>Contains methods to encrypt/decrypt messages for end-to-end encryption.
     *
     * @param messageCrypto
     *            MessageCrypto object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action the consumer takes in case of decryption failures
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * Sets the size of the consumer receive queue.
     *
     * <p>The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value can potentially increase consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p><b>Setting the consumer queue size as zero</b>
     * <ul>
     * <li>Decreases the throughput of the consumer by disabling pre-fetching of messages. This approach improves the
     * message distribution on shared subscriptions by pushing messages only to the consumers that are ready to process
     * them. Neither {@link Consumer#receive(int, TimeUnit)} nor Partitioned Topics can be used if the consumer queue
     * size is zero. {@link Consumer#receive()} function call should not be interrupted when the consumer queue size is
     * zero.</li>
     * <li>Doesn't support Batch-Message. If a consumer receives a batch-message, it closes the consumer connection with
     * the broker and {@link Consumer#receive()} calls remain blocked while {@link Consumer#receiveAsync()} receives
     * exception in callback.
     *
     * <b> The consumer is not able to receive any further messages unless batch-message in pipeline
     * is removed.</b></li>
     * </ul>
     * The default value is {@code 1000} messages and should be adequate for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * Sets amount of time for group consumer acknowledgments.
     *
     * <p>By default, the consumer uses a 100 ms grouping time to send out acknowledgments to the broker.
     *
     * <p>Setting a group time of 0 sends out acknowledgments immediately. A longer acknowledgment group time
     * is more efficient, but at the expense of a slight increase in message re-deliveries after a failure.
     *
     * @param delay
     *            the max amount of time an acknowledgement can be delayed
     * @param unit
     *            the time unit for the delay
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);

    /**
     * Set the number of messages for group consumer acknowledgments.
     *
     * <p>By default, the consumer uses at most 1000 messages to send out acknowledgments to the broker.
     *
     * @param messageNum
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> maxAcknowledgmentGroupSize(int messageNum);

    /**
     * Configures initial replicated subscription state for a new subscription.
     * This setting does not affect existing subscription. Default is `null`.
     *
     * @param replicateSubscriptionState If true, the subscription state will be replicated
     *                                   across GEO-replicated clusters. If false, replication
     *                                   is disabled.
     */
    ConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState);

    /**
     * Sets the max total receiver queue size across partitions.
     *
     * <p>This setting is used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000).
     * The purpose of this setting is to have an upper-limit on the number
     * of messages that a consumer can be pushed at once from a broker, across all
     * the partitions.
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     *            max pending messages across all the partitions
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * Sets the consumer name.
     *
     * <p>Consumer names are informative, and can be used to identify a particular consumer
     * instance from the topic stats.
     *
     * @param consumerName
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> consumerName(String consumerName);

    /**
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>The consumer group listener is used for receiving consumer state changes in a consumer group for failover
     * subscriptions. The application can then react to the consumer state changes.
     *
     * @param consumerEventListener
     *            the consumer group listener object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);

    /**
     * If enabled, the consumer reads messages from the compacted topic rather than the full message topic backlog.
     * This means that, if the topic has been compacted, the consumer will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages are sent as normal.
     *
     * <p>readCompacted can only be enabled on subscriptions to persistent topics with a single active consumer
     * (i.e. failover or exclusive subscriptions). Enabling readCompacted on subscriptions to non-persistent
     * topics or on shared subscriptions will cause the subscription call to throw a PulsarClientException.
     *
     * @param readCompacted
     *            whether to read from the compacted topic or full message topic backlog
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> readCompacted(boolean readCompacted);

    /**
     * Sets topic's auto-discovery period when using a pattern for topics consumer.
     * The period is in minutes, and the default and minimum values are 1 minute.
     *
     * @param periodInMinutes
     *            number of minutes between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes);


    /**
     * Sets topic's auto-discovery period when using a pattern for topics consumer.
     *
     * @param interval
     *            the amount of delay between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @param unit
     *            the unit of the topics auto discovery period
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit);


    /**
     * <b>Shared subscription</b>
     * <p>Sets priority level for shared subscription consumers to determine which consumers the broker prioritizes when
     * dispatching messages. Here, the broker follows descending priorities. (eg: 0=max-priority, 1, 2,..)
     *
     * <p>In Shared subscription mode, the broker first dispatches messages to max priority-level
     * consumers if they have permits, otherwise the broker considers next priority level consumers.
     *
     * <p>If a subscription has consumer-A with priorityLevel 0 and Consumer-B with priorityLevel 1,
     * then the broker dispatches messages to only consumer-A until it is drained, and then the broker will
     * start dispatching messages to Consumer-B.
     *
     * <p><pre>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Order in which broker dispatches messages to consumers: C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <p><b>Failover subscription</b>
     * The broker selects the active consumer for a failover subscription based on consumer's priority-level and
     * lexicographical sorting of consumer name.
     * eg:
     * <pre>
     * 1. Active consumer = C1 : Same priority-level and lexicographical sorting
     * Consumer PriorityLevel Name
     * C1       0             aaa
     * C2       0             bbb
     *
     * 2. Active consumer = C2 : Consumer with highest priority
     * Consumer PriorityLevel Name
     * C1       1             aaa
     * C2       0             bbb
     *
     * Partitioned-topics:
     * Broker evenly assigns partitioned topics to highest priority consumers.
     * </pre>
     *
     * @param priorityLevel the priority of this consumer
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> priorityLevel(int priorityLevel);

    /**
     * Sets a name/value property with this consumer.
     *
     * <p>Properties are application-defined metadata that can be attached to the consumer.
     * When getting topic stats, this metadata is associated with the consumer stats for easier identification.
     *
     * @param key
     *            the property key
     * @param value
     *            the property value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> property(String key, String value);

    /**
     * Add all the properties in the provided map to the consumer.
     *
     * <p>Properties are application-defined metadata that can be attached to the consumer.
     * When getting topic stats, this metadata is associated with the consumer stats for easier identification.
     *
     * @param properties the map with properties
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> properties(Map<String, String> properties);

    /**
     * Sets the {@link SubscriptionInitialPosition} for the consumer.
     *
     * @param subscriptionInitialPosition
     *            the position where to initialize a newly created subscription
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);

    /**
     * Determines which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. Only used
     * with pattern subscriptions.
     *
     * @param regexSubscriptionMode
     *            Pattern subscription mode
     */
    ConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode);

    /**
     * Intercept {@link Consumer}.
     *
     * @param interceptors the list of interceptors to intercept the consumer created by this builder.
     */
    ConsumerBuilder<T> intercept(ConsumerInterceptor<T> ...interceptors);

    /**
     * Sets dead letter policy for a consumer.
     *
     * <p>By default, messages are redelivered as many times as possible until they are acknowledged.
     * If you enable a dead letter mechanism, messages will have a maxRedeliverCount. When a message exceeds the maximum
     * number of redeliveries, the message is sent to the Dead Letter Topic and acknowledged automatically.
     *
     * <p>Enable the dead letter mechanism by setting dead letter policy.
     * example:
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
     *          .subscribe();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
     * To set a custom dead letter topic name:
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy
     *              .builder()
     *              .maxRedeliverCount(10)
     *              .deadLetterTopic("your-topic-name")
     *              .build())
     *          .subscribe();
     * </pre>
     */
    ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);

    /**
     * If enabled, the consumer auto-subscribes for partition increases.
     * This is only for partitioned consumers.
     *
     * @param autoUpdate
     *            whether to auto-update partition increases
     */
    ConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate);

    /**
     * Sets the interval of updating partitions <i>(default: 1 minute)</i>. This only works if autoUpdatePartitions is
     * enabled.
     *
     * @param interval
     *            the interval of updating partitions
     * @param unit
     *            the time unit of the interval.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);

    /**
     * Sets KeyShared subscription policy for consumer.
     *
     * <p>By default, KeyShared subscriptions use auto split hash ranges to maintain consumers. If you want to
     * set a different KeyShared policy, set a policy by using one of the following examples:
     *
     * <p><b>Sticky hash range policy</b></p>
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 10)))
     *          .subscribe();
     * </pre>
     * For details about sticky hash range policies, see {@link KeySharedPolicy.KeySharedPolicySticky}.
     *
     * <p><b>Auto-split hash range policy</b></p>
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
     *          .subscribe();
     * </pre>
     * For details about auto-split hash range policies, see {@link KeySharedPolicy.KeySharedPolicyAutoSplit}.
     *
     * @param keySharedPolicy The {@link KeySharedPolicy} to specify
     */
    ConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy);

    /**
     * Sets the consumer to include the given position of any reset operation like {@link Consumer#seek(long)} or
     * {@link Consumer#seek(MessageId)}}.
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> startMessageIdInclusive();

    /**
     * Sets {@link BatchReceivePolicy} for the consumer.
     * By default, consumer uses {@link BatchReceivePolicy#DEFAULT_POLICY} as batch receive policy.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().batchReceivePolicy(BatchReceivePolicy.builder()
     *              .maxNumMessages(100)
     *              .maxNumBytes(5 * 1024 * 1024)
     *              .timeout(100, TimeUnit.MILLISECONDS)
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy);

    /**
     * If enabled, the consumer auto-retries messages.
     * Default: disabled.
     *
     * @param retryEnable
     *            whether to auto retry message
     */
    ConsumerBuilder<T> enableRetry(boolean retryEnable);

    /**
     * Enable or disable batch index acknowledgment. To enable this feature, ensure batch index acknowledgment
     * is enabled on the broker side.
     */
    ConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled);

    /**
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message. While
     * consuming chunk-messages, chunks from same message might not be contiguous in the stream and they might be mixed
     * with other messages' chunks. so, consumer has to maintain multiple buffers to manage chunks coming from different
     * messages. This mainly happens when multiple publishers are publishing messages on the topic concurrently or
     * publisher failed to publish all chunks of the messages.
     *
     * <pre>
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 messages belong to M2 message.
     * </pre>
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChuckedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking or asking broker to redeliver later by marking it unacked.
     * This behavior can be controlled by configuration: @autoAckOldestChunkedMessageOnQueueFull
     *
     * The default value is 10.
     *
     * @param maxPendingChuckedMessage
     * @return
     * @deprecated use {@link #maxPendingChunkedMessage(int)}
     */
    @Deprecated
    ConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage);

    /**
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message. While
     * consuming chunk-messages, chunks from same message might not be contiguous in the stream and they might be mixed
     * with other messages' chunks. so, consumer has to maintain multiple buffers to manage chunks coming from different
     * messages. This mainly happens when multiple publishers are publishing messages on the topic concurrently or
     * publisher failed to publish all chunks of the messages.
     *
     * <pre>
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 messages belong to M2 message.
     * </pre>
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking or asking broker to redeliver later by marking it unacked.
     * This behavior can be controlled by configuration: @autoAckOldestChunkedMessageOnQueueFull
     *
     * The default value is 10.
     *
     * @param maxPendingChunkedMessage
     * @return
     */
    ConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage);

    /**
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once the consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acknowledging if autoAckOldestChunkedMessageOnQueueFull is true,
     * otherwise it marks them for redelivery.
     *
     * @default false
     *
     * @param autoAckOldestChunkedMessageOnQueueFull
     * @return
     */
    ConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull);

    /**
     * If the producer fails to publish all the chunks of a message, then the consumer can expire incomplete chunks if
     * the consumer doesn't receive all chunks during the expiration period (default 1 minute).
     *
     * @param duration
     * @param unit
     * @return
     */
    ConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit);

    /**
     * Enable pooling of messages and the underlying data buffers.
     * <p/>
     * When pooling is enabled, the application is responsible for calling Message.release() after the handling of every
     * received message. If “release()” is not called on a received message, it causes a memory leak. If an
     * application attempts to use an already “released” message, it might experience undefined behavior (eg: memory
     * corruption, deserialization error, etc.).
     */
    ConsumerBuilder<T> poolMessages(boolean poolMessages);

    /**
     * If configured with a non-null value, the consumer uses the processor to process the payload, including
     * decoding it to messages and triggering the listener.
     *
     * Default: null
     */
    ConsumerBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor);

    /**
     * negativeAckRedeliveryBackoff doesn't work with `consumer.negativeAcknowledge(MessageId messageId)`
     * because we are unable to get the redelivery count from the message ID.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().negativeAckRedeliveryBackoff(ExponentialRedeliveryBackoff.builder()
     *              .minNackTimeMs(1000)
     *              .maxNackTimeMs(60 * 1000)
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> negativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff);

    /**
     * redeliveryBackoff doesn't work with `consumer.negativeAcknowledge(MessageId messageId)`
     * because we are unable to get the redelivery count from the message ID.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().ackTimeout(10, TimeUnit.SECOND)
     *              .ackTimeoutRedeliveryBackoff(ExponentialRedeliveryBackoff.builder()
     *              .minNackTimeMs(1000)
     *              .maxNackTimeMs(60 * 1000)
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> ackTimeoutRedeliveryBackoff(RedeliveryBackoff ackTimeoutRedeliveryBackoff);

    /**
     * Starts the consumer in a paused state. When enabled, the consumer does not immediately fetch messages when
     * {@link #subscribe()} is called. Instead, the consumer waits to fetch messages until {@link Consumer#resume()} is
     * called.
     * <p/>
     * See also {@link Consumer#pause()}.
     * @default false
     */
    ConsumerBuilder<T> startPaused(boolean paused);

    /**
     * If this is enabled, the consumer receiver queue size is initialized as a very small value, 1 by default,
     * and will double itself until it reaches either the value set by {@link #receiverQueueSize(int)} or the client
     * memory limit set by {@link ClientBuilder#memoryLimit(long, SizeUnit)}.
     *
     * <p>The consumer receiver queue size will double if and only if:
     * <p>1) User calls receive() and there are no messages in receiver queue.
     * <p>2) The last message we put in the receiver queue took the last space available in receiver queue.
     *
     * <p>This is disabled by default and currentReceiverQueueSize is initialized as maxReceiverQueueSize.
     *
     * <p>The feature should be able to reduce client memory usage.
     *
     * @param enabled whether to enable AutoScaledReceiverQueueSize.
     */
    ConsumerBuilder<T> autoScaledReceiverQueueSizeEnabled(boolean enabled);

    /**
     * Configure topic specific options to override those set at the {@link ConsumerBuilder} level.
     *
     * @param topicName a topic name
     * @return a {@link TopicConsumerBuilder} instance
     */
    TopicConsumerBuilder<T> topicConfiguration(String topicName);

    /**
     * Configure topic specific options to override those set at the {@link ConsumerBuilder} level.
     *
     * @param topicName a topic name
     * @param builderConsumer a consumer to allow the configuration of the {@link TopicConsumerBuilder} instance
     */
    ConsumerBuilder<T> topicConfiguration(String topicName,
                                          java.util.function.Consumer<TopicConsumerBuilder<T>> builderConsumer);

    /**
     * Configure topic specific options to override those set at the {@link ConsumerBuilder} level.
     *
     * @param topicsPattern a regular expression to match a topic name
     * @return a {@link TopicConsumerBuilder} instance
     */
    TopicConsumerBuilder<T> topicConfiguration(Pattern topicsPattern);

    /**
     * Configure topic specific options to override those set at the {@link ConsumerBuilder} level.
     *
     * @param topicsPattern a regular expression to match a topic name
     * @param builderConsumer a consumer to allow the configuration of the {@link TopicConsumerBuilder} instance
     */
    ConsumerBuilder<T> topicConfiguration(Pattern topicsPattern,
                                          java.util.function.Consumer<TopicConsumerBuilder<T>> builderConsumer);
}
