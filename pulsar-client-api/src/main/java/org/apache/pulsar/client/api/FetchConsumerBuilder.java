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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A consumer is only allowed to subscribe to one topic.
 * The subscription type must be Exclusive or Failover,
 * and autoScaledReceiverQueueSizeEnabled must be enabled.
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FetchConsumerBuilder<T> extends Cloneable {

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("ackTimeoutMillis", 1000);
     * config.put("receiverQueueSize", 2000);
     *
     * Consumer<byte[]> builder = client.newFetchConsumer()
     *              .loadConf(config)
     *              .subscribe();
     *
     * Consumer<byte[]> consumer = builder.subscribe();
     * }</pre>
     *
     * @param config configuration to load
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Finalize the {@link FetchConsumer} creation by subscribing to the topic.
     *
     * <p>If the subscription does not exist, a new subscription is created. By default, the subscription
     * is created at the end of the topic. See {@link #subscriptionInitialPosition(SubscriptionInitialPosition)}
     * to configure the initial position behavior.
     *
     * <p>Once a subscription is created, it retains the data and the subscription cursor even if the consumer
     * is not connected.
     *
     * @return the fetch consumer builder instance
     * @throws PulsarClientException
     *             if the subscribe operation fails
     */
    FetchConsumer<T> subscribe() throws PulsarClientException;

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
    CompletableFuture<FetchConsumer<T>> subscribeAsync();

    /**
     * Specify the topics this consumer subscribes to.
     *
     * @param topicNames a set of topics that the consumer subscribes to
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> topic(String... topicNames);

    /**
     * Specify a list of topics that this consumer subscribes to.
     *
     * @param topicNames a list of topics that the consumer subscribes to
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * Specify the subscription name for this consumer.
     *
     * <p>This argument is required when constructing the consumer.
     *
     * @param subscriptionName the name of the subscription that this consumer should attach to
     *
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Specify the subscription properties for this subscription.
     * Properties are immutable, and consumers under the same subscription will fail to create a subscription
     * if they use different properties.
     * @param subscriptionProperties the properties of the subscription
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> subscriptionProperties(Map<String, String> subscriptionProperties);

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
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> isAckReceiptEnabled(boolean isAckReceiptEnabled);

    /**
     * Select the subscription type to be used when subscribing to a topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionType#Exclusive} (Default)</li>
     *  <li>{@link SubscriptionType#Failover}</li>
     *  <li>{@link SubscriptionType#Shared}</li>
     *  <li>{@link SubscriptionType#Key_Shared}</li>
     * </ul>
     *
     * @param subscriptionType
     *            the subscription type value
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);

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
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode);

    /**
     * Sets a {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param privateKey
     *            the private key that is always used to decrypt message payloads.
     * @return the fetch consumer builder instance
     * @since 2.8.0
     */
    FetchConsumerBuilder<T> defaultCryptoKeyReader(String privateKey);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKeys
     *            the map of private key names and their URIs used to decrypt message payloads.
     * @return the fetch consumer builder instance
     * @since 2.8.0
     */
    FetchConsumerBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys);

    /**
     * Sets a {@link MessageCrypto}.
     *
     * <p>Contains methods to encrypt/decrypt messages for end-to-end encryption.
     *
     * @param messageCrypto
     *            MessageCrypto object
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto);

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action the consumer takes in case of decryption failures
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);


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
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);

    /**
     * Set the number of messages for group consumer acknowledgments.
     *
     * <p>By default, the consumer uses at most 1000 messages to send out acknowledgments to the broker.
     *
     * @param messageNum
     *
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> maxAcknowledgmentGroupSize(int messageNum);

    /**
     *
     * @param replicateSubscriptionState
     */
    FetchConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState);

    /**
     * Sets the consumer name.
     *
     * <p>Consumer names are informative, and can be used to identify a particular consumer
     * instance from the topic stats.
     *
     * @param consumerName
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> consumerName(String consumerName);

    /**
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>The consumer group listener is used for receiving consumer state changes in a consumer group for failover
     * subscriptions. The application can then react to the consumer state changes.
     *
     * @param consumerEventListener
     *            the consumer group listener object
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);

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
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> readCompacted(boolean readCompacted);

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
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> property(String key, String value);

    /**
     * Add all the properties in the provided map to the consumer.
     *
     * <p>Properties are application-defined metadata that can be attached to the consumer.
     * When getting topic stats, this metadata is associated with the consumer stats for easier identification.
     *
     * @param properties the map with properties
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> properties(Map<String, String> properties);

    /**
     * Sets the {@link SubscriptionInitialPosition} for the consumer.
     *
     * @param subscriptionInitialPosition
     *            the position where to initialize a newly created subscription
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);

    /**
     * Intercept {@link Consumer}.
     *
     * @param interceptors the list of interceptors to intercept the consumer created by this builder.
     */
    FetchConsumerBuilder<T> intercept(ConsumerInterceptor<T> ...interceptors);

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
     * client.newFetchConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
     *          .subscribe();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
     * To set a custom dead letter topic name:
     * <pre>
     * client.newFetchConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy
     *              .builder()
     *              .maxRedeliverCount(10)
     *              .deadLetterTopic("your-topic-name")
     *              .build())
     *          .subscribe();
     * </pre>
     */
    FetchConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);

    /**
     * Sets {@link BatchReceivePolicy} for the consumer.
     * By default, consumer uses {@link BatchReceivePolicy#DEFAULT_POLICY} as batch receive policy.
     *
     * <p>Example:
     * <pre>
     * client.newFetchConsumer().batchReceivePolicy(BatchReceivePolicy.builder()
     *              .maxNumMessages(100)
     *              .maxNumBytes(5 * 1024 * 1024)
     *              .timeout(100, TimeUnit.MILLISECONDS)
     *              .build()).subscribe();
     * </pre>
     */
    FetchConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy);

    /**
     * If enabled, the consumer auto-retries messages.
     * Default: disabled.
     *
     * @param retryEnable
     *            whether to auto retry message
     */
    FetchConsumerBuilder<T> enableRetry(boolean retryEnable);

    /**
     * Enable or disable batch index acknowledgment. To enable this feature, ensure batch index acknowledgment
     * is enabled on the broker side.
     */
    FetchConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled);

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
    FetchConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage);

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
    FetchConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage);

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
    FetchConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull);

    /**
     * If the producer fails to publish all the chunks of a message, then the consumer can expire incomplete chunks if
     * the consumer doesn't receive all chunks during the expiration period (default 1 minute).
     *
     * @param duration
     * @param unit
     * @return
     */
    FetchConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit);

    /**
     * Enable pooling of messages and the underlying data buffers.
     * <p/>
     * When pooling is enabled, the application is responsible for calling Message.release() after the handling of every
     * received message. If “release()” is not called on a received message, it causes a memory leak. If an
     * application attempts to use an already “released” message, it might experience undefined behavior (eg: memory
     * corruption, deserialization error, etc.).
     */
    FetchConsumerBuilder<T> poolMessages(boolean poolMessages);

    /**
     * If configured with a non-null value, the consumer uses the processor to process the payload, including
     * decoding it to messages and triggering the listener.
     *
     * Default: null
     */
    FetchConsumerBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor);

    /**
     * negativeAckRedeliveryBackoff sets the redelivery backoff policy for messages that are negatively acknowledged
     * using
     * `consumer.negativeAcknowledge(Message<?> message)` but not with `consumer.negativeAcknowledge(MessageId
     * messageId)`.
     * This setting allows specifying a backoff policy for messages that are negatively acknowledged,
     * enabling more flexible control over the delay before such messages are redelivered.
     *
     * <p>This configuration accepts a {@link RedeliveryBackoff} object that defines the backoff policy.
     * The policy can be either a fixed delay or an exponential backoff. An exponential backoff policy
     * is beneficial in scenarios where increasing the delay between consecutive redeliveries can help
     * mitigate issues like temporary resource constraints or processing bottlenecks.
     *
     * <p>Note: This backoff policy does not apply when using `consumer.negativeAcknowledge(MessageId messageId)`
     * because the redelivery count cannot be determined from just the message ID. It is recommended to use
     * `consumer.negativeAcknowledge(Message<?> message)` if you want to leverage the redelivery backoff policy.
     *
     * <p>Example usage:
     * <pre>{@code
     * client.newFetchConsumer()
     *       .negativeAckRedeliveryBackoff(ExponentialRedeliveryBackoff.builder()
     *           .minDelayMs(1000)   // Set minimum delay to 1 second
     *           .maxDelayMs(60000)  // Set maximum delay to 60 seconds
     *           .build())
     *       .subscribe();
     * }</pre>
     *
     * @param negativeAckRedeliveryBackoff the backoff policy to use for negatively acknowledged messages
     * @return the fetch consumer builder instance
     */
    FetchConsumerBuilder<T> negativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff);

    /**
     * Starts the consumer in a paused state. When enabled, the consumer does not immediately fetch messages when
     * {@link #subscribe()} is called. Instead, the consumer waits to fetch messages until {@link Consumer#resume()} is
     * called.
     * <p/>
     * See also {@link Consumer#pause()}.
     * @default false
     */
    FetchConsumerBuilder<T> startPaused(boolean paused);

    FetchConsumerBuilder<T> clone();
}
