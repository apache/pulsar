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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.v1.ConsumerV1Impl;
/**
 * Class specifying the configuration of a consumer. In Exclusive subscription, only a single consumer is allowed to
 * attach to the subscription. Other consumers will get an error message. In Shared subscription, multiple consumers
 * will be able to use the same subscription name and the messages will be dispatched in a round robin fashion.
 *
 * @deprecated Use {@link PulsarClient#newConsumer} to build and configure a {@link Consumer} instance
 */
@Deprecated
public class ConsumerConfiguration implements Serializable {

    /**
     * Resend shouldn't be requested before minAckTimeoutMillis.
     */
    static long minAckTimeoutMillis = 1000;

    private static final long serialVersionUID = 1L;

    private final ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();

    private MessageListener<byte[]> messageListener;

    public ConsumerConfiguration() {
        // Disable acknowledgment grouping when using v1 API
        conf.setAcknowledgementsGroupTimeMicros(0);
    }

    /**
     * @return the configured timeout in milliseconds for unacked messages.
     */
    public long getAckTimeoutMillis() {
        return conf.getAckTimeoutMillis();
    }

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
    public ConsumerConfiguration setAckTimeout(long ackTimeout, TimeUnit timeUnit) {
        long ackTimeoutMillis = timeUnit.toMillis(ackTimeout);
        checkArgument(ackTimeoutMillis >= minAckTimeoutMillis,
                "Ack timeout should be should be greater than " + minAckTimeoutMillis + " ms");
        conf.setAckTimeoutMillis(timeUnit.toMillis(ackTimeout));
        return this;
    }

    /**
     * @return the configured subscription type
     */
    public SubscriptionType getSubscriptionType() {
        return conf.getSubscriptionType();
    }

    /**
     * Select the subscription type to be used when subscribing to the topic.
     * <p>
     * Default is {@link SubscriptionType#Exclusive}
     *
     * @param subscriptionType
     *            the subscription type value
     */
    public ConsumerConfiguration setSubscriptionType(SubscriptionType subscriptionType) {
        checkNotNull(subscriptionType);
        conf.setSubscriptionType(subscriptionType);
        return this;
    }

    /**
     * @return the configured {@link MessageListener} for the consumer
     */
    public MessageListener<byte[]> getMessageListener() {
        return messageListener;
    }

    /**
     * Sets a {@link MessageListener} for the consumer
     * <p>
     * When a {@link MessageListener} is set, application will receive messages through it. Calls to
     * {@link Consumer#receive()} will not be allowed.
     *
     * @param messageListener
     *            the listener object
     */
    public ConsumerConfiguration setMessageListener(MessageListener<byte[]> messageListener) {
        checkNotNull(messageListener);
        this.messageListener = messageListener;
        conf.setMessageListener(new org.apache.pulsar.shade.client.api.v2.MessageListener<byte[]>() {

            @Override
            public void received(org.apache.pulsar.shade.client.api.v2.Consumer<byte[]> consumer, Message<byte[]> msg) {
                messageListener.received(new ConsumerV1Impl(consumer), msg);
            }

            @Override
            public void reachedEndOfTopic(org.apache.pulsar.shade.client.api.v2.Consumer<byte[]> consumer) {
                messageListener.reachedEndOfTopic(new ConsumerV1Impl(consumer));
            }
        });
        return this;
    }

    /**
     * @return this configured {@link ConsumerEventListener} for the consumer.
     * @see #setConsumerEventListener(ConsumerEventListener)
     * @since 2.0
     */
    public ConsumerEventListener getConsumerEventListener() {
        return conf.getConsumerEventListener();
    }

    /**
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>
     * The consumer group listener is used for receiving consumer state change in a consumer group for failover
     * subscription. Application can then react to the consumer state changes.
     *
     * <p>
     * This change is experimental. It is subject to changes coming in release 2.0.
     *
     * @param listener
     *            the consumer group listener object
     * @return consumer configuration
     * @since 2.0
     */
    public ConsumerConfiguration setConsumerEventListener(ConsumerEventListener listener) {
        checkNotNull(listener);
        conf.setConsumerEventListener(listener);
        return this;
    }

    /**
     * @return the configure receiver queue size value
     */
    public int getReceiverQueueSize() {
        return conf.getReceiverQueueSize();
    }

    /**
     * @return the configured max total receiver queue size across partitions
     */
    public int getMaxTotalReceiverQueueSizeAcrossPartitions() {
        return conf.getMaxTotalReceiverQueueSizeAcrossPartitions();
    }

    /**
     * Set the max total receiver queue size across partitons.
     * <p>
     * This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #setReceiverQueueSize(int)} if the total exceeds this value (default: 50000).
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     */
    public void setMaxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
        checkArgument(maxTotalReceiverQueueSizeAcrossPartitions >= conf.getReceiverQueueSize());
        conf.setMaxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
    }

    /**
     * @return the CryptoKeyReader
     */
    public CryptoKeyReader getCryptoKeyReader() {
        return conf.getCryptoKeyReader();
    }

    /**
     * Sets a {@link CryptoKeyReader}
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     */
    public ConsumerConfiguration setCryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        checkNotNull(cryptoKeyReader);
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    /**
     * Sets the ConsumerCryptoFailureAction to the value specified
     *
     * @param action
     *            consumer action
     */
    public void setCryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
    }

    /**
     * @return The ConsumerCryptoFailureAction
     */
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return conf.getCryptoFailureAction();
    }

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
    public ConsumerConfiguration setReceiverQueueSize(int receiverQueueSize) {
        checkArgument(receiverQueueSize >= 0, "Receiver queue size cannot be negative");
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    /**
     * @return the consumer name
     */
    public String getConsumerName() {
        return conf.getConsumerName();
    }

    /**
     * Set the consumer name.
     *
     * @param consumerName
     */
    public ConsumerConfiguration setConsumerName(String consumerName) {
        checkArgument(consumerName != null && !consumerName.equals(""));
        conf.setConsumerName(consumerName);
        return this;
    }

    public int getPriorityLevel() {
        return conf.getPriorityLevel();
    }

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
    public void setPriorityLevel(int priorityLevel) {
        conf.setPriorityLevel(priorityLevel);
    }

    public boolean getReadCompacted() {
        return conf.isReadCompacted();
    }

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
    public ConsumerConfiguration setReadCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }

    /**
     * Set a name/value property with this consumer.
     *
     * @param key
     * @param value
     * @return
     */
    public ConsumerConfiguration setProperty(String key, String value) {
        checkArgument(key != null);
        checkArgument(value != null);
        conf.getProperties().put(key, value);
        return this;
    }

    /**
     * Add all the properties in the provided map
     *
     * @param properties
     * @return
     */
    public ConsumerConfiguration setProperties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }

    public Map<String, String> getProperties() {
        return conf.getProperties();
    }

    public ConsumerConfigurationData<byte[]> getConfigurationData() {
        return conf;
    }

     /**
     * @param subscriptionInitialPosition the initial position at which to set
     * set cursor  when subscribing to the topic first time
     * Default is {@value InitialPosition.Latest}
     */
    public ConsumerConfiguration setSubscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
        conf.setSubscriptionInitialPosition(subscriptionInitialPosition);
        return this;
    }

    /**
     * @return the configured {@link subscriptionInitialPosition} for the consumer
     */
    public SubscriptionInitialPosition getSubscriptionInitialPosition(){
        return conf.getSubscriptionInitialPosition();
    }
}
