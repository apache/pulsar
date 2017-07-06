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
import java.util.concurrent.TimeUnit;

/**
 * Class specifying the configuration of a consumer. In Exclusive subscription, only a single consumer is allowed to
 * attach to the subscription. Other consumers will get an error message. In Shared subscription, multiple consumers
 * will be able to use the same subscription name and the messages will be dispatched in a round robin fashion.
 *
 *
 */
public class ConsumerConfiguration implements Serializable {

    /**
     * Resend shouldn't be requested before minAckTimeoutMillis.
     */
    static long minAckTimeoutMillis = 1000;

    private static final long serialVersionUID = 1L;

    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private MessageListener messageListener;

    private int receiverQueueSize = 1000;

    private String consumerName = null;

    private long ackTimeoutMillis = 0;
    
    private int priorityLevel = 0;

    /**
     * @return the configured timeout in milliseconds for unacked messages.
     */
    public long getAckTimeoutMillis() {
        return ackTimeoutMillis;
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
        this.ackTimeoutMillis = ackTimeoutMillis;
        return this;
    }

    /**
     * @return the configured subscription type
     */
    public SubscriptionType getSubscriptionType() {
        return this.subscriptionType;
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
        this.subscriptionType = subscriptionType;
        return this;
    }

    /**
     * @return the configured {@link MessageListener} for the consumer
     */
    public MessageListener getMessageListener() {
        return this.messageListener;
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
    public ConsumerConfiguration setMessageListener(MessageListener messageListener) {
        checkNotNull(messageListener);
        this.messageListener = messageListener;
        return this;
    }

    /**
     * @return the configure receiver queue size value
     */
    public int getReceiverQueueSize() {
        return this.receiverQueueSize;
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
        this.receiverQueueSize = receiverQueueSize;
        return this;
    }

    /**
     * @return the consumer name
     */
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * Set the consumer name.
     *
     * @param consumerName
     */
    public ConsumerConfiguration setConsumerName(String consumerName) {
        checkArgument(consumerName != null && !consumerName.equals(""));
        this.consumerName = consumerName;
        return this;
    }
    
    public int getPriorityLevel() {
        return priorityLevel;
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
        this.priorityLevel = priorityLevel;
    }
}
