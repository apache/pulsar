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
package org.apache.pulsar.storm;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Class used to specify pulsar spout configuration
 *
 *
 */
public class PulsarSpoutConfiguration extends PulsarStormConfiguration {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static final long DEFAULT_FAILED_RETRIES_TIMEOUT_NANO = TimeUnit.SECONDS.toNanos(60);
    public static final int DEFAULT_MAX_FAILED_RETRIES = -1;

    private String subscriptionName = null;
    private MessageToValuesMapper messageToValuesMapper = null;
    private long failedRetriesTimeoutNano = DEFAULT_FAILED_RETRIES_TIMEOUT_NANO;
    private int maxFailedRetries = DEFAULT_MAX_FAILED_RETRIES;
    private boolean sharedConsumerEnabled = false;

    private SubscriptionType subscriptionType = SubscriptionType.Shared;
    private boolean autoUnsubscribe = false;
    private boolean durableSubscription = true;
    // read position if non-durable subscription is enabled : default oldest message available in topic
    private MessageId nonDurableSubscriptionReadPosition = MessageId.earliest; 

    
    /**
     * @return the subscription name for the consumer in the spout
     */
    public String getSubscriptionName() {
        return subscriptionName;
    }

    /**
     * Sets the subscription name for the consumer in the spout
     *
     * @param subscriptionName
     */
    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public void setSubscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    /**
     * @return the mapper to convert pulsar message to a storm tuple
     */
    public MessageToValuesMapper getMessageToValuesMapper() {
        return messageToValuesMapper;
    }

    /**
     * Sets the mapper to convert pulsar message to a storm tuple.
     * <p>
     * Note: If the mapper returns null, the message is not emitted to the collector and is acked immediately
     * </p>
     *
     * @param mapper
     */
    public void setMessageToValuesMapper(MessageToValuesMapper mapper) {
        this.messageToValuesMapper = Objects.requireNonNull(mapper);
    }

    /**
     *
     * @param unit
     * @return the timeout for retrying failed messages
     */
    public long getFailedRetriesTimeout(TimeUnit unit) {
        return unit.convert(failedRetriesTimeoutNano, TimeUnit.NANOSECONDS);
    }

    /**
     * Sets the timeout within which the spout will re-inject failed messages with an exponential backoff <i>(default:
     * 60 seconds)</i> Note: If set to 0, the message will not be retried when failed. If set to < 0, the message will
     * be retried forever till it is successfully processed or max message retry count is reached, whichever comes
     * first.
     *
     * @param failedRetriesTimeout
     * @param unit
     */
    public void setFailedRetriesTimeout(long failedRetriesTimeout, TimeUnit unit) {
        this.failedRetriesTimeoutNano = unit.toNanos(failedRetriesTimeout);
    }

    /**
     *
     * @return the maximum number of times a failed message will be retried
     */
    public int getMaxFailedRetries() {
        return maxFailedRetries;
    }

    /**
     * Sets the maximum number of times the spout will re-inject failed messages with an exponential backoff
     * <i>(default: -1)</i> Note: If set to 0, the message will not be retried when failed. If set to < 0, the message
     * will be retried forever till it is successfully processed or configured timeout expires, whichever comes first.
     *
     * @param maxFailedRetries
     */
    public void setMaxFailedRetries(int maxFailedRetries) {
        this.maxFailedRetries = maxFailedRetries;
    }

    /**
     *
     * @return if the consumer is shared across different executors of a spout
     */
    public boolean isSharedConsumerEnabled() {
        return sharedConsumerEnabled;
    }

    /**
     * Sets whether the consumer will be shared across different executors of a spout. <i>(default: false)</i>
     *
     * @param sharedConsumerEnabled
     */
    public void setSharedConsumerEnabled(boolean sharedConsumerEnabled) {
        this.sharedConsumerEnabled = sharedConsumerEnabled;
    }
    
    public boolean isAutoUnsubscribe() {
        return autoUnsubscribe;
    }

    /**
     * It unsubscribes the subscription when spout gets closed in the topology.
     * 
     * @param autoUnsubscribe
     */
    public void setAutoUnsubscribe(boolean autoUnsubscribe) {
        this.autoUnsubscribe = autoUnsubscribe;
    }
    
    public boolean isDurableSubscription() {
        return durableSubscription;
    }

    /**
     * if subscription is not durable then it creates non-durable reader to start reading from the
     * {@link #setNonDurableSubscriptionReadPosition(MessagePosition)} in topic.
     * 
     * @param nonDurableSubscription
     */
    public void setDurableSubscription(boolean durableSubscription) {
        this.durableSubscription = durableSubscription;
    }

    public MessageId getNonDurableSubscriptionReadPosition() {
        return nonDurableSubscriptionReadPosition;
    }

    /**
     * Non-durable-subscription/Reader can be set to start reading from a specific position earliest/latest.
     * 
     * @param nonDurableSubscriptionReadPosition
     */
    public void setNonDurableSubscriptionReadPosition(MessageId nonDurableSubscriptionReadPosition) {
        this.nonDurableSubscriptionReadPosition = nonDurableSubscriptionReadPosition;
    }
}
