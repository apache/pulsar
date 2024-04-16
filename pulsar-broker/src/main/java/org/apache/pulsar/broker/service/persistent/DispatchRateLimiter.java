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
package org.apache.pulsar.broker.service.persistent;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRateLimiter {
    public enum Type {
        TOPIC,
        SUBSCRIPTION,
        REPLICATOR,
        BROKER
    }

    private final PersistentTopic topic;
    private final String topicName;
    private final String subscriptionName;
    private final Type type;

    private final BrokerService brokerService;
    private volatile AsyncTokenBucket dispatchRateLimiterOnMessage;
    private volatile AsyncTokenBucket dispatchRateLimiterOnByte;

    public DispatchRateLimiter(PersistentTopic topic, Type type) {
        this(topic, null, type);
    }

    public DispatchRateLimiter(PersistentTopic topic, String subscriptionName, Type type) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.subscriptionName = subscriptionName;
        this.brokerService = topic.getBrokerService();
        this.type = type;
        updateDispatchRate();
    }

    public DispatchRateLimiter(BrokerService brokerService) {
        this.topic = null;
        this.topicName = null;
        this.subscriptionName = null;
        this.brokerService = brokerService;
        this.type = Type.BROKER;
        updateDispatchRate();
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        return dispatchRateLimiterOnMessage == null ? -1 : Math.max(dispatchRateLimiterOnMessage.getTokens(), 0);
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnByte() {
        return dispatchRateLimiterOnByte == null ? -1 : Math.max(dispatchRateLimiterOnByte.getTokens(), 0);
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param numberOfMessages
     * @param byteSize
     */
    public void consumeDispatchQuota(long numberOfMessages, long byteSize) {
        if (numberOfMessages > 0 && dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage.consumeTokens(numberOfMessages);
        }
        if (byteSize > 0 && dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte.consumeTokens(byteSize);
        }
    }

    /**
     * Checks if dispatch-rate limiting is enabled.
     *
     * @return
     */
    public boolean isDispatchRateLimitingEnabled() {
        return dispatchRateLimiterOnMessage != null || dispatchRateLimiterOnByte != null;
    }

    /**
     * createDispatchRate according to broker service config.
     *
     * @return
     */
    private DispatchRate createDispatchRate() {
        int dispatchThrottlingRateInMsg;
        long dispatchThrottlingRateInByte;
        ServiceConfiguration config = brokerService.pulsar().getConfiguration();

        switch (type) {
            case TOPIC:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerTopicInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerTopicInByte();
                break;
            case SUBSCRIPTION:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerSubscriptionInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerSubscriptionInByte();
                break;
            case REPLICATOR:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerReplicatorInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerReplicatorInByte();
                break;
            case BROKER:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRateInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRateInByte();
                break;
            default:
                dispatchThrottlingRateInMsg = -1;
                dispatchThrottlingRateInByte = -1;
        }


        return DispatchRate.builder()
                .dispatchThrottlingRateInMsg(dispatchThrottlingRateInMsg)
                .dispatchThrottlingRateInByte(dispatchThrottlingRateInByte)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(type != Type.BROKER && config.isDispatchThrottlingRateRelativeToPublishRate())
                .build();
    }

    /**
     * Update dispatch-throttling-rate.
     * Topic-level has the highest priority, then namespace-level, and finally use dispatch-throttling-rate in
     * broker-level
     */
    public void updateDispatchRate() {
        DispatchRate dispatchRate;
        switch (type) {
            case TOPIC:
                dispatchRate = topic.getDispatchRate();
                break;
            case SUBSCRIPTION:
                dispatchRate = topic.getSubscriptionDispatchRate(subscriptionName);
                break;
            case REPLICATOR:
                dispatchRate = topic.getReplicatorDispatchRate();
                break;
            case BROKER:
                dispatchRate = createDispatchRate();
                break;
            default:
                log.warn("ignore configured dispatch rate for type {}", type);
                return;
        }
        if (type == Type.BROKER) {
            log.info("configured broker message-dispatch rate {}", dispatchRate);
        } else {
            log.info("[{}] configured {} message-dispatch rate at broker {} subscriptionName [{}]",
                    this.topicName, type, subscriptionName == null ? "null" : subscriptionName, dispatchRate);
        }
        updateDispatchRate(dispatchRate);
    }

    public static CompletableFuture<Optional<Policies>> getPoliciesAsync(BrokerService brokerService,
         String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace);
    }

    /**
     * @deprecated Avoid using the deprecated method
     * #{@link org.apache.pulsar.broker.resources.NamespaceResources#getPoliciesIfCached(NamespaceName)} and blocking
     * call. we can use #{@link DispatchRateLimiter#getPoliciesAsync(BrokerService, String)} to instead of it.
     */
    @Deprecated
    public static Optional<Policies> getPolicies(BrokerService brokerService, String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesIfCached(namespace);
    }

    /**
     * Update dispatch rate by updating msg and byte rate-limiter. If dispatch-rate is configured &lt; 0 then it closes
     * the rate-limiter and disables appropriate rate-limiter.
     *
     * @param dispatchRate
     */
    public synchronized void updateDispatchRate(DispatchRate dispatchRate) {
        // synchronized to prevent race condition from concurrent zk-watch
        log.info("setting message-dispatch-rate {}", dispatchRate);

        long msgRate = dispatchRate.getDispatchThrottlingRateInMsg();
        long byteRate = dispatchRate.getDispatchThrottlingRateInByte();
        long ratePeriodNanos = TimeUnit.SECONDS.toNanos(Math.max(dispatchRate.getRatePeriodInSecond(), 1));

        // update msg-rateLimiter
        if (msgRate > 0) {
            if (dispatchRate.isRelativeToPublishRate()) {
                this.dispatchRateLimiterOnMessage =
                        AsyncTokenBucket.builderForDynamicRate()
                                .rateFunction(() -> getRelativeDispatchRateInMsg(dispatchRate))
                                .ratePeriodNanosFunction(() -> ratePeriodNanos)
                                .build();
            } else {
                this.dispatchRateLimiterOnMessage =
                        AsyncTokenBucket.builder().rate(msgRate).ratePeriodNanos(ratePeriodNanos)
                                .build();
            }
        } else {
            this.dispatchRateLimiterOnMessage = null;
        }

        // update byte-rateLimiter
        if (byteRate > 0) {
            if (dispatchRate.isRelativeToPublishRate()) {
                this.dispatchRateLimiterOnByte =
                        AsyncTokenBucket.builderForDynamicRate()
                                .rateFunction(() -> getRelativeDispatchRateInByte(dispatchRate))
                                .ratePeriodNanosFunction(() -> ratePeriodNanos)
                                .build();
            } else {
                this.dispatchRateLimiterOnByte =
                        AsyncTokenBucket.builder().rate(byteRate).ratePeriodNanos(ratePeriodNanos)
                                .build();
            }
        } else {
            this.dispatchRateLimiterOnByte = null;
        }
    }

    private long getRelativeDispatchRateInMsg(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInMsg() + dispatchRate.getDispatchThrottlingRateInMsg()
                : 0;
    }

    private long getRelativeDispatchRateInByte(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInByte() + dispatchRate.getDispatchThrottlingRateInByte()
                : 0;
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnMsg() {
        return dispatchRateLimiterOnMessage != null ? dispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnByte() {
        return dispatchRateLimiterOnByte != null ? dispatchRateLimiterOnByte.getRate() : -1;
    }


    public static boolean isDispatchRateEnabled(DispatchRate dispatchRate) {
        return dispatchRate != null && (dispatchRate.getDispatchThrottlingRateInMsg() > 0
                || dispatchRate.getDispatchThrottlingRateInByte() > 0);
    }

    public void close() {
        // close rate-limiter
        if (dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DispatchRateLimiter.class);
}
