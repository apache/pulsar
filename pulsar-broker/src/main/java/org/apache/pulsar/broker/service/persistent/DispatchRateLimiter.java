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
import java.util.function.Supplier;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.RateLimiter;
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
    private RateLimiter dispatchRateLimiterOnMessage;
    private RateLimiter dispatchRateLimiterOnByte;

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
        return dispatchRateLimiterOnMessage == null ? -1 : dispatchRateLimiterOnMessage.getAvailablePermits();
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnByte() {
        return dispatchRateLimiterOnByte == null ? -1 : dispatchRateLimiterOnByte.getAvailablePermits();
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param msgPermits
     * @param bytePermits
     * @return
     */
    public boolean tryDispatchPermit(long msgPermits, long bytePermits) {
        boolean acquiredMsgPermit = msgPermits <= 0 || dispatchRateLimiterOnMessage == null
        // acquiring permits must be < configured msg-rate;
                || dispatchRateLimiterOnMessage.tryAcquire(msgPermits);
        boolean acquiredBytePermit = bytePermits <= 0 || dispatchRateLimiterOnByte == null
        // acquiring permits must be < configured msg-rate;
                || dispatchRateLimiterOnByte.tryAcquire(bytePermits);
        return acquiredMsgPermit && acquiredBytePermit;
    }

    /**
     * checks if dispatch-rate limit is configured and if it's configured then check if permits are available or not.
     *
     * @return
     */
    public boolean hasMessageDispatchPermit() {
        return (dispatchRateLimiterOnMessage == null || dispatchRateLimiterOnMessage.getAvailablePermits() > 0)
                && (dispatchRateLimiterOnByte == null || dispatchRateLimiterOnByte.getAvailablePermits() > 0);
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
            log.info("[{}] configured {} message-dispatch rate at broker {}",
                this.topicName, type, dispatchRate);
        }
        updateDispatchRate(dispatchRate);
    }

    public static CompletableFuture<Optional<Policies>> getPoliciesAsync(BrokerService brokerService,
         String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace);
    }

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
        long ratePeriod = dispatchRate.getRatePeriodInSecond();

        Supplier<Long> permitUpdaterMsg = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInMsg(dispatchRate)
                : null;
        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.dispatchRateLimiterOnMessage == null) {
                this.dispatchRateLimiterOnMessage =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(msgRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterMsg)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            } else {
                this.dispatchRateLimiterOnMessage.setRate(msgRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterMsg);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        Supplier<Long> permitUpdaterByte = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInByte(dispatchRate)
                : null;
        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.dispatchRateLimiterOnByte == null) {
                this.dispatchRateLimiterOnByte =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(byteRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterByte)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            } else {
                this.dispatchRateLimiterOnByte.setRate(byteRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterByte);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.close();
                this.dispatchRateLimiterOnByte = null;
            }
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
            dispatchRateLimiterOnMessage.close();
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte.close();
            dispatchRateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DispatchRateLimiter.class);
}
