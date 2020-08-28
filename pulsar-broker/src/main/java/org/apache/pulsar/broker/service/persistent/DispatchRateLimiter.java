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
package org.apache.pulsar.broker.service.persistent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRateLimiter {

    public enum Type {
        TOPIC,
        SUBSCRIPTION,
        REPLICATOR
    }

    private final PersistentTopic topic;
    private final String topicName;
    private final Type type;

    private final BrokerService brokerService;
    private RateLimiter dispatchRateLimiterOnMessage;
    private RateLimiter dispatchRateLimiterOnByte;
    private long subscriptionRelativeRatelimiterOnMessage;
    private long subscriptionRelativeRatelimiterOnByte;

    public DispatchRateLimiter(PersistentTopic topic, Type type) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.brokerService = topic.getBrokerService();
        this.type = type;
        this.subscriptionRelativeRatelimiterOnMessage = -1;
        this.subscriptionRelativeRatelimiterOnByte = -1;
        updateDispatchRate();
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        return dispatchRateLimiterOnMessage == null ? -1 : dispatchRateLimiterOnMessage.getAvailablePermits();
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
            default:
                dispatchThrottlingRateInMsg = -1;
                dispatchThrottlingRateInByte = -1;
        }

        return new DispatchRate(dispatchThrottlingRateInMsg, dispatchThrottlingRateInByte, 1,
                config.isDispatchThrottlingRateRelativeToPublishRate());
    }

    /**
     * Update dispatch-throttling-rate. gives first priority to namespace-policy configured dispatch rate else applies
     * default broker dispatch-throttling-rate
     */
    public void updateDispatchRate() {
        Optional<DispatchRate> dispatchRate = getSystemTopicDispatchRate(brokerService, topicName);
        if (!dispatchRate.isPresent()) {
            dispatchRate =Optional.ofNullable(getPoliciesDispatchRate(brokerService));

            if (!dispatchRate.isPresent()) {
                dispatchRate = Optional.of(createDispatchRate());
            }
        }

        updateDispatchRate(dispatchRate.get());
        log.info("[{}] configured {} message-dispatch rate at broker {}", this.topicName, type, dispatchRate.get());
    }

    public static boolean isDispatchRateNeeded(BrokerService brokerService, Optional<Policies> policies,
            String topicName, Type type) {
        final ServiceConfiguration serviceConfig = brokerService.pulsar().getConfiguration();
        if (serviceConfig.isTopicLevelPoliciesEnabled() && type == Type.TOPIC) {
            Optional<DispatchRate> dispatchRate = getSystemTopicDispatchRate(brokerService, topicName);
            if (dispatchRate.isPresent()) {
                return true;
            }
        }

        policies = policies.isPresent() ? policies : getPolicies(brokerService, topicName);
        return isDispatchRateNeeded(serviceConfig, policies, topicName, type);
    }

    public static Optional<DispatchRate> getSystemTopicDispatchRate(BrokerService brokerService, String topicName) {
        Optional<DispatchRate> dispatchRate = Optional.empty();
        final ServiceConfiguration serviceConfiguration = brokerService.pulsar().getConfiguration();
        if (serviceConfiguration.isTopicLevelPoliciesEnabled()) {
            try {
                dispatchRate = Optional.ofNullable(brokerService.pulsar()
                    .getTopicPoliciesService().getTopicPolicies(TopicName.get(topicName)))
                    .map(TopicPolicies::getDispatchRate);
            } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e){
                log.debug("Topic {} policies cache have not init.", topicName);
            } catch (Exception e) {
                log.debug("[{}] Failed to get topic policies. Exception: {}", topicName, e);
            }
        }

        return dispatchRate;
    }

    public static boolean isDispatchRateNeeded(final ServiceConfiguration serviceConfig,
            final Optional<Policies> policies, final String topicName, final Type type) {
        DispatchRate dispatchRate = getPoliciesDispatchRate(serviceConfig.getClusterName(), policies, type);
        if (dispatchRate == null) {
            switch (type) {
                case TOPIC:
                    return serviceConfig.getDispatchThrottlingRatePerTopicInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerTopicInByte() > 0;
                case SUBSCRIPTION:
                    return serviceConfig.getDispatchThrottlingRatePerSubscriptionInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerSubscriptionInByte() > 0;
                case REPLICATOR:
                    return serviceConfig.getDispatchThrottlingRatePerReplicatorInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerReplicatorInByte() > 0;
                default:
                    log.error("error DispatchRateLimiter type: {} ", type);
                    return false;
            }
        }
        return true;
    }

    @SuppressWarnings("deprecation")
    public void onPoliciesUpdate(Policies data) {
        String cluster = brokerService.pulsar().getConfiguration().getClusterName();

        DispatchRate dispatchRate;

        switch (type) {
            case TOPIC:
                dispatchRate = data.topicDispatchRate.get(cluster);
                if (dispatchRate == null) {
                    dispatchRate = data.clusterDispatchRate.get(cluster);
                }
                break;
            case SUBSCRIPTION:
                dispatchRate = data.subscriptionDispatchRate.get(cluster);
                break;
            case REPLICATOR:
                dispatchRate = data.replicatorDispatchRate.get(cluster);
                break;
            default:
                log.error("error DispatchRateLimiter type: {} ", type);
                dispatchRate = null;
        }

        // update dispatch-rate only if it's configured in policies else ignore
        if (dispatchRate != null) {
            final DispatchRate newDispatchRate = createDispatchRate();

            // if policy-throttling rate is disabled and cluster-throttling is enabled then apply
            // cluster-throttling rate
            if (!isDispatchRateEnabled(dispatchRate) && isDispatchRateEnabled(newDispatchRate)) {
                dispatchRate = newDispatchRate;
            }
            updateDispatchRate(dispatchRate);
        }
    }

    @SuppressWarnings("deprecation")
    public static DispatchRate getPoliciesDispatchRate(final String cluster, Optional<Policies> policies, Type type) {
        // return policy-dispatch rate only if it's enabled in policies
        return policies.map(p -> {
            DispatchRate dispatchRate;
            switch (type) {
                case TOPIC:
                    dispatchRate = p.topicDispatchRate.get(cluster);
                    if (dispatchRate == null) {
                        dispatchRate = p.clusterDispatchRate.get(cluster);
                    }
                    break;
                case SUBSCRIPTION:
                    dispatchRate = p.subscriptionDispatchRate.get(cluster);
                    break;
                case REPLICATOR:
                    dispatchRate = p.replicatorDispatchRate.get(cluster);
                    break;
                default:
                    log.error("error DispatchRateLimiter type: {} ", type);
                    return null;
            }
            return isDispatchRateEnabled(dispatchRate) ? dispatchRate : null;
        }).orElse(null);
    }


    /**
     * Gets configured dispatch-rate from namespace policies. Returns null if dispatch-rate is not configured
     *
     * @return
     */
    public DispatchRate getPoliciesDispatchRate(BrokerService brokerService) {
        final String cluster = brokerService.pulsar().getConfiguration().getClusterName();
        final Optional<Policies> policies = getPolicies(brokerService, topicName);
        return getPoliciesDispatchRate(cluster, policies, type);
    }

    public static Optional<Policies> getPolicies(BrokerService brokerService, String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        final String path = path(POLICIES, namespace.toString());
        Optional<Policies> policies = Optional.empty();
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache().getAsync(path)
                    .get(brokerService.pulsar().getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
        } catch (Exception e) {
            log.warn("Failed to get message-rate for {} ", topicName, e);
        }
        return policies;
    }

    /**
     * Update dispatch rate by updating msg and byte rate-limiter. If dispatch-rate is configured < 0 then it closes
     * the rate-limiter and disables appropriate rate-limiter.
     *
     * @param dispatchRate
     */
    public synchronized void updateDispatchRate(DispatchRate dispatchRate) {
        // synchronized to prevent race condition from concurrent zk-watch
        log.info("setting message-dispatch-rate {}", dispatchRate);

        long msgRate = dispatchRate.dispatchThrottlingRateInMsg;
        long byteRate = dispatchRate.dispatchThrottlingRateInByte;
        long ratePeriod = dispatchRate.ratePeriodInSecond;

        Supplier<Long> permitUpdaterMsg = dispatchRate.relativeToPublishRate
                ? () -> getRelativeDispatchRateInMsg(dispatchRate)
                : null;
        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.dispatchRateLimiterOnMessage == null) {
                this.dispatchRateLimiterOnMessage = new RateLimiter(brokerService.pulsar().getExecutor(), msgRate,
                        ratePeriod, TimeUnit.SECONDS, permitUpdaterMsg);
            } else {
                this.dispatchRateLimiterOnMessage.setRate(msgRate, dispatchRate.ratePeriodInSecond,
                        TimeUnit.SECONDS, permitUpdaterMsg);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        Supplier<Long> permitUpdaterByte = dispatchRate.relativeToPublishRate
                ? () -> getRelativeDispatchRateInByte(dispatchRate)
                : null;
        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.dispatchRateLimiterOnByte == null) {
                this.dispatchRateLimiterOnByte = new RateLimiter(brokerService.pulsar().getExecutor(), byteRate,
                        ratePeriod, TimeUnit.SECONDS, permitUpdaterByte);
            } else {
                this.dispatchRateLimiterOnByte.setRate(byteRate, dispatchRate.ratePeriodInSecond,
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
                ? (long) topic.getLastUpdatedAvgPublishRateInMsg() + dispatchRate.dispatchThrottlingRateInMsg
                : 0;
    }

    private long getRelativeDispatchRateInByte(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInByte() + dispatchRate.dispatchThrottlingRateInByte
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


    private static boolean isDispatchRateEnabled(DispatchRate dispatchRate) {
        return dispatchRate != null && (dispatchRate.dispatchThrottlingRateInMsg > 0
                || dispatchRate.dispatchThrottlingRateInByte > 0);
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
