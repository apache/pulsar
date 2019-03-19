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

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchRateLimiter {

    private final String topicName;
    private final String subscriptionName;
    private final BrokerService brokerService;
    private RateLimiter dispatchRateLimiterOnMessage;
    private RateLimiter dispatchRateLimiterOnByte;

    public DispatchRateLimiter(PersistentTopic topic, String subscriptionName) {
        this.topicName = topic.getName();
        this.subscriptionName = subscriptionName;
        this.brokerService = topic.getBrokerService();
        updateDispatchRate();
    }

    public DispatchRateLimiter(PersistentTopic topic) {
        this(topic, null);
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
     * Update dispatch-throttling-rate. gives first priority to namespace-policy configured dispatch rate else applies
     * default broker dispatch-throttling-rate
     */
    public void updateDispatchRate() {
        DispatchRate dispatchRate = getPoliciesDispatchRate(brokerService);
        if (dispatchRate == null) {
            if (subscriptionName == null) {
                dispatchRate = new DispatchRate(brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInMsg(),
                    brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInByte(), 1);
            } else {
                dispatchRate = new DispatchRate(brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg(),
                    brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInByte(), 1);
            }
        }
        updateDispatchRate(dispatchRate);
        log.info("[{}] [{}] configured message-dispatch rate at broker {}", this.topicName, this.subscriptionName, dispatchRate);
    }

    
    public static boolean isDispatchRateNeeded(BrokerService brokerService, Optional<Policies> policies,
            String topicName, String subscriptionName) {
        final ServiceConfiguration serviceConfig = brokerService.pulsar().getConfiguration();
        policies = policies.isPresent() ? policies : getPolicies(brokerService, topicName);
        return isDispatchRateNeeded(serviceConfig, policies, topicName, subscriptionName);
    }

    public static boolean isDispatchRateNeeded(final ServiceConfiguration serviceConfig,
            final Optional<Policies> policies, final String topicName, final String subscriptionName) {
        DispatchRate dispatchRate = getPoliciesDispatchRate(serviceConfig.getClusterName(), policies, topicName,
                subscriptionName);
        if (dispatchRate == null) {
            if (subscriptionName == null) {
                return serviceConfig.getDispatchThrottlingRatePerTopicInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerTopicInByte() > 0;
            } else {
                return serviceConfig.getDispatchThrottlingRatePerSubscriptionInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerSubscribeInByte() > 0;
            }
        }
        return true;
    }
    
    public void onPoliciesUpdate(Policies data) {
        String cluster = brokerService.pulsar().getConfiguration().getClusterName();

        DispatchRate dispatchRate;
        if (subscriptionName == null) {
            dispatchRate = data.clusterDispatchRate.get(cluster);
        } else {
            dispatchRate = data.subscriptionDispatchRate.get(cluster);
        }
        // update dispatch-rate only if it's configured in policies else ignore
        if (dispatchRate != null) {
            int inMsg = (subscriptionName == null) ?
                brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInMsg() :
                brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg();
            long inByte = (subscriptionName == null) ?
                brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInByte() :
                brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInByte();
            final DispatchRate newDispatchRate = new DispatchRate(inMsg, inByte, 1);
            // if policy-throttling rate is disabled and cluster-throttling is enabled then apply
            // cluster-throttling rate
            if (!isDispatchRateEnabled(dispatchRate) && isDispatchRateEnabled(newDispatchRate)) {
                dispatchRate = newDispatchRate;
            }
            updateDispatchRate(dispatchRate);
        }
    }

    /**
     * Gets configured dispatch-rate from namespace policies. Returns null if dispatch-rate is not configured
     *
     * @return
     */
    public DispatchRate getPoliciesDispatchRate(BrokerService brokerService) {
        final String cluster = brokerService.pulsar().getConfiguration().getClusterName();
        final Optional<Policies> policies = getPolicies(brokerService, topicName);
        return getPoliciesDispatchRate(cluster, policies, topicName, subscriptionName);
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

    public static DispatchRate getPoliciesDispatchRate(final String cluster, Optional<Policies> policies, final String topicName, final String subscriptionName) {
        // return policy-dispatch rate only if it's enabled in policies
        return policies.map(p -> {
            DispatchRate dispatchRate;
            if (subscriptionName == null) {
                dispatchRate = p.clusterDispatchRate.get(cluster);
            } else {
                dispatchRate = p.subscriptionDispatchRate.get(cluster);
            }
            return isDispatchRateEnabled(dispatchRate) ? dispatchRate : null;
        }).orElse(null);
    }

    /**
     * Update dispatch rate by updating msg and byte rate-limiter. If dispatch-rate is configured < 0 then it closes
     * the rate-limiter and disables appropriate rate-limiter.
     *
     * @param dispatchRate
     */
    public synchronized void updateDispatchRate(DispatchRate dispatchRate) {
        // synchronized to prevent race condition from concurrent zk-watch
        log.info("[{}] [{}] setting message-dispatch-rate {}", topicName, subscriptionName, dispatchRate);

        long msgRate = dispatchRate.dispatchThrottlingRateInMsg;
        long byteRate = dispatchRate.dispatchThrottlingRateInByte;
        long ratePeriod = dispatchRate.ratePeriodInSecond;

        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.dispatchRateLimiterOnMessage == null) {
                this.dispatchRateLimiterOnMessage = new RateLimiter(brokerService.pulsar().getExecutor(), msgRate,
                        ratePeriod, TimeUnit.SECONDS);
            } else {
                this.dispatchRateLimiterOnMessage.setRate(msgRate, dispatchRate.ratePeriodInSecond,
                        TimeUnit.SECONDS);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.dispatchRateLimiterOnByte == null) {
                this.dispatchRateLimiterOnByte = new RateLimiter(brokerService.pulsar().getExecutor(), byteRate,
                        ratePeriod, TimeUnit.SECONDS);
            } else {
                this.dispatchRateLimiterOnByte.setRate(byteRate, dispatchRate.ratePeriodInSecond,
                        TimeUnit.SECONDS);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.close();
                this.dispatchRateLimiterOnByte = null;
            }
        }
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
