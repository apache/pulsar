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
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;
import static org.apache.pulsar.zookeeper.ZooKeeperCache.cacheTimeOutInSec;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// TODO: remove this, use DispatchRateLimiter instead.
public class SubscriptionRateLimiter {

    private final String topicName;
    private final String subName;
    private final BrokerService brokerService;
    private RateLimiter rateLimiterOnMessage;
    private RateLimiter rateLimiterOnByte;

    public SubscriptionRateLimiter(PersistentTopic topic, String subName) {
        this.topicName = topic.getName();
        this.subName = subName;
        this.brokerService = topic.getBrokerService();
        updateDispatchRate();
        registerLocalPoliciesListener();
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        return rateLimiterOnMessage == null ? -1 : rateLimiterOnMessage.getAvailablePermits();
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param msgPermits
     * @param bytePermits
     * @return
     */
    public boolean tryDispatchPermit(long msgPermits, long bytePermits) {
        boolean acquiredMsgPermit = msgPermits <= 0 || rateLimiterOnMessage == null
        // acquiring permits must be < configured msg-rate;
                || rateLimiterOnMessage.tryAcquire(msgPermits);
        boolean acquiredBytePermit = bytePermits <= 0 || rateLimiterOnByte == null
        // acquiring permits must be < configured msg-rate;
                || rateLimiterOnByte.tryAcquire(bytePermits);
        return acquiredMsgPermit && acquiredBytePermit;
    }

    /**
     * checks if subscription dispatch-rate limit is configured and if it's configured then check if permits are available or not.
     *
     * @return
     */
    public boolean hasMessageDispatchPermit() {
        return (rateLimiterOnMessage == null || rateLimiterOnMessage.getAvailablePermits() > 0)
                && (rateLimiterOnByte == null || rateLimiterOnByte.getAvailablePermits() > 0);
    }

    /**
     * Checks if subscription dispatch-rate limiting is enabled.
     *
     * @return
     */
    public boolean isDispatchRateLimitingEnabled() {
        return rateLimiterOnMessage != null || rateLimiterOnByte != null;
    }

    /**
     * Update subscription dispatch-throttling-rate.
     * gives first priority to namespace-policy configured subscription dispatch rate else applies
     * default broker dispatch-throttling-rate
     */
    public void updateDispatchRate() {
        DispatchRate dispatchRate = getPoliciesDispatchRate();
        if (dispatchRate == null) {
            dispatchRate = new DispatchRate(brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInMsg(),
                    brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInByte(), 1);
        }
        updateDispatchRate(dispatchRate);
        log.info("[{}] [{}] configured message-dispatch rate for subscription at broker {}",
            this.topicName, this.subName, dispatchRate);
    }

    /**
     * Register listener on namespace policy change to update dispatch-rate if required
     */
    private void registerLocalPoliciesListener() {
        brokerService.pulsar().getConfigurationCache().policiesCache().registerListener((path, data, stat) -> {
            final NamespaceName namespace = TopicName.get(this.topicName).getNamespaceObject();
            final String cluster = brokerService.pulsar().getConfiguration().getClusterName();
            final String policiesPath = path(POLICIES, namespace.toString());
            if (policiesPath.equals(path)) {
                DispatchRate dispatchRate = data.subscriptionDispatchRate.get(cluster);
                // update dispatch-rate only if it's configured in policies else ignore
                if (dispatchRate != null) {
                    final DispatchRate clusterDispatchRate = new DispatchRate(
                            brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInMsg(),
                            brokerService.pulsar().getConfiguration().getDispatchThrottlingRatePerSubscribeInByte(), 1);
                    // if policy-throttling rate is disabled and cluster-throttling is enabled then apply
                    // cluster-throttling rate
                    if (!isDispatchRateEnabled(dispatchRate) && isDispatchRateEnabled(clusterDispatchRate)) {
                        dispatchRate = clusterDispatchRate;
                    }
                    updateDispatchRate(dispatchRate);
                }
            }
        });
    }

    /**
     * Gets configured subscription dispatch-rate from namespace policies. Returns null if dispatch-rate is not configured
     *
     * @return
     */
    public DispatchRate getPoliciesDispatchRate() {
        final NamespaceName namespace = TopicName.get(this.topicName).getNamespaceObject();
        final String cluster = brokerService.pulsar().getConfiguration().getClusterName();
        final String path = path(POLICIES, namespace.toString());
        Optional<Policies> policies = Optional.empty();
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache().getAsync(path)
                    .get(cacheTimeOutInSec, SECONDS);
        } catch (Exception e) {
            log.warn("Failed to get message-rate for {}", this.topicName, e);
        }
        // return policy-dispatch rate only if it's enabled in policies
        return policies.map(p -> {
            DispatchRate dispatchRate = p.subscriptionDispatchRate.get(cluster);
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
        log.info("[{}] setting message-dispatch-rate {}", topicName, dispatchRate);

        long msgRate = dispatchRate.dispatchThrottlingRateInMsg;
        long byteRate = dispatchRate.dispatchThrottlingRateInByte;
        long ratePerid = dispatchRate.ratePeriodInSecond;

        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.rateLimiterOnMessage == null) {
                this.rateLimiterOnMessage = new RateLimiter(brokerService.pulsar().getExecutor(), msgRate,
                        ratePerid, TimeUnit.SECONDS);
            } else {
                this.rateLimiterOnMessage.setRate(msgRate, dispatchRate.ratePeriodInSecond,
                        TimeUnit.SECONDS);
            }
        } else {
            // message-rate should be disable and close
            if (this.rateLimiterOnMessage != null) {
                this.rateLimiterOnMessage.close();
                this.rateLimiterOnMessage = null;
            }
        }

        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.rateLimiterOnByte == null) {
                this.rateLimiterOnByte = new RateLimiter(brokerService.pulsar().getExecutor(), byteRate,
                        ratePerid, TimeUnit.SECONDS);
            } else {
                this.rateLimiterOnByte.setRate(byteRate, dispatchRate.ratePeriodInSecond,
                        TimeUnit.SECONDS);
            }
        } else {
            // message-rate should be disable and close
            if (this.rateLimiterOnByte != null) {
                this.rateLimiterOnByte.close();
                this.rateLimiterOnByte = null;
            }
        }
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnMsg() {
        return rateLimiterOnMessage != null ? rateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnByte() {
        return rateLimiterOnByte != null ? rateLimiterOnByte.getRate() : -1;
    }


    private boolean isDispatchRateEnabled(DispatchRate dispatchRate) {
        return dispatchRate != null && (dispatchRate.dispatchThrottlingRateInMsg > 0
                || dispatchRate.dispatchThrottlingRateInByte > 0);
    }

    public void close() {
        // close rate-limiter
        if (rateLimiterOnMessage != null) {
            rateLimiterOnMessage.close();
            rateLimiterOnMessage = null;
        }
        if (rateLimiterOnByte != null) {
            rateLimiterOnByte.close();
            rateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SubscriptionRateLimiter.class);
}
