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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBaseTopic implements Topic {

    protected String topic;
    protected BrokerService brokerService;
    protected volatile PublishRateLimiter publishRateLimiter;
    protected final ConcurrentOpenHashSet<Producer> producers = new ConcurrentOpenHashSet<Producer>(16, 1);
    private static final Logger log = LoggerFactory.getLogger(AbstractBaseTopic.class);

    public AbstractBaseTopic(String topic, BrokerService brokerService) {
        this.topic = topic;
        this.brokerService = brokerService;
        Policies policies = null;
        try {
            policies = brokerService.pulsar().getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, TopicName.get(topic).getNamespace()))
                    .orElseThrow(() -> new KeeperException.NoNodeException());
        } catch (Exception e) {
            log.warn("[{}] Error getting policies {} and publish throttling will be disabled", topic, e.getMessage());
        }
        updatePublishDispatcher(policies);
    }

    @Override
    public void checkPublishThrottlingRate() {
        this.publishRateLimiter.checkPublishRate();
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        this.publishRateLimiter.incrementPublishCount(numOfMessages, msgSizeInBytes);
    }

    @Override
    public void resetPublishCountAndEnableReadIfRequired() {
        if (this.publishRateLimiter.resetPublishCount()) {
            enableProduerRead();
        }
    }

    /**
     * it sets cnx auto-readable if producer's cnx is disabled due to publish-throttling
     */
    protected void enableProduerRead() {
        if (producers != null) {
            producers.forEach(producer -> producer.getCnx().enableCnxAutoRead());
        }
    }

    @Override
    public boolean isPublishRateExceeded() {
        return this.publishRateLimiter.isPublishRateExceeded();
    }

    public PublishRateLimiter getPublishRateLimiter() {
        return publishRateLimiter;
    }

    public void updateMaxPublishRate(Policies policies) {
        updatePublishDispatcher(policies);
    }

    private void updatePublishDispatcher(Policies policies) {
        final String clusterName = brokerService.pulsar().getConfiguration().getClusterName();
        final PublishRate publishRate = policies != null && policies.publish_max_message_rate != null
                ? policies.publish_max_message_rate.get(clusterName)
                : null;
        if (publishRate != null
                && (publishRate.publishThrottlingRateInByte > 0 || publishRate.publishThrottlingRateInMsg > 0)) {
            log.info("Enabling publish rate limiting {} on topic {}", publishRate, this.topic);
            if (this.publishRateLimiter == null
                    || this.publishRateLimiter == PublishRateLimiter.DISABLED_RATE_LIMITER) {
                // create new rateLimiter if rate-limiter is disabled
                this.publishRateLimiter = new PublishRateLimiterImpl(policies, clusterName);
                // lazy init Publish-rateLimiting monitoring if not initialized yet
                this.brokerService.setupPublishRateLimiterMonitor();
            } else {
                this.publishRateLimiter.update(policies, clusterName);
            }
        } else {
            log.info("Disabling publish throttling for {}", this.topic);
            this.publishRateLimiter = PublishRateLimiter.DISABLED_RATE_LIMITER;
            enableProduerRead();
        }
    }
}
