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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

public class BacklogQuotaManager {
    private static final Logger log = LoggerFactory.getLogger(BacklogQuotaManager.class);
    private final BacklogQuota defaultQuota;
    private final ZooKeeperDataCache<Policies> zkCache;
    private final PulsarService pulsar;
    private final boolean isTopicLevelPoliciesEnable;


    public BacklogQuotaManager(PulsarService pulsar) {
        this.isTopicLevelPoliciesEnable = pulsar.getConfiguration().isTopicLevelPoliciesEnabled();
        this.defaultQuota = new BacklogQuota(
                pulsar.getConfiguration().getBacklogQuotaDefaultLimitGB() * 1024 * 1024 * 1024,
                pulsar.getConfiguration().getBacklogQuotaDefaultRetentionPolicy());
        this.zkCache = pulsar.getConfigurationCache().policiesCache();
        this.pulsar = pulsar;
    }

    public BacklogQuota getDefaultQuota() {
        return this.defaultQuota;
    }

    public BacklogQuota getBacklogQuota(String namespace, String policyPath) {
        try {
            return zkCache.get(policyPath)
                    .map(p -> p.backlog_quota_map.getOrDefault(BacklogQuotaType.destination_storage, defaultQuota))
                    .orElse(defaultQuota);
        } catch (Exception e) {
            log.error("Failed to read policies data, will apply the default backlog quota: namespace={}", namespace, e);
            return this.defaultQuota;
        }
    }

    public BacklogQuota getBacklogQuota(TopicName topicName) {
        String policyPath = AdminResource.path(POLICIES, topicName.getNamespace());
        if (!isTopicLevelPoliciesEnable) {
            return getBacklogQuota(topicName.getNamespace(),policyPath);
        }

        try {
            return Optional.ofNullable(pulsar.getTopicPoliciesService().getTopicPolicies(topicName))
                    .map(TopicPolicies::getBackLogQuotaMap)
                    .map(map -> map.get(BacklogQuotaType.destination_storage.name()))
                    .orElseGet(() -> getBacklogQuota(topicName.getNamespace(),policyPath));
        } catch (Exception e) {
            log.error("Failed to read policies data, will apply the default backlog quota: topicName={}", topicName, e);
        }
        return getBacklogQuota(topicName.getNamespace(),policyPath);
    }

    public long getBacklogQuotaLimit(TopicName topicName) {
        return getBacklogQuota(topicName).getLimit();
    }

    /**
     * Handle exceeded backlog by using policies set in the zookeeper for given topic
     *
     * @param persistentTopic
     *            Topic on which backlog has been exceeded
     */
    public void handleExceededBacklogQuota(PersistentTopic persistentTopic) {
        TopicName topicName = TopicName.get(persistentTopic.getName());
        BacklogQuota quota = getBacklogQuota(topicName);
        log.info("Backlog quota exceeded for topic [{}]. Applying [{}] policy", persistentTopic.getName(),
                quota.getPolicy());
        switch (quota.getPolicy()) {
        case consumer_backlog_eviction:
            dropBacklog(persistentTopic, quota);
            break;
        case producer_exception:
        case producer_request_hold:
            disconnectProducers(persistentTopic);
            break;
        default:
            break;
        }
    }

    /**
     * Drop the backlog on the topic
     *
     * @param persistentTopic
     *            The topic from which backlog should be dropped
     * @param quota
     *            Backlog quota set for the topic
     */
    private void dropBacklog(PersistentTopic persistentTopic, BacklogQuota quota) {
        // Set the reduction factor to 90%. The aim is to drop down the backlog to 90% of the quota limit.
        double reductionFactor = 0.9;
        double targetSize = reductionFactor * quota.getLimit();

        // Get estimated unconsumed size for the managed ledger associated with this topic. Estimated size is more
        // useful than the actual storage size. Actual storage size gets updated only when managed ledger is trimmed.
        ManagedLedgerImpl mLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        long backlogSize = mLedger.getEstimatedBacklogSize();

        if (log.isDebugEnabled()) {
            log.debug("[{}] target size is [{}] for quota limit [{}], backlog size is [{}]", persistentTopic.getName(),
                    targetSize, targetSize / reductionFactor, backlogSize);
        }
        ManagedCursor previousSlowestConsumer = null;
        while (backlogSize > targetSize) {
            // Get the slowest consumer for this managed ledger and save the ledger id of the marked delete position of
            // slowest consumer. Calculate the factor which is used in calculating number of messages to be skipped.
            ManagedCursor slowestConsumer = mLedger.getSlowestConsumer();
            if (slowestConsumer == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] slowest consumer null.", persistentTopic.getName());
                }
                break;
            }
            double messageSkipFactor = ((backlogSize - targetSize) / backlogSize);

            if (slowestConsumer == previousSlowestConsumer) {
                log.info("[{}] Cursors not progressing, target size is [{}] for quota limit [{}], backlog size is [{}]",
                        persistentTopic.getName(), targetSize, targetSize / reductionFactor, backlogSize);
                break;
            }

            // Calculate number of messages to be skipped using the current backlog and the skip factor.
            long entriesInBacklog = slowestConsumer.getNumberOfEntriesInBacklog(false);
            int messagesToSkip = (int) (messageSkipFactor * entriesInBacklog);
            try {
                // If there are no messages to skip, break out of the loop
                if (messagesToSkip == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("no messages to skip for [{}]", slowestConsumer);
                    }
                    break;
                }
                // Skip messages on the slowest consumer
                if (log.isDebugEnabled()) {
                    log.debug("Skipping [{}] messages on slowest consumer [{}] having backlog entries : [{}]",
                            messagesToSkip, slowestConsumer.getName(), entriesInBacklog);
                }
                slowestConsumer.skipEntries(messagesToSkip, IndividualDeletedEntries.Include);
            } catch (Exception e) {
                log.error("Error skipping [{}] messages from slowest consumer : [{}]", messagesToSkip,
                        slowestConsumer.getName());
            }

            // Make sure that unconsumed size is updated every time when we skip the messages.
            backlogSize = mLedger.getEstimatedBacklogSize();
            previousSlowestConsumer = slowestConsumer;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Updated unconsumed size = [{}]. skipFactor: [{}]", persistentTopic.getName(),
                        backlogSize, messageSkipFactor);
            }
        }

    }

    /**
     * Disconnect producers on given topic
     *
     * @param persistentTopic
     *            The topic on which all producers should be disconnected
     */
    private void disconnectProducers(PersistentTopic persistentTopic) {
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        Map<String, Producer> producers = persistentTopic.getProducers();

        producers.values().forEach(producer -> {
            log.info("Producer [{}] has exceeded backlog quota on topic [{}]. Disconnecting producer",
                    producer.getProducerName(), persistentTopic.getName());
            futures.add(producer.disconnect());
        });

        FutureUtil.waitForAll(futures).thenRun(() -> {
            log.info("All producers on topic [{}] are disconnected", persistentTopic.getName());
        }).exceptionally(exception -> {
            log.error("Error in disconnecting producers on topic [{}] [{}]", persistentTopic.getName(), exception);
            return null;

        });
    }
}
