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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class BacklogQuotaManager {
    private final BacklogQuotaImpl defaultQuota;
    private final PulsarService pulsar;
    private final boolean isTopicLevelPoliciesEnable;
    private final NamespaceResources namespaceResources;


    public BacklogQuotaManager(PulsarService pulsar) {
        this.isTopicLevelPoliciesEnable = pulsar.getConfiguration().isTopicLevelPoliciesEnabled();
        double backlogQuotaGB = pulsar.getConfiguration().getBacklogQuotaDefaultLimitGB();
        this.defaultQuota = BacklogQuotaImpl.builder()
                .limitSize(backlogQuotaGB > 0 ? (long) (backlogQuotaGB * BacklogQuotaImpl.BYTES_IN_GIGABYTE)
                        : pulsar.getConfiguration().getBacklogQuotaDefaultLimitBytes())
                .limitTime(pulsar.getConfiguration().getBacklogQuotaDefaultLimitSecond())
                .retentionPolicy(pulsar.getConfiguration().getBacklogQuotaDefaultRetentionPolicy())
                .build();
        this.namespaceResources = pulsar.getPulsarResources().getNamespaceResources();
        this.pulsar = pulsar;
    }

    public BacklogQuotaImpl getDefaultQuota() {
        return this.defaultQuota;
    }

    public BacklogQuotaImpl getBacklogQuota(NamespaceName namespace, BacklogQuotaType backlogQuotaType) {
        try {
            return namespaceResources.getPolicies(namespace)
                    .map(p -> (BacklogQuotaImpl) p.backlog_quota_map
                            .getOrDefault(backlogQuotaType, defaultQuota))
                    .orElse(defaultQuota);
        } catch (Exception e) {
            log.warn("Failed to read policies data, will apply the default backlog quota: namespace={}", namespace, e);
            return this.defaultQuota;
        }
    }

    public BacklogQuotaImpl getBacklogQuota(TopicName topicName, BacklogQuotaType backlogQuotaType) {
        if (!isTopicLevelPoliciesEnable) {
            return getBacklogQuota(topicName.getNamespaceObject(), backlogQuotaType);
        }

        try {
            return Optional.ofNullable(pulsar.getTopicPoliciesService().getTopicPolicies(topicName))
                    .map(TopicPolicies::getBackLogQuotaMap)
                    .map(map -> map.get(backlogQuotaType.name()))
                    .orElseGet(() -> getBacklogQuota(topicName.getNamespaceObject(), backlogQuotaType));
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
            log.debug("Topic policies cache have not init, will apply the namespace backlog quota: topicName={}",
                    topicName);
        } catch (Exception e) {
            log.error("Failed to read topic policies data, "
                            + "will apply the namespace backlog quota: topicName={}", topicName, e);
        }
        return getBacklogQuota(topicName.getNamespaceObject(), backlogQuotaType);
    }

    public long getBacklogQuotaLimitInSize(TopicName topicName) {
        return getBacklogQuota(topicName, BacklogQuotaType.destination_storage).getLimitSize();
    }

    public int getBacklogQuotaLimitInTime(TopicName topicName) {
        return getBacklogQuota(topicName, BacklogQuotaType.message_age).getLimitTime();
    }

    /**
     * Handle exceeded size backlog by using policies set in the zookeeper for given topic.
     *
     * @param persistentTopic Topic on which backlog has been exceeded
     */
    public void handleExceededBacklogQuota(PersistentTopic persistentTopic, BacklogQuotaType backlogQuotaType,
                                           boolean preciseTimeBasedBacklogQuotaCheck) {
        TopicName topicName = TopicName.get(persistentTopic.getName());
        BacklogQuota quota = getBacklogQuota(topicName, backlogQuotaType);
        log.info("Backlog quota type {} exceeded for topic [{}]. Applying [{}] policy", backlogQuotaType,
                persistentTopic.getName(), quota.getPolicy());
        switch (quota.getPolicy()) {
        case consumer_backlog_eviction:
            switch (backlogQuotaType) {
                case destination_storage:
                        dropBacklogForSizeLimit(persistentTopic, quota);
                        break;
                case message_age:
                        dropBacklogForTimeLimit(persistentTopic, quota, preciseTimeBasedBacklogQuotaCheck);
                        break;
                default:
                    break;
            }
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
     * Drop the backlog on the topic.
     *
     * @param persistentTopic
     *            The topic from which backlog should be dropped
     * @param quota
     *            Backlog quota set for the topic
     */
    private void dropBacklogForSizeLimit(PersistentTopic persistentTopic, BacklogQuota quota) {
        // Set the reduction factor to 90%. The aim is to drop down the backlog to 90% of the quota limit.
        double reductionFactor = 0.9;
        double targetSize = reductionFactor * quota.getLimitSize();

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
                    log.debug("[{}] Skipping [{}] messages on slowest consumer [{}] having backlog entries : [{}]",
                            persistentTopic.getName(), messagesToSkip, slowestConsumer.getName(), entriesInBacklog);
                }
                slowestConsumer.skipEntries(messagesToSkip, IndividualDeletedEntries.Include);
            } catch (Exception e) {
                log.error("[{}] Error skipping [{}] messages from slowest consumer [{}]", persistentTopic.getName(),
                        messagesToSkip, slowestConsumer.getName(), e);
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
     * Drop the backlog on the topic.
     *
     * @param persistentTopic
     *            The topic from which backlog should be dropped
     * @param quota
     *            Backlog quota set for the topic
     */
    private void dropBacklogForTimeLimit(PersistentTopic persistentTopic, BacklogQuota quota,
                                         boolean preciseTimeBasedBacklogQuotaCheck) {
        // If enabled precise time based backlog quota check, will expire message based on the timeBaseQuota
        if (preciseTimeBasedBacklogQuotaCheck) {
            // Set the reduction factor to 90%. The aim is to drop down the backlog to 90% of the quota limit.
            double reductionFactor = 0.9;
            int target = (int) (reductionFactor * quota.getLimitTime());
            if (log.isDebugEnabled()) {
                log.debug("[{}] target backlog expire time is [{}]", persistentTopic.getName(), target);
            }

            persistentTopic.getSubscriptions().forEach((__, subscription) ->
                    subscription.getExpiryMonitor().expireMessages(target)
            );
        } else {
            // If disabled precise time based backlog quota check, will try to remove whole ledger from cursor's backlog
            Long currentMillis = ((ManagedLedgerImpl) persistentTopic.getManagedLedger()).getClock().millis();
            ManagedLedgerImpl mLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            try {
                Long ledgerId =  mLedger.getCursors().getSlowestReaderPosition().getLedgerId();
                MLDataFormats.ManagedLedgerInfo.LedgerInfo  ledgerInfo = mLedger.getLedgerInfo(ledgerId).get();
                // Timestamp only > 0 if ledger has been closed
                while (ledgerInfo.getTimestamp() > 0
                        && currentMillis - ledgerInfo.getTimestamp() > quota.getLimitTime()) {
                    ManagedCursor slowestConsumer = mLedger.getSlowestConsumer();
                    // skip whole ledger for the slowest cursor
                    slowestConsumer.resetCursor(mLedger.getNextValidPosition(
                            PositionImpl.get(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1)));
                    ledgerId =  mLedger.getCursors().getSlowestReaderPosition().getLedgerId();
                    ledgerInfo = mLedger.getLedgerInfo(ledgerId).get();
                }
            } catch (Exception e) {
                log.error("[{}] Error resetting cursor for slowest consumer [{}]", persistentTopic.getName(),
                        mLedger.getSlowestConsumer().getName(), e);
            }
        }
    }

    /**
     * Disconnect producers on given topic.
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
