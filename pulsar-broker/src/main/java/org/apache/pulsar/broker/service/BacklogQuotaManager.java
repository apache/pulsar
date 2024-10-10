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
package org.apache.pulsar.broker.service;

import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics.BacklogQuotaMetrics;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@Slf4j
public class BacklogQuotaManager {
    @Getter
    private final BacklogQuotaImpl defaultQuota;
    private final NamespaceResources namespaceResources;

    public BacklogQuotaManager(PulsarService pulsar) {
        double backlogQuotaGB = pulsar.getConfiguration().getBacklogQuotaDefaultLimitGB();
        this.defaultQuota = BacklogQuotaImpl.builder()
                .limitSize(backlogQuotaGB > 0 ? (long) (backlogQuotaGB * BacklogQuotaImpl.BYTES_IN_GIGABYTE)
                        : pulsar.getConfiguration().getBacklogQuotaDefaultLimitBytes())
                .limitTime(pulsar.getConfiguration().getBacklogQuotaDefaultLimitSecond())
                .retentionPolicy(pulsar.getConfiguration().getBacklogQuotaDefaultRetentionPolicy())
                .build();
        this.namespaceResources = pulsar.getPulsarResources().getNamespaceResources();
    }

    public BacklogQuotaImpl getBacklogQuota(NamespaceName namespace, BacklogQuotaType backlogQuotaType) {
        try {
            if (namespaceResources == null) {
                log.warn("Failed to read policies data from metadata store because namespaceResources is null."
                        + "default backlog quota will be applied: namespace={}", namespace);
                return this.defaultQuota;
            } else {
                return namespaceResources.getPolicies(namespace)
                        .map(p -> (BacklogQuotaImpl) p.backlog_quota_map
                                .getOrDefault(backlogQuotaType, defaultQuota))
                        .orElse(defaultQuota);
            }
        } catch (MetadataStoreException e) {
            log.warn("Failed to read policies data from metadata store,"
                    + " will apply the default backlog quota: namespace={}", namespace, e);
            return this.defaultQuota;
        }
    }

    /**
     * Handle exceeded size backlog by using policies set in the zookeeper for given topic.
     *
     * @param persistentTopic Topic on which backlog has been exceeded
     */
    public void handleExceededBacklogQuota(PersistentTopic persistentTopic, BacklogQuotaType backlogQuotaType,
                                           boolean preciseTimeBasedBacklogQuotaCheck) {
        BacklogQuota quota = persistentTopic.getBacklogQuota(backlogQuotaType);
        BacklogQuotaMetrics topicBacklogQuotaMetrics =
                persistentTopic.getPersistentTopicMetrics().getBacklogQuotaMetrics();
        log.info("Backlog quota type {} exceeded for topic [{}]. Applying [{}] policy", backlogQuotaType,
                persistentTopic.getName(), quota.getPolicy());
        switch (quota.getPolicy()) {
            case consumer_backlog_eviction:
                switch (backlogQuotaType) {
                    case destination_storage:
                        dropBacklogForSizeLimit(persistentTopic, quota);
                        topicBacklogQuotaMetrics.recordSizeBasedBacklogEviction();
                        break;
                    case message_age:
                        dropBacklogForTimeLimit(persistentTopic, quota, preciseTimeBasedBacklogQuotaCheck);
                        topicBacklogQuotaMetrics.recordTimeBasedBacklogEviction();
                        break;
                    default:
                        break;
                }
                break;
            case producer_exception:
            case producer_request_hold:
                if (!advanceSlowestSystemCursor(persistentTopic)) {
                    // The slowest is not a system cursor. Disconnecting producers to put backpressure.
                    disconnectProducers(persistentTopic);
                }
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
        ManagedLedger mLedger = persistentTopic.getManagedLedger();
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
            long currentMillis = persistentTopic.getManagedLedger().getConfig().getClock().millis();
            ManagedLedger mLedger = persistentTopic.getManagedLedger();
            try {
                for (; ; ) {
                    ManagedCursor slowestConsumer = mLedger.getSlowestConsumer();
                    Position oldestPosition = slowestConsumer.getMarkDeletedPosition();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] slowest consumer mark delete position is [{}], read position is [{}]",
                            slowestConsumer.getName(), oldestPosition, slowestConsumer.getReadPosition());
                    }
                    MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo =
                        mLedger.getLedgerInfo(oldestPosition.getLedgerId()).get();
                    if (ledgerInfo == null) {
                        long ledgerId = mLedger.getLedgersInfo().ceilingKey(oldestPosition.getLedgerId() + 1);
                        Position nextPosition = PositionFactory.create(ledgerId, -1);
                        slowestConsumer.markDelete(nextPosition);
                        continue;
                    }
                    // Timestamp only > 0 if ledger has been closed
                    if (ledgerInfo.getTimestamp() > 0
                        && currentMillis - ledgerInfo.getTimestamp() > SECONDS.toMillis(quota.getLimitTime())) {
                        // skip whole ledger for the slowest cursor
                        long ledgerId = mLedger.getLedgersInfo().ceilingKey(oldestPosition.getLedgerId() + 1);
                        Position nextPosition = PositionFactory.create(ledgerId, -1);
                        if (!nextPosition.equals(oldestPosition)) {
                            slowestConsumer.markDelete(nextPosition);
                            continue;
                        }
                    }
                    break;
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
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Map<String, Producer> producers = persistentTopic.getProducers();

        producers.values().forEach(producer -> {
            log.info("Producer [{}] has exceeded backlog quota on topic [{}]. Disconnecting producer",
                    producer.getProducerName(), persistentTopic.getName());
            futures.add(producer.disconnect());
        });

        FutureUtil.waitForAll(futures)
                .thenRun(() ->
                        log.info("All producers on topic [{}] are disconnected", persistentTopic.getName()))
                .exceptionally(exception -> {
                    log.error("Error in disconnecting producers on topic [{}] [{}]", persistentTopic.getName(),
                            exception);
                    return null;
        });
    }

    /**
     * Advances the slowest cursor if that is a system cursor.
     *
     * @param persistentTopic Persistent topic
     * @return true if the slowest cursor is a system cursor
     */
    private boolean advanceSlowestSystemCursor(PersistentTopic persistentTopic) {

        ManagedLedger mLedger = persistentTopic.getManagedLedger();
        ManagedCursor slowestConsumer = mLedger.getSlowestConsumer();
        if (slowestConsumer == null) {
            return false;
        }

        if (PersistentTopic.isDedupCursorName(slowestConsumer.getName())) {
            persistentTopic.getMessageDeduplication().takeSnapshot();
            return true;
        }

        // We may need to check other system cursors here : replicator, compaction
        return false;
    }
}
