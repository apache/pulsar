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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that contains all the logic to control and perform the deduplication on the broker side.
 */
public class MessageDeduplication {

    private final PulsarService pulsar;
    private final PersistentTopic topic;
    private final ManagedLedger managedLedger;
    private ManagedCursor managedCursor;

    enum Status {

        // Deduplication is initialized
        Initialized,

        // Deduplication is disabled
        Disabled,

        // Initializing deduplication state
        Recovering,

        // Deduplication is in effect
        Enabled,

        // Turning off deduplication
        Removing,

        // Failed to enable/disable
        Failed,
    }

    enum MessageDupStatus {
        // whether a message is a definitely a duplicate or not cannot be determined at this time
        Unknown,
        // message is definitely NOT a duplicate
        NotDup,
        // message is definitely a duplicate
        Dup,
    }

    public static class MessageDupUnknownException extends RuntimeException {
        public MessageDupUnknownException() {
            super("Cannot determine whether the message is a duplicate at this time");
        }
    }


    private volatile Status status;

    // Map that contains the highest sequenceId that have been sent by each producers. The map will be updated before
    // the messages are persisted
    @VisibleForTesting
    final ConcurrentOpenHashMap<String, Long> highestSequencedPushed = new ConcurrentOpenHashMap<>(16, 1);

    // Map that contains the highest sequenceId that have been persistent by each producers. The map will be updated
    // after the messages are persisted
    @VisibleForTesting
    final ConcurrentOpenHashMap<String, Long> highestSequencedPersisted = new ConcurrentOpenHashMap<>(16, 1);

    // Number of persisted entries after which to store a snapshot of the sequence ids map
    private final int snapshotInterval;

    // Counter of number of entries stored after last snapshot was taken
    private int snapshotCounter;

    // The timestamp when the snapshot was taken by the scheduled task last time
    private volatile long lastSnapshotTimestamp = 0L;

    // Max number of producer for which to persist the sequence id information
    private final int maxNumberOfProducers;

    // Map used to track the inactive producer along with the timestamp of their last activity
    private final Map<String, Long> inactiveProducers = new HashMap<>();

    private final String replicatorPrefix;

    public MessageDeduplication(PulsarService pulsar, PersistentTopic topic, ManagedLedger managedLedger) {
        this.pulsar = pulsar;
        this.topic = topic;
        this.managedLedger = managedLedger;
        this.status = Status.Initialized;
        this.snapshotInterval = pulsar.getConfiguration().getBrokerDeduplicationEntriesInterval();
        this.maxNumberOfProducers = pulsar.getConfiguration().getBrokerDeduplicationMaxNumberOfProducers();
        this.snapshotCounter = 0;
        this.replicatorPrefix = pulsar.getConfiguration().getReplicatorPrefix();
    }

    private CompletableFuture<Void> recoverSequenceIdsMap() {
        // Load the sequence ids from the snapshot in the cursor properties
        managedCursor.getProperties().forEach((k, v) -> {
            producerRemoved(k);
            highestSequencedPushed.put(k, v);
            highestSequencedPersisted.put(k, v);
        });

        // Replay all the entries and apply all the sequence ids updates
        log.info("[{}] Replaying {} entries for deduplication", topic.getName(), managedCursor.getNumberOfEntries());
        CompletableFuture<Void> future = new CompletableFuture<>();
        replayCursor(future);
        return future;
    }

    /**
     * Read all the entries published from the cursor position until the most recent and update the highest sequence id
     * from each producer.
     *
     * @param future future to trigger when the replay is complete
     */
    private void replayCursor(CompletableFuture<Void> future) {
        managedCursor.asyncReadEntries(100, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {

                for (Entry entry : entries) {
                    ByteBuf messageMetadataAndPayload = entry.getDataBuffer();
                    MessageMetadata md = Commands.parseMessageMetadata(messageMetadataAndPayload);

                    String producerName = md.getProducerName();
                    long sequenceId = Math.max(md.getHighestSequenceId(), md.getSequenceId());
                    highestSequencedPushed.put(producerName, sequenceId);
                    highestSequencedPersisted.put(producerName, sequenceId);
                    producerRemoved(producerName);

                    entry.release();
                }

                if (managedCursor.hasMoreEntries()) {
                    // Read next batch of entries
                    pulsar.getExecutor().execute(() -> replayCursor(future));
                } else {
                    // Done replaying
                    future.complete(null);
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, PositionImpl.latest);
    }

    public Status getStatus() {
        return status;
    }

    /**
     * Check the status of deduplication. If the configuration has changed, it will enable/disable deduplication,
     * returning a future to track the completion of the task
     */
    public CompletableFuture<Void> checkStatus() {
        boolean shouldBeEnabled = isDeduplicationEnabled();
        synchronized (this) {
            if (status == Status.Recovering || status == Status.Removing) {
                // If there's already a transition happening, check later for status
                pulsar.getExecutor().schedule(this::checkStatus, 1, TimeUnit.MINUTES);
                return CompletableFuture.completedFuture(null);
            }
            if (status == Status.Initialized && !shouldBeEnabled) {
                status = Status.Removing;
                managedLedger.asyncDeleteCursor(PersistentTopic.DEDUPLICATION_CURSOR_NAME,
                        new DeleteCursorCallback() {
                            @Override
                            public void deleteCursorComplete(Object ctx) {
                                status = Status.Disabled;
                                log.info("[{}] Deleted deduplication cursor", topic.getName());
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                if (exception instanceof ManagedLedgerException.CursorNotFoundException) {
                                    status = Status.Disabled;
                                } else {
                            log.error("[{}] Deleted deduplication cursor error", topic.getName(), exception);
                        }
                    }
                }, null);
            }

            if (status == Status.Enabled && !shouldBeEnabled) {
                // Disabled deduping
                CompletableFuture<Void> future = new CompletableFuture<>();
                status = Status.Removing;
                managedLedger.asyncDeleteCursor(PersistentTopic.DEDUPLICATION_CURSOR_NAME,
                        new DeleteCursorCallback() {

                            @Override
                            public void deleteCursorComplete(Object ctx) {
                                status = Status.Disabled;
                                managedCursor = null;
                                highestSequencedPushed.clear();
                                highestSequencedPersisted.clear();
                                future.complete(null);
                                log.info("[{}] Disabled deduplication", topic.getName());
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                // It's ok for disable message deduplication.
                                if (exception instanceof ManagedLedgerException.CursorNotFoundException) {
                                    status = Status.Disabled;
                                    managedCursor = null;
                                    highestSequencedPushed.clear();
                                    highestSequencedPersisted.clear();
                                    future.complete(null);
                                } else {
                                    log.warn("[{}] Failed to disable deduplication: {}", topic.getName(),
                                            exception.getMessage());
                                    status = Status.Failed;
                                    future.completeExceptionally(exception);
                                }
                            }
                        }, null);

                return future;
            } else if ((status == Status.Disabled || status == Status.Initialized) && shouldBeEnabled) {
                // Enable deduping
                CompletableFuture<Void> future = new CompletableFuture<>();
                managedLedger.asyncOpenCursor(PersistentTopic.DEDUPLICATION_CURSOR_NAME, new OpenCursorCallback() {

                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        // We don't want to retain cache for this cursor
                        cursor.setAlwaysInactive();
                        managedCursor = cursor;
                        recoverSequenceIdsMap().thenRun(() -> {
                            status = Status.Enabled;
                            future.complete(null);
                            log.info("[{}] Enabled deduplication", topic.getName());
                        }).exceptionally(ex -> {
                            status = Status.Failed;
                            log.warn("[{}] Failed to enable deduplication: {}", topic.getName(), ex.getMessage());
                            future.completeExceptionally(ex);
                            return null;
                        });
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}] Failed to enable deduplication: {}", topic.getName(),
                                exception.getMessage());
                        future.completeExceptionally(exception);
                    }

                }, null);
                return future;
            } else {
                // Nothing to do, we are in the correct state
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    public boolean isEnabled() {
        return status == Status.Enabled;
    }

    /**
     * Assess whether the message was already stored in the topic.
     *
     * @return true if the message should be published or false if it was recognized as a duplicate
     */
    public MessageDupStatus isDuplicate(PublishContext publishContext, ByteBuf headersAndPayload) {
        if (!isEnabled()) {
            return MessageDupStatus.NotDup;
        }

        String producerName = publishContext.getProducerName();
        long sequenceId = publishContext.getSequenceId();
        long highestSequenceId = Math.max(publishContext.getHighestSequenceId(), sequenceId);
        if (producerName.startsWith(replicatorPrefix)) {
            // Message is coming from replication, we need to use the original producer name and sequence id
            // for the purpose of deduplication and not rely on the "replicator" name.
            int readerIndex = headersAndPayload.readerIndex();
            MessageMetadata md = Commands.parseMessageMetadata(headersAndPayload);
            producerName = md.getProducerName();
            sequenceId = md.getSequenceId();
            highestSequenceId = Math.max(md.getHighestSequenceId(), sequenceId);
            publishContext.setOriginalProducerName(producerName);
            publishContext.setOriginalSequenceId(sequenceId);
            publishContext.setOriginalHighestSequenceId(highestSequenceId);
            headersAndPayload.readerIndex(readerIndex);
        }

        // Synchronize the get() and subsequent put() on the map. This would only be relevant if the producer
        // disconnects and re-connects very quickly. At that point the call can be coming from a different thread
        synchronized (highestSequencedPushed) {
            Long lastSequenceIdPushed = highestSequencedPushed.get(producerName);
            if (lastSequenceIdPushed != null && sequenceId <= lastSequenceIdPushed) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Message identified as duplicated producer={} seq-id={} -- highest-seq-id={}",
                            topic.getName(), producerName, sequenceId, lastSequenceIdPushed);
                }

                // Also need to check sequence ids that has been persisted.
                // If current message's seq id is smaller or equals to the
                // lastSequenceIdPersisted than its definitely a dup
                // If current message's seq id is between lastSequenceIdPersisted and
                // lastSequenceIdPushed, then we cannot be sure whether the message is a dup or not
                // we should return an error to the producer for the latter case so that it can retry at a future time
                Long lastSequenceIdPersisted = highestSequencedPersisted.get(producerName);
                if (lastSequenceIdPersisted != null && sequenceId <= lastSequenceIdPersisted) {
                    return MessageDupStatus.Dup;
                } else {
                    return MessageDupStatus.Unknown;
                }
            }
            highestSequencedPushed.put(producerName, highestSequenceId);
        }
        return MessageDupStatus.NotDup;
    }

    /**
     * Call this method whenever a message is persisted to get the chance to trigger a snapshot.
     */
    public void recordMessagePersisted(PublishContext publishContext, PositionImpl position) {
        if (!isEnabled()) {
            return;
        }

        String producerName = publishContext.getProducerName();
        long sequenceId = publishContext.getSequenceId();
        long highestSequenceId = publishContext.getHighestSequenceId();
        if (publishContext.getOriginalProducerName() != null) {
            // In case of replicated messages, this will be different from the current replicator producer name
            producerName = publishContext.getOriginalProducerName();
            sequenceId = publishContext.getOriginalSequenceId();
            highestSequenceId = publishContext.getOriginalHighestSequenceId();
        }

        highestSequencedPersisted.put(producerName, Math.max(highestSequenceId, sequenceId));
        if (++snapshotCounter >= snapshotInterval) {
            snapshotCounter = 0;
            takeSnapshot(position);
        }
    }

    public void resetHighestSequenceIdPushed() {
        if (!isEnabled()) {
            return;
        }

        highestSequencedPushed.clear();
        for (String producer : highestSequencedPersisted.keys()) {
            highestSequencedPushed.put(producer, highestSequencedPersisted.get(producer));
        }
    }

    private void takeSnapshot(PositionImpl position) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Taking snapshot of sequence ids map", topic.getName());
        }
        Map<String, Long> snapshot = new TreeMap<>();
        highestSequencedPersisted.forEach((producerName, sequenceId) -> {
            if (snapshot.size() < maxNumberOfProducers) {
                snapshot.put(producerName, sequenceId);
            }
        });

        managedCursor.asyncMarkDelete(position, snapshot, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Stored new deduplication snapshot at {}", topic.getName(), position);
                }
                lastSnapshotTimestamp = System.currentTimeMillis();
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to store new deduplication snapshot at {}", topic.getName(), position);
            }
        }, null);
    }

    private boolean isDeduplicationEnabled() {
        return topic.getHierarchyTopicPolicies().getDeduplicationEnabled().get();
    }

    /**
     * Topic will call this method whenever a producer connects.
     */
    public synchronized void producerAdded(String producerName) {
        // Producer is no-longer inactive
        inactiveProducers.remove(producerName);
    }

    /**
     * Topic will call this method whenever a producer disconnects.
     */
    public synchronized void producerRemoved(String producerName) {
        // Producer is no-longer active
        inactiveProducers.put(producerName, System.currentTimeMillis());
    }

    /**
     * Remove from hash maps all the producers that were inactive for more than the configured amount of time.
     */
    public synchronized void purgeInactiveProducers() {
        long minimumActiveTimestamp = System.currentTimeMillis() - TimeUnit.MINUTES
                .toMillis(pulsar.getConfiguration().getBrokerDeduplicationProducerInactivityTimeoutMinutes());

        Iterator<java.util.Map.Entry<String, Long>> mapIterator = inactiveProducers.entrySet().iterator();
        while (mapIterator.hasNext()) {
            java.util.Map.Entry<String, Long> entry = mapIterator.next();
            String producerName = entry.getKey();
            long lastActiveTimestamp = entry.getValue();

            mapIterator.remove();

            if (lastActiveTimestamp < minimumActiveTimestamp) {
                log.info("[{}] Purging dedup information for producer {}", topic.getName(), producerName);
                highestSequencedPushed.remove(producerName);
                highestSequencedPersisted.remove(producerName);
            }
        }
    }

    public long getLastPublishedSequenceId(String producerName) {
        Long sequenceId = highestSequencedPushed.get(producerName);
        return sequenceId != null ? sequenceId : -1;
    }

    public void takeSnapshot() {
        // try to get topic-level policies
        Integer interval = topic.getTopicPolicies()
                .map(TopicPolicies::getDeduplicationSnapshotIntervalSeconds)
                .orElse(null);
        try {
            //if topic-level policies not exists, try to get namespace-level policies
            if (interval == null) {
                final Optional<Policies> policies = pulsar.getPulsarResources().getNamespaceResources()
                        .getPolicies(TopicName.get(topic.getName()).getNamespaceObject());
                if (policies.isPresent()) {
                    interval = policies.get().deduplicationSnapshotIntervalSeconds;
                }
            }
        } catch (Exception e) {
            log.error("Failed to get namespace policies", e);
        }
        //There is no other level of policies, use the broker-level by default
        if (interval == null) {
            interval = pulsar.getConfiguration().getBrokerDeduplicationSnapshotIntervalSeconds();
        }
        long currentTimeStamp = System.currentTimeMillis();
        if (interval == null || interval <= 0
                || currentTimeStamp - lastSnapshotTimestamp < TimeUnit.SECONDS.toMillis(interval)) {
            return;
        }
        PositionImpl position = (PositionImpl) managedLedger.getLastConfirmedEntry();
        if (position == null) {
            return;
        }
        PositionImpl markDeletedPosition = (PositionImpl) managedCursor.getMarkDeletedPosition();
        if (markDeletedPosition != null && position.compareTo(markDeletedPosition) <= 0) {
            return;
        }
        takeSnapshot(position);
    }

    @VisibleForTesting
    ManagedCursor getManagedCursor() {
        return managedCursor;
    }

    private static final Logger log = LoggerFactory.getLogger(MessageDeduplication.class);
}
