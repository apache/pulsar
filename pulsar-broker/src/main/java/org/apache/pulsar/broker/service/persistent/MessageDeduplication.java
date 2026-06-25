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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.markDelete;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.openCursor;
import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_IS_REPL_MARKER;
import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION;
import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerReplayTask;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;

/**
 * Class that contains all the logic to control and perform the deduplication on the broker side.
 */
public class MessageDeduplication {

    private static final Logger LOG = Logger.get(MessageDeduplication.class);
    private final Logger log;

    private final PulsarService pulsar;
    private final PersistentTopic topic;
    private final ManagedLedger managedLedger;
    private final ManagedLedgerReplayTask replayTask;
    private ManagedCursor managedCursor;

    private static final String IS_LAST_CHUNK = "isLastChunk";

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

    @VisibleForTesting
    public enum MessageDupStatus {
        // whether a message is a definitely a duplicate or not cannot be determined at this time
        Unknown,
        // message is definitely NOT a duplicate
        NotDup,
        // message is definitely a duplicate
        Dup,
    }

    public static class MessageDupUnknownException extends RuntimeException {
        public MessageDupUnknownException(String topicName, String producerName) {
            super(String.format("[%s][%s]Cannot determine whether the message is a duplicate at this time", topicName,
                    producerName));
        }
    }

    @VisibleForTesting
    record ReplSourcePosition(long ledgerId, long entryId) {
    }

    private volatile Status status;
    private CompletableFuture<Void> statusChangeFuture = CompletableFuture.completedFuture(null);

    // Map that contains the highest sequenceId that have been sent by each producers. The map will be updated before
    // the messages are persisted
    @VisibleForTesting
    final Map<String, Long> highestSequencedPushed = new ConcurrentHashMap<>();

    // Map that contains the highest sequenceId that have been persistent by each producers. The map will be updated
    // after the messages are persisted
    @VisibleForTesting
    final Map<String, Long> highestSequencedPersisted = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<String, ReplSourcePosition> highestReplPositionPushed = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<String, ReplSourcePosition> highestReplPositionPersisted = new ConcurrentHashMap<>();

    // Number of persisted entries after which to store a snapshot of the sequence ids map
    private final int snapshotInterval;

    // Counter of number of entries stored after last snapshot was taken
    private int snapshotCounter;

    // The timestamp when the snapshot was taken by the scheduled task last time
    private volatile long lastSnapshotTimestamp = 0L;

    // Max number of producer for which to persist the sequence id information
    private final int maxNumberOfProducers;

    // Map used to track the inactive producer along with the timestamp of their last activity
    private final Map<String, Long> inactiveProducers = new ConcurrentHashMap<>();

    private final String replicatorPrefix;

    private static final String REPL_LEDGER_ID_SUFFIX = "_LID";
    private static final String REPL_ENTRY_ID_SUFFIX = "_EID";

    private final AtomicBoolean snapshotTaking = new AtomicBoolean(false);

    public MessageDeduplication(PulsarService pulsar, PersistentTopic topic, ManagedLedger managedLedger) {
        this.pulsar = pulsar;
        this.topic = topic;
        this.managedLedger = managedLedger;
        this.status = Status.Initialized;
        this.snapshotInterval = pulsar.getConfiguration().getBrokerDeduplicationEntriesInterval();
        this.maxNumberOfProducers = pulsar.getConfiguration().getBrokerDeduplicationMaxNumberOfProducers();
        this.snapshotCounter = 0;
        this.replicatorPrefix = pulsar.getConfiguration().getReplicatorPrefix();
        this.replayTask = new ManagedLedgerReplayTask("MessageDeduplication", pulsar.getExecutor(), 100);
        this.log = LOG.with().attr("topic", topic.getName()).build();
    }

    public Status getStatus() {
        return status;
    }

    /**
     * Check the status of deduplication. If the configuration has changed, it will enable/disable deduplication,
     * returning a future to track the completion of the task
     */
    public CompletableFuture<Void> checkStatus() {
        boolean shouldBeEnabled = topic.isDeduplicationEnabled();
        synchronized (this) {
            if (status == Status.Recovering) {
                return statusChangeFuture;
            }
            if (status == Status.Removing) {
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
                                log.info("Deleted deduplication cursor");
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                if (exception instanceof ManagedLedgerException.CursorNotFoundException) {
                                    status = Status.Disabled;
                                } else {
                            log.error()
                                    .exception(exception)
                                    .log("Deleted deduplication cursor error");
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
                                highestReplPositionPushed.clear();
                                highestReplPositionPersisted.clear();
                                future.complete(null);
                                log.info("Disabled deduplication");
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                // It's ok for disable message deduplication.
                                if (exception instanceof ManagedLedgerException.CursorNotFoundException) {
                                    status = Status.Disabled;
                                    managedCursor = null;
                                    highestSequencedPushed.clear();
                                    highestSequencedPersisted.clear();
                                    highestReplPositionPushed.clear();
                                    highestReplPositionPersisted.clear();
                                    future.complete(null);
                                } else {
                                    log.warn()
                                            .exceptionMessage(exception)
                                            .log("Failed to disable deduplication");
                                    status = Status.Failed;
                                    future.completeExceptionally(exception);
                                }
                            }
                        }, null);

                return future;
            } else if ((status == Status.Disabled || status == Status.Initialized || status == Status.Failed)
                    && shouldBeEnabled) {
                // Enable deduping
                status = Status.Recovering;
                final CompletableFuture<Void> future;
                try {
                    future = openCursor(managedLedger, PersistentTopic.DEDUPLICATION_CURSOR_NAME)
                            .thenCompose(this::replayCursor);
                } catch (Throwable e) {
                    status = Status.Failed;
                    statusChangeFuture = CompletableFuture.failedFuture(e);
                    log.error().exception(e).log("Failed to enable deduplication");
                    return statusChangeFuture;
                }
                statusChangeFuture = future.whenComplete((__, e) -> {
                    if (e != null) {
                        status = Status.Failed;
                        log.error().exception(e).log("Failed to enable deduplication");
                    }
                });
                return statusChangeFuture;
            } else {
                // Nothing to do, we are in the correct state
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    private CompletableFuture<Void> replayCursor(ManagedCursor cursor) {
        managedCursor = cursor;
        cursor.rewind();
        snapshotCounter = 0;
        highestSequencedPushed.clear();
        highestSequencedPersisted.clear();
        highestReplPositionPushed.clear();
        highestReplPositionPersisted.clear();
        inactiveProducers.clear();
        // Load the sequence ids from the snapshot in the cursor properties
        Map<String, Long> replLedgerIds = new HashMap<>();
        Map<String, Long> replEntryIds = new HashMap<>();
        managedCursor.getProperties().forEach((k, v) -> {
            // Geo-replication V2 keys are source-position watermarks, not producer lifecycle state.
            if (isReplSequenceKey(k)) {
                String baseProducerName = getBaseProducerName(k);
                if (k.endsWith(REPL_LEDGER_ID_SUFFIX)) {
                    replLedgerIds.put(baseProducerName, v);
                } else {
                    replEntryIds.put(baseProducerName, v);
                }
                return;
            }
            highestSequencedPushed.put(k, v);
            highestSequencedPersisted.put(k, v);
            producerRemoved(k);
        });
        replLedgerIds.forEach((producerName, ledgerId) -> {
            Long entryId = replEntryIds.get(producerName);
            if (entryId != null) {
                ReplSourcePosition position = new ReplSourcePosition(ledgerId, entryId);
                highestReplPositionPushed.put(producerName, position);
                highestReplPositionPersisted.put(producerName, position);
            } else {
                logIncompleteReplSourcePositionSnapshot(producerName, ledgerId, null);
            }
        });
        replEntryIds.forEach((producerName, entryId) -> {
            if (!replLedgerIds.containsKey(producerName)) {
                logIncompleteReplSourcePositionSnapshot(producerName, null, entryId);
            }
        });
        // Replay all the entries and apply all the sequence ids updates
        log.info()
                .attr("numberOfEntries", managedCursor.getNumberOfEntries())
                .log("Replaying entries for deduplication");
        return replayTask.replay(cursor, (__, buffer) -> {
            final var metadata = Commands.parseMessageMetadata(buffer);
            if (metadata.hasMarkerType() || getReplProducerName(metadata) == null) {
                recoverProducerSequenceIdFromMetadata(metadata);
            } else {
                // Rebuild replication watermarks from entries written after the last dedup snapshot.
                recoverReplWatermarkFromMetadata(metadata);
            }
        }).thenCompose(optPosition -> {
            if (optPosition.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            snapshotCounter = replayTask.getNumEntriesProcessed();
            if (snapshotCounter >= snapshotInterval) {
                return takeSnapshot(optPosition.get());
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).thenRun(() -> {
            status = Status.Enabled;
            log.info("Enabled deduplication");
        });
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
        setContextPropsIfRepl(publishContext, headersAndPayload);
        if (!isEnabled() || publishContext.isMarkerMessage()) {
            return MessageDupStatus.NotDup;
        }
        if (Producer.isRemoteOrShadow(publishContext.getProducerName(), replicatorPrefix)) {
            if (!publishContext.supportsReplDedupByLidAndEid()) {
                return isDuplicateReplV1(publishContext, headersAndPayload);
            } else {
                return isDuplicateReplV2(publishContext, headersAndPayload);
            }
        }
        return isDuplicateNormal(publishContext, headersAndPayload, false);
    }

    public MessageDupStatus isDuplicateReplV1(PublishContext publishContext, ByteBuf headersAndPayload) {
        // Message is coming from replication, we need to use the original producer name and sequence id
        // for the purpose of deduplication and not rely on the "replicator" name.
        int readerIndex = headersAndPayload.readerIndex();
        MessageMetadata md = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.readerIndex(readerIndex);

        String producerName = md.getProducerName();
        long sequenceId = md.getSequenceId();
        long highestSequenceId = Math.max(md.getHighestSequenceId(), sequenceId);
        publishContext.setOriginalProducerName(producerName);
        publishContext.setOriginalSequenceId(sequenceId);
        publishContext.setOriginalHighestSequenceId(highestSequenceId);
        return isDuplicateNormal(publishContext, headersAndPayload, true);
    }

    private void setContextPropsIfRepl(PublishContext publishContext, ByteBuf headersAndPayload) {
        // Case-1: is a replication marker.
        if (publishContext.isMarkerMessage()) {
            // Message is coming from replication, we need to use the replication's producer name, ledger id and entry
            // id for the purpose of deduplication.
            MessageMetadata md = Commands.peekMessageMetadata(headersAndPayload, "Check-Deduplicate", -1);
            if (md != null && md.hasMarkerType() && Markers.isReplicationMarker(md.getMarkerType())) {
                publishContext.setProperty(MSG_PROP_IS_REPL_MARKER, "");
            }
            return;
        }

        // Case-2: is a replicated message.
        if (Producer.isRemoteOrShadow(publishContext.getProducerName(), replicatorPrefix)) {
            // Message is coming from replication, we need to use the replication's producer name, source cluster's
            // ledger id and entry id for the purpose of deduplication.
            int readerIndex = headersAndPayload.readerIndex();
            MessageMetadata md = Commands.parseMessageMetadata(headersAndPayload);
            headersAndPayload.readerIndex(readerIndex);

            long[] position = getReplSourcePosition(md, publishContext.getProducerName());
            if (position != null) {
                publishContext.setProperty(MSG_PROP_REPL_SOURCE_POSITION, position);
            }
        }
    }

    private long[] getReplSourcePosition(MessageMetadata md, String producerName) {
        List<KeyValue> kvPairList = md.getPropertiesList();
        for (KeyValue kvPair : kvPairList) {
            if (kvPair.getKey().equals(MSG_PROP_REPL_SOURCE_POSITION)) {
                String[] ledgerIdAndEntryId = kvPair.getValue().split(":", -1);
                if (ledgerIdAndEntryId.length != 2) {
                    logUnexpectedReplSourcePosition(producerName, kvPair.getValue());
                    return null;
                }
                try {
                    long ledgerId = Long.parseLong(ledgerIdAndEntryId[0]);
                    long entryId = Long.parseLong(ledgerIdAndEntryId[1]);
                    if (ledgerId >= 0 && entryId >= 0) {
                        return new long[]{ledgerId, entryId};
                    }
                } catch (NumberFormatException e) {
                    logUnexpectedReplSourcePosition(producerName, kvPair.getValue());
                    return null;
                }
                logUnexpectedReplSourcePosition(producerName, kvPair.getValue());
                return null;
            }
        }
        return null;
    }

    private long[] getReplSourcePosition(PublishContext publishContext) {
        Object positionPairObj = publishContext.getProperty(MSG_PROP_REPL_SOURCE_POSITION);
        if (!(positionPairObj instanceof long[]) || ((long[]) positionPairObj).length < 2) {
            return null;
        }
        return (long[]) positionPairObj;
    }

    private void logInvalidReplSourcePosition(PublishContext publishContext, String message) {
        log.error()
                .attr("producerName", publishContext.getProducerName())
                .attr("supportsReplDedupByLidAndEid", publishContext.supportsReplDedupByLidAndEid())
                .attr("sequenceId", publishContext.getSequenceId())
                .attr("propKey", MSG_PROP_REPL_SOURCE_POSITION)
                .log(message);
    }

    private void recoverReplWatermark(String producerName, long ledgerId, long entryId) {
        ReplSourcePosition replSourcePosition = new ReplSourcePosition(ledgerId, entryId);
        highestReplPositionPushed.put(producerName, replSourcePosition);
        highestReplPositionPersisted.put(producerName, replSourcePosition);
    }

    private void recordReplWatermarkPersisted(String producerName, long ledgerId, long entryId) {
        highestReplPositionPersisted.put(producerName, new ReplSourcePosition(ledgerId, entryId));
    }

    private boolean isReplPositionAtOrBefore(long ledgerId, long entryId, ReplSourcePosition watermark) {
        return ledgerId < watermark.ledgerId()
                || (ledgerId == watermark.ledgerId() && entryId <= watermark.entryId());
    }

    private void logUnexpectedReplSourcePosition(String producerName, String value) {
        log.warn()
                .attr("producerName", producerName)
                .attr("MSG_PROP_REPL_SOURCE_POSITION", MSG_PROP_REPL_SOURCE_POSITION)
                .attr("value", value)
                .log("Unexpected");
    }

    private void logIncompleteReplSourcePositionSnapshot(String producerName, Long ledgerId, Long entryId) {
        log.error()
                .attr("producerName", producerName)
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .log("Incomplete geo-replication deduplication snapshot");
    }

    private void recoverProducerSequenceIdFromMetadata(MessageMetadata md) {
        final var producerName = md.getProducerName();
        final var sequenceId = Math.max(md.getHighestSequenceId(), md.getSequenceId());
        highestSequencedPushed.put(producerName, sequenceId);
        highestSequencedPersisted.put(producerName, sequenceId);
        producerRemoved(producerName);
    }

    @VisibleForTesting
    void recoverReplWatermarkFromMetadata(MessageMetadata md) {
        if (md.hasMarkerType()) {
            return;
        }
        String replProducerName = getReplProducerName(md);
        if (replProducerName != null) {
            long[] replSourcePosition = getReplSourcePosition(md, replProducerName);
            if (replSourcePosition != null) {
                recoverReplWatermark(replProducerName, replSourcePosition[0], replSourcePosition[1]);
            }
        }
    }

    @VisibleForTesting
    String getReplProducerName(MessageMetadata md) {
        final var shadowSourceTopic = topic.getShadowSourceTopic();
        if (md.hasReplicatedFrom() && shadowSourceTopic.isPresent()) {
            return ShadowReplicator.getShadowProducerName(replicatorPrefix,
                    shadowSourceTopic.get().toString(), topic.getName());
        }
        if (md.hasReplicatedFrom()) {
            return AbstractReplicator.getReplicatorName(replicatorPrefix, md.getReplicatedFrom())
                    + AbstractReplicator.REPL_PRODUCER_NAME_DELIMITER
                    + pulsar.getConfiguration().getClusterName();
        }
        if (Producer.isRemoteOrShadow(md.getProducerName(), replicatorPrefix)) {
            return md.getProducerName();
        }
        return null;
    }

    public MessageDupStatus isDuplicateReplV2(PublishContext publishContext, ByteBuf headersAndPayload) {
        long[] replSourcePosition = getReplSourcePosition(publishContext);
        if (replSourcePosition == null) {
            logInvalidReplSourcePosition(publishContext, "Message cannot determine whether it is duplicated due to "
                    + "the acquired message props being invalid, prop not in expected format");
            return MessageDupStatus.Unknown;
        }

        long replSequenceLId = replSourcePosition[0];
        long replSequenceEId = replSourcePosition[1];
        String producerName = publishContext.getProducerName();
        ReplSourcePosition lastPushed;
        ReplSourcePosition lastPersisted = null;
        MessageDupStatus status;
        synchronized (highestReplPositionPushed) {
            lastPushed = highestReplPositionPushed.get(producerName);
            if (lastPushed != null && isReplPositionAtOrBefore(replSequenceLId, replSequenceEId, lastPushed)) {
                lastPersisted = highestReplPositionPersisted.get(producerName);
                status = lastPersisted != null
                        && isReplPositionAtOrBefore(replSequenceLId, replSequenceEId, lastPersisted)
                        ? MessageDupStatus.Dup : MessageDupStatus.Unknown;
            } else {
                status = MessageDupStatus.NotDup;
                highestReplPositionPushed.put(producerName,
                        new ReplSourcePosition(replSequenceLId, replSequenceEId));
            }
        }
        log.debug()
                .attr("producerName", producerName)
                .attr("replSequenceLId", replSequenceLId)
                .attr("replSequenceEId", replSequenceEId)
                .attr("lastPushed", lastPushed)
                .attr("lastPersisted", lastPersisted)
                .attr("dupStatus", status)
                .log("Checked replicated message deduplication status");
        return status;
    }

    public MessageDupStatus isDuplicateNormal(PublishContext publishContext, ByteBuf headersAndPayload,
                                              boolean useOriginalProducerName) {
        String producerName = publishContext.getProducerName();
        if (useOriginalProducerName) {
            producerName = publishContext.getOriginalProducerName();
        }
        long sequenceId = publishContext.getSequenceId();
        long highestSequenceId = Math.max(publishContext.getHighestSequenceId(), sequenceId);
        long chunkID = -1;
        long totalChunk = -1;
        if (publishContext.isChunked()) {
            int readerIndex = headersAndPayload.readerIndex();
            MessageMetadata md = Commands.parseMessageMetadata(headersAndPayload);
            headersAndPayload.readerIndex(readerIndex);
            chunkID = md.getChunkId();
            totalChunk = md.getNumChunksFromMsg();
        }
        // All chunks of a message use the same message metadata and sequence ID,
        // so we only need to check the sequence ID for the last chunk in a chunk message.
        if (chunkID != -1 && chunkID != totalChunk - 1) {
            publishContext.setProperty(IS_LAST_CHUNK, Boolean.FALSE);
            return MessageDupStatus.NotDup;
        }
        // Synchronize the get() and subsequent put() on the map. This would only be relevant if the producer
        // disconnects and re-connects very quickly. At that point the call can be coming from a different thread
        synchronized (highestSequencedPushed) {
            Long lastSequenceIdPushed = highestSequencedPushed.get(producerName);
            if (lastSequenceIdPushed != null && sequenceId <= lastSequenceIdPushed) {
                log.debug()
                        .attr("producerName", producerName)
                        .attr("sequenceId", sequenceId)
                        .attr("lastSequenceIdPushed", lastSequenceIdPushed)
                        .log("Message identified as duplicated");

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
        // Only put sequence ID into highestSequencedPushed and
        // highestSequencedPersisted until receive and persistent the last chunk.
        if (chunkID != -1 && chunkID == totalChunk - 1) {
            publishContext.setProperty(IS_LAST_CHUNK, Boolean.TRUE);
        }
        return MessageDupStatus.NotDup;
    }

    /**
     * Call this method whenever a message is persisted to get the chance to trigger a snapshot.
     */
    public void recordMessagePersisted(PublishContext publishContext, Position position) {
        if (!isEnabled() || publishContext.isMarkerMessage()) {
            return;
        }
        if (publishContext.getProducerName().startsWith(replicatorPrefix)
                && publishContext.supportsReplDedupByLidAndEid()) {
            recordMessagePersistedRepl(publishContext, position);
        } else {
            recordMessagePersistedNormal(publishContext, position);
        }
    }

    public void recordMessagePersistedRepl(PublishContext publishContext, Position position) {
        long[] replSourcePosition = getReplSourcePosition(publishContext);
        if (replSourcePosition == null) {
            logInvalidReplSourcePosition(publishContext, "Can not persist highest sequence-id due to the acquired "
                    + "message props being invalid, prop not in expected format");
            recordMessagePersistedNormal(publishContext, position);
            return;
        }
        recordReplWatermarkPersisted(publishContext.getProducerName(), replSourcePosition[0], replSourcePosition[1]);
        increaseSnapshotCounterAndTakeSnapshotIfNeeded(position);
    }

    public void recordMessagePersistedNormal(PublishContext publishContext, Position position) {
        String producerName = publishContext.getProducerName();
        long sequenceId = publishContext.getSequenceId();
        long highestSequenceId = publishContext.getHighestSequenceId();
        if (publishContext.getOriginalProducerName() != null) {
            // In case of replicated messages, this will be different from the current replicator producer name
            producerName = publishContext.getOriginalProducerName();
            sequenceId = publishContext.getOriginalSequenceId();
            highestSequenceId = publishContext.getOriginalHighestSequenceId();
        }
        Boolean isLastChunk = (Boolean) publishContext.getProperty(IS_LAST_CHUNK);
        if (isLastChunk == null || isLastChunk) {
            highestSequencedPersisted.put(producerName, Math.max(highestSequenceId, sequenceId));
        }
        increaseSnapshotCounterAndTakeSnapshotIfNeeded(position);
    }

    private void increaseSnapshotCounterAndTakeSnapshotIfNeeded(Position position) {
        if (++snapshotCounter >= snapshotInterval) {
            snapshotCounter = 0;
            takeSnapshot(position);
        } else {
            log.debug()
                    .attr("snapshotCounter", snapshotCounter)
                    .attr("snapshotInterval", snapshotInterval)
                    .log("Waiting for sequence-id snapshot");
        }
    }

    public void resetHighestSequenceIdPushed() {
        if (!isEnabled()) {
            return;
        }

        highestSequencedPushed.clear();
        for (String producer : highestSequencedPersisted.keySet()) {
            highestSequencedPushed.put(producer, highestSequencedPersisted.get(producer));
        }
        highestReplPositionPushed.clear();
        highestReplPositionPushed.putAll(highestReplPositionPersisted);
    }

    private CompletableFuture<Void> takeSnapshot(Position position) {
        log.debug("Taking snapshot of sequence ids map");

        if (!snapshotTaking.compareAndSet(false, true)) {
            log.warn()
                    .attr("position", position)
                    .log("There is a pending snapshot when taking snapshot for");
            return CompletableFuture.completedFuture(null);
        }

        Map<String, Long> snapshot = new TreeMap<>();
        highestReplPositionPersisted.forEach((producerName, replSourcePosition) -> {
            snapshot.put(producerName + REPL_LEDGER_ID_SUFFIX, replSourcePosition.ledgerId());
            snapshot.put(producerName + REPL_ENTRY_ID_SUFFIX, replSourcePosition.entryId());
        });
        int normalProducerCount = 0;
        for (Map.Entry<String, Long> entry : highestSequencedPersisted.entrySet()) {
            String producerName = entry.getKey();
            if (isReplSequenceKey(producerName)) {
                continue;
            }
            if (normalProducerCount >= maxNumberOfProducers) {
                break;
            }
            snapshot.put(producerName, entry.getValue());
            normalProducerCount++;
        }

        final var cursor = managedCursor;
        if (cursor == null) {
            log.warn()
                    .attr("position", position)
                    .log("Cursor is null when taking snapshot for");
            return CompletableFuture.completedFuture(null);
        }
        final var future = markDelete(cursor, position, snapshot).thenRun(() -> {
            log.debug()
                    .attr("position", position)
                    .log("Stored new deduplication snapshot at");
            lastSnapshotTimestamp = System.currentTimeMillis();
            snapshotTaking.set(false);
        });
        future.exceptionally(e -> {
            log.warn()
                    .attr("position", position)
                    .exception(e)
                    .log("Failed to store new deduplication snapshot at");
            snapshotTaking.set(false);
            return null;
        });
        return future;
    }

    /**
     * Topic will call this method whenever a producer connects.
     */
    public void producerAdded(String producerName) {
        if (!isEnabled()) {
            return;
        }

        // Producer is no-longer inactive
        inactiveProducers.remove(producerName);
    }

    /**
     * Topic will call this method whenever a producer disconnects.
     */
    public void producerRemoved(String producerName) {
        if (!isEnabled()) {
            return;
        }

        // Producer is no-longer active
        inactiveProducers.put(getBaseProducerName(producerName), System.currentTimeMillis());
    }

    /**
     * Remove from hash maps all the producers that were inactive for more than the configured amount of time.
     */
    public synchronized void purgeInactiveProducers() {
        long minimumActiveTimestamp = System.currentTimeMillis() - TimeUnit.MINUTES
                .toMillis(pulsar.getConfiguration().getBrokerDeduplicationProducerInactivityTimeoutMinutes());

        // if not enabled just clear all inactive producer record.
        if (!isEnabled()) {
            if (!inactiveProducers.isEmpty()) {
                inactiveProducers.clear();
            }
            return;
        }

        Iterator<Map.Entry<String, Long>> mapIterator = inactiveProducers.entrySet().iterator();
        boolean hasInactive = false;
        while (mapIterator.hasNext()) {
            java.util.Map.Entry<String, Long> entry = mapIterator.next();
            String producerName = entry.getKey();
            long lastActiveTimestamp = entry.getValue();

            if (lastActiveTimestamp < minimumActiveTimestamp) {
                mapIterator.remove();
                if (Producer.isRemoteOrShadow(producerName, replicatorPrefix)) {
                    // Keep the geo-replication watermark; the source can replay this producer after failover.
                    log.info()
                            .attr("producerName", producerName)
                            .log("Keeping geo-replication deduplication state for inactive producer");
                } else {
                    log.info()
                            .attr("producerName", producerName)
                            .log("Purging dedup information for producer");
                    highestSequencedPushed.remove(producerName);
                    highestSequencedPersisted.remove(producerName);
                    hasInactive = true;
                }
            }
        }
        if (hasInactive && isEnabled()) {
            takeSnapshot(getManagedCursor().getMarkDeletedPosition());
        }
    }

    private String getBaseProducerName(String producerName) {
        if (Producer.isRemoteOrShadow(producerName, replicatorPrefix)) {
            if (producerName.endsWith(REPL_LEDGER_ID_SUFFIX)) {
                return producerName.substring(0, producerName.length() - REPL_LEDGER_ID_SUFFIX.length());
            } else if (producerName.endsWith(REPL_ENTRY_ID_SUFFIX)) {
                return producerName.substring(0, producerName.length() - REPL_ENTRY_ID_SUFFIX.length());
            }
        }
        return producerName;
    }

    private boolean isReplSequenceKey(String producerName) {
        return Producer.isRemoteOrShadow(producerName, replicatorPrefix)
                && (producerName.endsWith(REPL_LEDGER_ID_SUFFIX) || producerName.endsWith(REPL_ENTRY_ID_SUFFIX));
    }

    public long getLastPublishedSequenceId(String producerName) {
        Long sequenceId = highestSequencedPushed.get(producerName);
        return sequenceId != null ? sequenceId : -1;
    }

    public void takeSnapshot() {
        if (!isEnabled()) {
            return;
        }

        Integer interval = topic.getHierarchyTopicPolicies().getDeduplicationSnapshotIntervalSeconds().get();
        long currentTimeStamp = System.currentTimeMillis();
        if (interval == null || interval <= 0
                || currentTimeStamp - lastSnapshotTimestamp < TimeUnit.SECONDS.toMillis(interval)) {
            return;
        }
        Position position = managedLedger.getLastConfirmedEntry();
        if (position == null) {
            return;
        }
        Position markDeletedPosition = managedCursor.getMarkDeletedPosition();
        if (markDeletedPosition != null && position.compareTo(markDeletedPosition) <= 0) {
            return;
        }
        takeSnapshot(position);
    }

    @VisibleForTesting
    ManagedCursor getManagedCursor() {
        return managedCursor;
    }

    @VisibleForTesting
    void setManagedCursor(ManagedCursor managedCursor) {
        this.managedCursor = managedCursor;
    }

    @VisibleForTesting
    Map<String, Long> getInactiveProducers() {
        return inactiveProducers;
    }
}
