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

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.DrainingHashesTracker;
import org.apache.pulsar.broker.service.EntryAndMetadata;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.ImpactedConsumersResult;
import org.apache.pulsar.broker.service.PendingAcksMap;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyDispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers implements
        StickyKeyDispatcher {

    private final boolean allowOutOfOrderDelivery;
    private final StickyKeyConsumerSelector selector;
    private final boolean drainingHashesRequired;

    private boolean skipNextReplayToTriggerLookAhead = false;
    private final KeySharedMode keySharedMode;
    @Getter
    private final DrainingHashesTracker drainingHashesTracker;

    private final RescheduleReadHandler rescheduleReadHandler;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        super(topic, cursor, subscription, ksm.isAllowOutOfOrderDelivery());

        this.allowOutOfOrderDelivery = ksm.isAllowOutOfOrderDelivery();
        this.keySharedMode = ksm.getKeySharedMode();
        // recent joined consumer tracking is required only for AUTO_SPLIT mode when out-of-order delivery is disabled
        this.drainingHashesRequired =
                keySharedMode == KeySharedMode.AUTO_SPLIT && !allowOutOfOrderDelivery;
        this.drainingHashesTracker =
                drainingHashesRequired ? new DrainingHashesTracker(this.getName(), this::stickyKeyHashUnblocked) : null;
        this.rescheduleReadHandler = new RescheduleReadHandler(conf::getKeySharedUnblockingIntervalMs,
                topic.getBrokerService().executor(), this::cancelPendingRead, () -> reScheduleReadInMs(0),
                () -> havePendingRead, this::getReadMoreEntriesCallCount, () -> !redeliveryMessages.isEmpty());
        switch (this.keySharedMode) {
        case AUTO_SPLIT:
            if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                selector = new ConsistentHashingStickyKeyConsumerSelector(
                        conf.getSubscriptionKeySharedConsistentHashingReplicaPoints(), drainingHashesRequired);
            } else {
                selector = new HashRangeAutoSplitStickyKeyConsumerSelector(drainingHashesRequired);
            }
            break;
        case STICKY:
            this.selector = new HashRangeExclusiveStickyKeyConsumerSelector();
            break;
        default:
            throw new IllegalArgumentException("Invalid key-shared mode: " + keySharedMode);
        }
    }

    private void stickyKeyHashUnblocked(int stickyKeyHash) {
        if (log.isDebugEnabled()) {
            if (stickyKeyHash > -1) {
                log.debug("[{}] Sticky key hash {} is unblocked", getName(), stickyKeyHash);
            } else {
                log.debug("[{}] Some sticky key hashes are unblocked", getName());
            }
        }
        reScheduleReadWithKeySharedUnblockingInterval();
    }

    private void reScheduleReadWithKeySharedUnblockingInterval() {
        rescheduleReadHandler.rescheduleRead();
    }

    @VisibleForTesting
    public StickyKeyConsumerSelector getSelector() {
        return selector;
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
            consumer.disconnect();
            return CompletableFuture.completedFuture(null);
        }
        return super.addConsumer(consumer).thenCompose(__ -> selector.addConsumer(consumer))
                .thenAccept(impactedConsumers -> {
            // TODO: Add some way to prevent changes in between the time the consumer is added and the
            // time the draining hashes are applied. It might be fine for ConsistentHashingStickyKeyConsumerSelector
            // since it's not really asynchronous, although it returns a CompletableFuture
            if (drainingHashesRequired) {
                consumer.setPendingAcksAddHandler(this::handleAddingPendingAck);
                consumer.setPendingAcksRemoveHandler(new PendingAcksMap.PendingAcksRemoveHandler() {
                    @Override
                    public void handleRemoving(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash,
                                               boolean closing) {
                        drainingHashesTracker.reduceRefCount(consumer, stickyKeyHash, closing);
                    }

                    @Override
                    public void startBatch() {
                        drainingHashesTracker.startBatch();
                    }

                    @Override
                    public void endBatch() {
                        drainingHashesTracker.endBatch();
                    }
                });
                consumer.setDrainingHashesConsumerStatsUpdater(drainingHashesTracker::updateConsumerStats);
                registerDrainingHashes(consumer, impactedConsumers.orElseThrow());
            }
        }).exceptionally(ex -> {
            internalRemoveConsumer(consumer);
            throw FutureUtil.wrapToCompletionException(ex);
        });
    }

    private synchronized void registerDrainingHashes(Consumer skipConsumer,
                                                     ImpactedConsumersResult impactedConsumers) {
        impactedConsumers.processRemovedHashRanges((c, removedHashRanges) -> {
            if (c != skipConsumer) {
                c.getPendingAcks().forEach((ledgerId, entryId, batchSize, stickyKeyHash) -> {
                    if (stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
                        log.warn("[{}] Sticky key hash was missing for {}:{}", getName(), ledgerId, entryId);
                        return;
                    }
                    if (removedHashRanges.containsStickyKey(stickyKeyHash)) {
                        // add the pending ack to the draining hashes tracker if the hash is in the range
                        drainingHashesTracker.addEntry(c, stickyKeyHash);
                    }
                });
            }
        });
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // The consumer must be removed from the selector before calling the superclass removeConsumer method.
        Optional<ImpactedConsumersResult> impactedConsumers = selector.removeConsumer(consumer);
        super.removeConsumer(consumer);
        if (drainingHashesRequired) {
            // register draining hashes for the impacted consumers and ranges, in case a hash switched from one
            // consumer to another. This will handle the case where a hash gets switched from an existing
            // consumer to another existing consumer during removal.
            registerDrainingHashes(consumer, impactedConsumers.orElseThrow());
            drainingHashesTracker.consumerRemoved(consumer);
        }
    }

    @Override
    protected synchronized void clearComponentsAfterRemovedAllConsumers() {
        super.clearComponentsAfterRemovedAllConsumers();
        if (drainingHashesRequired) {
            drainingHashesTracker.clear();
        }
    }

    @Override
    protected synchronized boolean trySendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        lastNumberOfEntriesProcessed = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalEntries = 0;
        long totalEntriesProcessed = 0;
        int entriesCount = entries.size();

        // Trigger read more messages
        if (entriesCount == 0) {
            return true;
        }

        if (consumerSet.isEmpty()) {
            entries.forEach(Entry::release);
            cursor.rewind();
            return false;
        }

        if (!allowOutOfOrderDelivery) {
            // A corner case that we have to retry a readMoreEntries in order to preserver order delivery.
            // This may happen when consumer closed. See issue #12885 for details.
            Optional<Position> firstReplayPosition = getFirstPositionInReplay();
            if (firstReplayPosition.isPresent()) {
                Position replayPosition = firstReplayPosition.get();
                if (this.minReplayedPosition != null) {
                    // If relayPosition is a new entry wither smaller position is inserted for redelivery during this
                    // async read, it is possible that this relayPosition should dispatch to consumer first. So in
                    // order to preserver order delivery, we need to discard this read result, and try to trigger a
                    // replay read, that containing "relayPosition", by calling readMoreEntries.
                    if (replayPosition.compareTo(minReplayedPosition) < 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Position {} (<{}) is inserted for relay during current {} read, "
                                            + "discard this read and retry with readMoreEntries.",
                                    name, replayPosition, minReplayedPosition, readType);
                        }
                        if (readType == ReadType.Normal) {
                            entries.forEach(this::addEntryToReplay);
                        } else if (readType == ReadType.Replay) {
                            entries.forEach(Entry::release);
                        }
                        skipNextBackoff = true;
                        return true;
                    }
                }
            }
        }

        // returns a boolean indicating whether look-ahead could be useful, when there's a consumer
        // with available permits, and it's not able to make progress because of blocked hashes.
        MutableBoolean triggerLookAhead = new MutableBoolean();
        // filter and group the entries by consumer for dispatching
        final Map<Consumer, List<Entry>> entriesByConsumerForDispatching =
                filterAndGroupEntriesForDispatching(entries, readType, triggerLookAhead);

        AtomicInteger remainingConsumersToFinishSending = new AtomicInteger(entriesByConsumerForDispatching.size());
        for (Map.Entry<Consumer, List<Entry>> current : entriesByConsumerForDispatching.entrySet()) {
            Consumer consumer = current.getKey();
            List<Entry> entriesForConsumer = current.getValue();
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer.consumerName(), entriesForConsumer.size(), readType);
            }
            // remove positions first from replay list first : sendMessages recycles entries
            if (readType == ReadType.Replay) {
                for (Entry entry : entriesForConsumer) {
                    redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                }
            }

            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForConsumer.size());
            EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForConsumer.size());
            totalEntries += filterEntriesForConsumer(entriesForConsumer, batchSizes, sendMessageInfo,
                    batchIndexesAcks, cursor, readType == ReadType.Replay, consumer);
            totalEntriesProcessed += entriesForConsumer.size();
            consumer.sendMessages(entriesForConsumer, batchSizes, batchIndexesAcks,
                    sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                    getRedeliveryTracker()).addListener(future -> {
                if (future.isDone() && remainingConsumersToFinishSending.decrementAndGet() == 0) {
                    readMoreEntriesAsync();
                }
            });

            TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this,
                    -(sendMessageInfo.getTotalMessages() - batchIndexesAcks.getTotalAckedIndexCount()));
            totalMessagesSent += sendMessageInfo.getTotalMessages();
            totalBytesSent += sendMessageInfo.getTotalBytes();
        }


        lastNumberOfEntriesProcessed = (int) totalEntriesProcessed;

        // acquire message-dispatch permits for already delivered messages
        acquirePermitsForDeliveredMessages(topic, cursor, totalEntries, totalMessagesSent, totalBytesSent);

        // trigger read more messages if necessary
        if (triggerLookAhead.booleanValue()) {
            // When all messages get filtered and no messages are sent, we should read more entries, "look ahead"
            // so that a possible next batch of messages might contain messages that can be dispatched.
            // This is done only when there's a consumer with available permits, and it's not able to make progress
            // because of blocked hashes. Without this rule we would be looking ahead in the stream while the
            // new consumers are not ready to accept the new messages,
            // therefore would be most likely only increase the distance between read-position and mark-delete position.
            skipNextReplayToTriggerLookAhead = true;
            // skip backoff delay before reading ahead in the "look ahead" mode to prevent any additional latency
            // only skip the delay if there are more entries to read
            skipNextBackoff = cursor.hasMoreEntries();
            return true;
        }

        // if no messages were sent to consumers, we should retry
        if (totalEntries == 0) {
            return true;
        }

        return false;
    }

    /**
     * Check if the sticky hash is already draining or blocked in the replay queue.
     * If it is, add the message to replay and return false so that the message isn't sent to a consumer.
     *
     * @param ledgerId the ledger id of the message
     * @param entryId the entry id of the message
     * @param stickyKeyHash the sticky hash of the message
     * @return true if the message should be added to pending acks and allow sending, false otherwise
     */
    private boolean handleAddingPendingAck(Consumer consumer, long ledgerId, long entryId, int stickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
            log.warn("[{}] Sticky key hash is missing for {}:{}", getName(), ledgerId, entryId);
            throw new IllegalArgumentException("Sticky key hash is missing for " + ledgerId + ":" + entryId);
        }
        DrainingHashesTracker.DrainingHashEntry drainingHashEntry = drainingHashesTracker.getEntry(stickyKeyHash);
        if (drainingHashEntry != null && drainingHashEntry.getConsumer() != consumer) {
            log.warn("[{}] Another consumer id {} is already draining hash {}. Skipping adding {}:{} to pending acks "
                            + "for consumer {}. Adding the message to replay.",
                    getName(), drainingHashEntry.getConsumer(), stickyKeyHash, ledgerId, entryId, consumer);
            addMessageToReplay(ledgerId, entryId, stickyKeyHash);
            // block message from sending
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}] Adding {}:{} to pending acks for consumer id:{} name:{} with sticky key hash {}",
                    getName(), ledgerId, entryId, consumer.consumerId(), consumer.consumerName(), stickyKeyHash);
        }
        // allow adding the message to pending acks and sending the message to the consumer
        return true;
    }

    private boolean isReplayQueueSizeBelowLimit() {
        return redeliveryMessages.size() < getEffectiveLookAheadLimit();
    }

    private int getEffectiveLookAheadLimit() {
        return getEffectiveLookAheadLimit(serviceConfig, consumerList.size());
    }

    static int getEffectiveLookAheadLimit(ServiceConfiguration serviceConfig, int consumerCount) {
        int perConsumerLimit = serviceConfig.getKeySharedLookAheadMsgInReplayThresholdPerConsumer();
        int perSubscriptionLimit = serviceConfig.getKeySharedLookAheadMsgInReplayThresholdPerSubscription();
        int effectiveLimit;
        if (perConsumerLimit <= 0) {
            effectiveLimit = perSubscriptionLimit;
        } else {
            effectiveLimit = perConsumerLimit * consumerCount;
            if (perSubscriptionLimit > 0 && perSubscriptionLimit < effectiveLimit) {
                effectiveLimit = perSubscriptionLimit;
            }
        }
        if (effectiveLimit <= 0) {
            // use max unacked messages limits if key shared look-ahead limits are disabled
            int maxUnackedMessagesPerSubscription = serviceConfig.getMaxUnackedMessagesPerSubscription();
            if (maxUnackedMessagesPerSubscription <= 0) {
                maxUnackedMessagesPerSubscription = Integer.MAX_VALUE;
            }
            int maxUnackedMessagesByConsumers = consumerCount * serviceConfig.getMaxUnackedMessagesPerConsumer();
            if (maxUnackedMessagesByConsumers <= 0) {
                maxUnackedMessagesByConsumers = Integer.MAX_VALUE;
            }
            effectiveLimit = Math.min(maxUnackedMessagesPerSubscription, maxUnackedMessagesByConsumers);
        }
        return effectiveLimit;
    }

    // groups the entries by consumer and filters out the entries that should not be dispatched
    // the entries are handled in the order they are received instead of first grouping them by consumer and
    // then filtering them
    private Map<Consumer, List<Entry>> filterAndGroupEntriesForDispatching(List<Entry> entries, ReadType readType,
                                                                           MutableBoolean triggerLookAhead) {
        // entries grouped by consumer
        Map<Consumer, List<Entry>> entriesGroupedByConsumer = new HashMap<>();
        // permits for consumer, permits are for entries/batches
        Map<Consumer, MutableInt> permitsForConsumer = new HashMap<>();
        boolean lookAheadAllowed = isReplayQueueSizeBelowLimit();
        // in normal read mode, keep track of consumers that are blocked by hash, to check if look-ahead could be useful
        Set<Consumer> blockedByHashConsumers = lookAheadAllowed && readType == ReadType.Normal ? new HashSet<>() : null;
        // in replay read mode, keep track of consumers for entries, used for look-ahead check
        Set<Consumer> consumersForEntriesForLookaheadCheck = lookAheadAllowed ? new HashSet<>() : null;

        for (Entry inputEntry : entries) {
            EntryAndMetadata entry;
            if (inputEntry instanceof EntryAndMetadata entryAndMetadataInstance) {
                entry = entryAndMetadataInstance;
            } else {
                // replace the input entry with EntryAndMetadata instance. In addition to the entry and metadata,
                // it will also carry the calculated sticky key hash
                entry = EntryAndMetadata.create(inputEntry,
                        Commands.peekAndCopyMessageMetadata(inputEntry.getDataBuffer(), getSubscriptionName(), -1));
            }
            int stickyKeyHash = getStickyKeyHash(entry);
            Consumer consumer = selector.select(stickyKeyHash);
            MutableBoolean blockedByHash = null;
            boolean dispatchEntry = false;
            if (consumer != null) {
                if (lookAheadAllowed) {
                    consumersForEntriesForLookaheadCheck.add(consumer);
                }
                blockedByHash = lookAheadAllowed && readType == ReadType.Normal ? new MutableBoolean(false) : null;
                MutableInt permits =
                        permitsForConsumer.computeIfAbsent(consumer,
                                k -> new MutableInt(getAvailablePermits(consumer)));
                // a consumer was found for the sticky key hash and the entry can be dispatched
                if (permits.intValue() > 0
                        && canDispatchEntry(consumer, entry, readType, stickyKeyHash, blockedByHash)) {
                    // decrement the permits for the consumer
                    permits.decrement();
                    // allow the entry to be dispatched
                    dispatchEntry = true;
                }
            }
            if (dispatchEntry) {
                // add the entry to consumer's entry list for dispatching
                List<Entry> consumerEntries =
                        entriesGroupedByConsumer.computeIfAbsent(consumer, k -> new ArrayList<>());
                consumerEntries.add(entry);
            } else {
                if (blockedByHash != null && blockedByHash.isTrue()) {
                    // the entry is blocked by hash, add the consumer to the blocked set
                    blockedByHashConsumers.add(consumer);
                }
                // add the message to replay
                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                // release the entry as it will not be dispatched
                entry.release();
            }
        }

        //
        // determine whether look-ahead could be useful for making more progress
        //
        if (lookAheadAllowed && entriesGroupedByConsumer.isEmpty()) {
            // check if look-ahead could be useful for the consumers that are blocked by a hash that is in the replay
            // queue. This check applies only to the normal read mode.
            if (readType == ReadType.Normal) {
                for (Consumer consumer : blockedByHashConsumers) {
                    // if the consumer isn't in the entriesGroupedByConsumer, it means that it won't receive any
                    // messages
                    // if it has available permits, then look-ahead could be useful for this particular consumer
                    // to make further progress
                    if (!entriesGroupedByConsumer.containsKey(consumer)
                            && permitsForConsumer.get(consumer).intValue() > 0) {
                        triggerLookAhead.setTrue();
                        break;
                    }
                }
            }
            // check if look-ahead could be useful for other consumers
            if (!triggerLookAhead.booleanValue()) {
                for (Consumer consumer : getConsumers()) {
                    // filter out the consumers that are already checked when the entries were processed for entries
                    if (!consumersForEntriesForLookaheadCheck.contains(consumer)) {
                        // if another consumer has available permits, then look-ahead could be useful
                        if (getAvailablePermits(consumer) > 0) {
                            triggerLookAhead.setTrue();
                            break;
                        }
                    }
                }
            }
        }
        return entriesGroupedByConsumer;
    }

    // checks if the entry can be dispatched to the consumer
    private boolean canDispatchEntry(Consumer consumer, Entry entry,
                                     ReadType readType, int stickyKeyHash,
                                     MutableBoolean blockedByHash) {
        // If redeliveryMessages contains messages that correspond to the same hash as the entry to be dispatched
        // do not send those messages for order guarantee
        if (readType == ReadType.Normal && redeliveryMessages.containsStickyKeyHash(stickyKeyHash)) {
            if (blockedByHash != null) {
                blockedByHash.setTrue();
            }
            return false;
        }

        if (drainingHashesRequired) {
            // If the hash is draining, do not send the message
            if (drainingHashesTracker.shouldBlockStickyKeyHash(consumer, stickyKeyHash)) {
                if (blockedByHash != null) {
                    blockedByHash.setTrue();
                }
                return false;
            }
        }

        return true;
    }

    /**
     * Creates a filter for replaying messages. The filter is stateful and shouldn't be cached or reused.
     * @see PersistentDispatcherMultipleConsumers#createFilterForReplay()
     */
    @Override
    protected Predicate<Position> createFilterForReplay() {
        return new ReplayPositionFilter();
    }

    /**
     * Filter for replaying messages. The filter is stateful for a single invocation and shouldn't be cached, shared
     * or reused. This is a short-lived object, and optimizing it for the "no garbage" coding style of Pulsar is
     * unnecessary since the JVM can optimize allocations for short-lived objects.
     */
    private class ReplayPositionFilter implements Predicate<Position> {
        // tracks the available permits for each consumer for the duration of the filter usage
        // the filter is stateful and shouldn't be shared or reused later
        private final Map<Consumer, MutableInt> availablePermitsMap = new HashMap<>();

        @Override
        public boolean test(Position position) {
            // if out of order delivery is allowed, then any position will be replayed
            if (isAllowOutOfOrderDelivery()) {
                return true;
            }
            // lookup the sticky key hash for the entry at the replay position
            Long stickyKeyHash = redeliveryMessages.getHash(position.getLedgerId(), position.getEntryId());
            if (stickyKeyHash == null) {
                // the sticky key hash is missing for delayed messages, the filtering will happen at the time of
                // dispatch after reading the entry from the ledger
                if (log.isDebugEnabled()) {
                    log.debug("[{}] replay of entry at position {} doesn't contain sticky key hash.", name, position);
                }
                return true;
            }

            // find the consumer for the sticky key hash
            Consumer consumer = selector.select(stickyKeyHash.intValue());
            // skip replaying the message position if there's no assigned consumer
            if (consumer == null) {
                return false;
            }
            // lookup the available permits for the consumer
            MutableInt availablePermits =
                    availablePermitsMap.computeIfAbsent(consumer,
                            k -> new MutableInt(getAvailablePermits(consumer)));
            // skip replaying the message position if the consumer has no available permits
            if (availablePermits.intValue() <= 0) {
                return false;
            }

            if (drainingHashesRequired
                    && drainingHashesTracker.shouldBlockStickyKeyHash(consumer, stickyKeyHash.intValue())) {
                // the hash is draining and the consumer is not the draining consumer
                return false;
            }

            availablePermits.decrement();
            return true;
        }
    }

    @Override
    protected int getStickyKeyHash(Entry entry) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            // use the cached sticky key hash if available, otherwise calculate the sticky key hash and cache it
            return entryAndMetadata.getOrUpdateCachedStickyKeyHash(selector::makeStickyKeyHash);
        }
        return selector.makeStickyKeyHash(peekStickyKey(entry.getDataBuffer()));
    }

    @Override
    public void markDeletePositionMoveForward() {
        // reschedule a read with a backoff after moving the mark-delete position forward since there might have
        // been consumers that were blocked by hash and couldn't make progress
        reScheduleReadWithKeySharedUnblockingInterval();
    }

    /**
     * The dispatcher will skip replaying messages when all messages in the replay queue are filtered out when
     * skipNextReplayToTriggerLookAhead=true. The flag gets resetted after the call.
     *
     * If we're stuck on replay, we want to move forward reading on the topic (until the configured look ahead
     * limits kick in), instead of keep replaying the same old messages, since the consumer that these
     * messages are routing to might be busy at the moment.
     *
     * Please see {@link ServiceConfiguration#getKeySharedLookAheadMsgInReplayThresholdPerConsumer} and
     * {@link ServiceConfiguration#getKeySharedLookAheadMsgInReplayThresholdPerSubscription} for configuring the limits.
     */
    @Override
    protected synchronized boolean canReplayMessages() {
        if (skipNextReplayToTriggerLookAhead) {
            skipNextReplayToTriggerLookAhead = false;
            return false;
        }
        return true;
    }

    private int getAvailablePermits(Consumer c) {
        // skip consumers that are currently closing
        if (!c.cnx().isActive()) {
            return 0;
        }
        int availablePermits = Math.max(c.getAvailablePermits(), 0);
        if (availablePermits > 0 && c.getMaxUnackedMessages() > 0) {
            // Calculate the maximum number of additional unacked messages allowed
            int maxAdditionalUnackedMessages = Math.max(c.getMaxUnackedMessages() - c.getUnackedMessages(), 0);
            if (maxAdditionalUnackedMessages == 0) {
                // if the consumer has reached the max unacked messages, then no more messages can be dispatched
                return 0;
            }
            // Estimate the remaining permits based on the average messages per entry
            // add "avgMessagesPerEntry - 1" to round up the division to the next integer without the need to use
            // floating point arithmetic
            int avgMessagesPerEntry = Math.max(c.getAvgMessagesPerEntry(), 1);
            int estimatedRemainingPermits =
                    (maxAdditionalUnackedMessages + avgMessagesPerEntry - 1) / avgMessagesPerEntry;
            // return the minimum of current available permits and estimated remaining permits
            return Math.min(availablePermits, estimatedRemainingPermits);
        } else {
            return availablePermits;
        }
    }

    /**
     * For Key_Shared subscription, the dispatcher will not read more entries while there are pending reads
     * or pending replay reads.
     * @return true if there are no pending reads or pending replay reads
     */
    @Override
    protected boolean doesntHavePendingRead() {
        return !havePendingRead && !havePendingReplayRead;
    }

    /**
     * For Key_Shared subscription, the dispatcher will not attempt to read more entries if the replay queue size
     * has reached the limit or if there are no consumers with permits.
     */
    @Override
    protected boolean isNormalReadAllowed() {
        // don't allow reading more if the replay queue size has reached the limit
        if (!isReplayQueueSizeBelowLimit()) {
            return false;
        }
        for (Consumer consumer : consumerList) {
            // skip blocked consumers
            if (consumer == null || consumer.isBlocked()) {
                continue;
            }
            // before reading more, check that there's at least one consumer that has permits
            if (getAvailablePermits(consumer) > 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected int getMaxEntriesReadLimit() {
        // prevent the redelivery queue from growing over the limit by limiting the number of entries to read
        // to the maximum number of entries that can be added to the redelivery queue
        return Math.max(getEffectiveLookAheadLimit() - redeliveryMessages.size(), 1);
    }

    /**
     * When a normal read is not allowed, the dispatcher will reschedule a read with a backoff.
     */
    @Override
    protected void handleNormalReadNotAllowed() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Skipping read for the topic since normal read isn't allowed. "
                    + "Rescheduling a read with a backoff.", topic.getName(), getSubscriptionName());
        }
        reScheduleReadWithBackoff();
    }

    @Override
    public SubType getType() {
        return SubType.Key_Shared;
    }

    @Override
    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }

    public KeySharedMode getKeySharedMode() {
        return this.keySharedMode;
    }

    public boolean isAllowOutOfOrderDelivery() {
        return this.allowOutOfOrderDelivery;
    }

    public boolean hasSameKeySharedPolicy(KeySharedMeta ksm) {
        return (ksm.getKeySharedMode() == this.keySharedMode
                && ksm.isAllowOutOfOrderDelivery() == this.allowOutOfOrderDelivery);
    }

    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return selector.getConsumerKeyHashRanges();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);
}
