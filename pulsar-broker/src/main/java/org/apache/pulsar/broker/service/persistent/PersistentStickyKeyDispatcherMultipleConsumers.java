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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    private final boolean allowOutOfOrderDelivery;
    private final StickyKeyConsumerSelector selector;

    private boolean skipNextReplayToTriggerLookAhead = false;
    private final KeySharedMode keySharedMode;

    /**
     * When a consumer joins, it will be added to this map with the current read position.
     * This means that, in order to preserve ordering, new consumers can only receive old
     * messages, until the mark-delete position will move past this point.
     */
    private final LinkedHashMap<Consumer, Position> recentlyJoinedConsumers;

    /**
     * The lastSentPosition and the individuallySentPositions are not thread safe.
     */
    @Nullable
    private Position lastSentPosition;
    private final LongPairRangeSet<Position> individuallySentPositions;
    private static final LongPairRangeSet.LongPairConsumer<Position> positionRangeConverter = PositionFactory::create;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        super(topic, cursor, subscription, ksm.isAllowOutOfOrderDelivery());

        this.allowOutOfOrderDelivery = ksm.isAllowOutOfOrderDelivery();
        this.recentlyJoinedConsumers = allowOutOfOrderDelivery ? null : new LinkedHashMap<>();
        this.individuallySentPositions =
                allowOutOfOrderDelivery ? null : new ConcurrentOpenLongPairRangeSet<>(4096, positionRangeConverter);
        this.keySharedMode = ksm.getKeySharedMode();
        switch (this.keySharedMode) {
        case AUTO_SPLIT:
            if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                selector = new ConsistentHashingStickyKeyConsumerSelector(
                        conf.getSubscriptionKeySharedConsistentHashingReplicaPoints());
            } else {
                selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
            }
            break;

        case STICKY:
            this.selector = new HashRangeExclusiveStickyKeyConsumerSelector();
            break;

        default:
            throw new IllegalArgumentException("Invalid key-shared mode: " + keySharedMode);
        }
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
        return super.addConsumer(consumer).thenCompose(__ ->
                selector.addConsumer(consumer).handle((result, ex) -> {
                    if (ex != null) {
                        synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                            consumerSet.removeAll(consumer);
                            consumerList.remove(consumer);
                        }
                        throw FutureUtil.wrapToCompletionException(ex);
                    }
                    return result;
                })
        ).thenRun(() -> {
            synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                if (!allowOutOfOrderDelivery) {
                    final Position lastSentPositionWhenJoining = updateIfNeededAndGetLastSentPosition();
                    if (lastSentPositionWhenJoining != null) {
                        consumer.setLastSentPositionWhenJoining(lastSentPositionWhenJoining);
                        // If this was the 1st consumer, or if all the messages are already acked, then we
                        // don't need to do anything special
                        if (recentlyJoinedConsumers != null
                                && consumerList.size() > 1
                                && cursor.getNumberOfEntriesSinceFirstNotAckedMessage() > 1) {
                            recentlyJoinedConsumers.put(consumer, lastSentPositionWhenJoining);
                        }
                    }
                }
            }
        });
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        // The consumer must be removed from the selector before calling the superclass removeConsumer method.
        // In the superclass removeConsumer method, the pending acks that the consumer has are added to
        // redeliveryMessages. If the consumer has not been removed from the selector at this point,
        // the broker will try to redeliver the messages to the consumer that has already been closed.
        // As a result, the messages are not redelivered to any consumer, and the mark-delete position does not move,
        // eventually causing all consumers to get stuck.
        selector.removeConsumer(consumer);
        super.removeConsumer(consumer);
        if (!allowOutOfOrderDelivery && recentlyJoinedConsumers != null) {
            recentlyJoinedConsumers.remove(consumer);
            if (consumerList.size() == 1) {
                recentlyJoinedConsumers.clear();
            } else if (consumerList.isEmpty()) {
                // The subscription removes consumers if rewind or reset cursor operations are called.
                // The dispatcher must clear lastSentPosition and individuallySentPositions because
                // these operations trigger re-sending messages.
                lastSentPosition = null;
                individuallySentPositions.clear();
            }
            if (removeConsumersFromRecentJoinedConsumers() || !redeliveryMessages.isEmpty()) {
                readMoreEntries();
            }
        }
    }

    @Override
    protected synchronized boolean trySendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        lastNumberOfEntriesDispatched = 0;
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        long totalEntries = 0;
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
                            entries.forEach(entry -> {
                                long stickyKeyHash = getStickyKeyHash(entry);
                                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                                entry.release();
                            });
                        } else if (readType == ReadType.Replay) {
                            entries.forEach(Entry::release);
                        }
                        return true;
                    }
                }
            }

            // Update if the markDeletePosition move forward
            updateIfNeededAndGetLastSentPosition();

            // Should not access to individualDeletedMessages from outside managed cursor
            // because it doesn't guarantee thread safety.
            if (lastSentPosition == null) {
                if (cursor.getMarkDeletedPosition() != null) {
                    lastSentPosition = ((ManagedCursorImpl) cursor)
                            .processIndividuallyDeletedMessagesAndGetMarkDeletedPosition(range -> {
                                final Position lower = range.lowerEndpoint();
                                final Position upper = range.upperEndpoint();
                                individuallySentPositions.addOpenClosed(lower.getLedgerId(), lower.getEntryId(),
                                        upper.getLedgerId(), upper.getEntryId());
                                return true;
                            });
                }
            }
        }

        final Map<Consumer, List<Entry>> entriesByConsumerForDispatching =
                filterAndGroupEntriesForDispatching(entries, readType);

        AtomicInteger remainingConsumersToFinishSending = new AtomicInteger(entriesByConsumerForDispatching.size());
        for (Map.Entry<Consumer, List<Entry>> current : entriesByConsumerForDispatching.entrySet()) {
            Consumer consumer = current.getKey();
            List<Entry> entriesForConsumer = current.getValue();
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer.consumerName(), entriesForConsumer.size(), readType);
            }
            final ManagedLedgerImpl managedLedger = ((ManagedLedgerImpl) cursor.getManagedLedger());
            for (Entry entry : entriesForConsumer) {
                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                }
                // Add positions to individuallySentPositions if necessary
                if (!allowOutOfOrderDelivery) {
                    final Position position = entry.getPosition();
                    // Store to individuallySentPositions even if lastSentPosition is null
                    if ((lastSentPosition == null || position.compareTo(lastSentPosition) > 0)
                            && !individuallySentPositions.contains(position.getLedgerId(), position.getEntryId())) {
                        final Position previousPosition = managedLedger.getPreviousPosition(position);
                        individuallySentPositions.addOpenClosed(previousPosition.getLedgerId(),
                                previousPosition.getEntryId(), position.getLedgerId(), position.getEntryId());
                    }
                }
            }

            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForConsumer.size());
            EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entriesForConsumer.size());
            totalEntries += filterEntriesForConsumer(entriesForConsumer, batchSizes, sendMessageInfo,
                    batchIndexesAcks, cursor, readType == ReadType.Replay, consumer);
            consumer.sendMessages(entriesForConsumer, batchSizes, batchIndexesAcks,
                    sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                    getRedeliveryTracker()).addListener(future -> {
                if (future.isDone() && remainingConsumersToFinishSending.decrementAndGet() == 0) {
                    readMoreEntries();
                }
            });

            TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this,
                    -(sendMessageInfo.getTotalMessages() - batchIndexesAcks.getTotalAckedIndexCount()));
            totalMessagesSent += sendMessageInfo.getTotalMessages();
            totalBytesSent += sendMessageInfo.getTotalBytes();
        }

        // Update the last sent position and remove ranges from individuallySentPositions if necessary
        if (!allowOutOfOrderDelivery && lastSentPosition != null) {
            final ManagedLedgerImpl managedLedger = ((ManagedLedgerImpl) cursor.getManagedLedger());
            com.google.common.collect.Range<Position> range = individuallySentPositions.firstRange();

            // If the upper bound is before the last sent position, we need to move ahead as these
            // individuallySentPositions are now irrelevant.
            if (range != null && range.upperEndpoint().compareTo(lastSentPosition) <= 0) {
                individuallySentPositions.removeAtMost(lastSentPosition.getLedgerId(),
                        lastSentPosition.getEntryId());
                range = individuallySentPositions.firstRange();
            }

            if (range != null) {
                // If the lowerBound is ahead of the last sent position,
                // verify if there are any entries in-between.
                if (range.lowerEndpoint().compareTo(lastSentPosition) <= 0 || managedLedger
                        .getNumberOfEntries(com.google.common.collect.Range.openClosed(lastSentPosition,
                                range.lowerEndpoint())) <= 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Found a position range to last sent: {}", name, range);
                    }
                    Position newLastSentPosition = range.upperEndpoint();
                    Position positionAfterNewLastSent = managedLedger
                            .getNextValidPosition(newLastSentPosition);
                    // sometime ranges are connected but belongs to different ledgers
                    // so, they are placed sequentially
                    // eg: (2:10..3:15] can be returned as (2:10..2:15],[3:0..3:15].
                    // So, try to iterate over connected range and found the last non-connected range
                    // which gives new last sent position.
                    final Position lastConfirmedEntrySnapshot = managedLedger.getLastConfirmedEntry();
                    if (lastConfirmedEntrySnapshot != null) {
                        while (positionAfterNewLastSent.compareTo(lastConfirmedEntrySnapshot) <= 0) {
                            if (individuallySentPositions.contains(positionAfterNewLastSent.getLedgerId(),
                                    positionAfterNewLastSent.getEntryId())) {
                                range = individuallySentPositions.rangeContaining(
                                        positionAfterNewLastSent.getLedgerId(), positionAfterNewLastSent.getEntryId());
                                newLastSentPosition = range.upperEndpoint();
                                positionAfterNewLastSent = managedLedger.getNextValidPosition(newLastSentPosition);
                                // check if next valid position is also deleted and part of the deleted-range
                                continue;
                            }
                            break;
                        }
                    }

                    if (lastSentPosition.compareTo(newLastSentPosition) < 0) {
                        lastSentPosition = newLastSentPosition;
                    }
                    individuallySentPositions.removeAtMost(lastSentPosition.getLedgerId(),
                            lastSentPosition.getEntryId());
                }
            }
        }

        lastNumberOfEntriesDispatched = (int) totalEntries;

        // acquire message-dispatch permits for already delivered messages
        acquirePermitsForDeliveredMessages(topic, cursor, totalEntries, totalMessagesSent, totalBytesSent);

        if (totalMessagesSent == 0 && isLookAheadAllowed()) {
            // This means, that all the messages we've just read cannot be dispatched right now.
            // This condition can only happen when:
            //  1. We have consumers ready to accept messages (otherwise the would not haven been triggered)
            //  2. All keys in the current set of messages are routing to consumers that are currently busy
            //
            // The solution here is to move on and read next batch of messages which might hopefully contain
            // also keys meant for other consumers.
            //
            // We do it unless that are "recently joined consumers". In that case, we would be looking
            // ahead in the stream while the new consumers are not ready to accept the new messages,
            // therefore would be most likely only increase the distance between read-position and mark-delete
            // position.
            skipNextReplayToTriggerLookAhead = true;
            return true;
        } else if (entriesByConsumerForDispatching.size() == 0) {
            // There should be a backoff delay before retrying
            return true;
        }
        return false;
    }

    private boolean isLookAheadAllowed() {
        if (serviceConfig.isKeySharedLookAheadEnabledWhenRecentlyJoinedConsumersExist()
                || (recentlyJoinedConsumers == null || recentlyJoinedConsumers.isEmpty())) {
            long keySharedNumberOfReplayMessagesThresholdForLookAhead = Math.max(
                    serviceConfig.getKeySharedLookAheadMsgInReplayThresholdPerConsumer()
                            * consumerList.size(),
                    serviceConfig.getKeySharedLookAheadMsgInReplayThresholdPerSubscription());
            if (keySharedNumberOfReplayMessagesThresholdForLookAhead == 0
                    || redeliveryMessages.size() < keySharedNumberOfReplayMessagesThresholdForLookAhead) {
                return true;
            }
        }
        return false;
    }

    // groups the entries by consumer and filters out the entries that should not be dispatched
    // the entries are handled in the order they are received instead of first grouping them by consumer and
    // then filtering them
    private Map<Consumer, List<Entry>> filterAndGroupEntriesForDispatching(List<Entry> entries, ReadType readType) {
        // entries grouped by consumer
        Map<Consumer, List<Entry>> entriesGroupedByConsumer = new HashMap<>();
        // consumers for which all remaining entries should be discarded
        Set<Consumer> remainingEntriesFilteredForConsumer = new HashSet<>();

        for (Entry entry : entries) {
            int stickyKeyHash = getStickyKeyHash(entry);
            Consumer consumer = selector.select(stickyKeyHash);
            boolean dispatchEntry = false;
            // a consumer was found for the sticky key hash and the consumer can get more entries
            if (consumer != null && !remainingEntriesFilteredForConsumer.contains(consumer)) {
                dispatchEntry = canDispatchEntry(consumer, entry, readType, stickyKeyHash);
            }
            if (dispatchEntry) {
                // add the entry to consumer's entry list for dispatching
                List<Entry> consumerEntries =
                        entriesGroupedByConsumer.computeIfAbsent(consumer, k -> new ArrayList<>());
                consumerEntries.add(entry);
            } else {
                // add the message to replay
                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                // release the entry as it will not be dispatched
                entry.release();
                // stop sending further entries to this consumer so that ordering is preserved
                remainingEntriesFilteredForConsumer.add(consumer);
            }
        }
        return entriesGroupedByConsumer;
    }

    // checks if the entry can be dispatched to the consumer
    private boolean canDispatchEntry(Consumer consumer, Entry entry, ReadType readType, int stickyKeyHash) {
        // check if the entry can be replayed to a recently joined consumer
        Position maxLastSentPosition = resolveMaxLastSentPositionForRecentlyJoinedConsumer(consumer, readType);
        if (maxLastSentPosition != null && entry.getPosition().compareTo(maxLastSentPosition) > 0) {
            return false;
        }

        // If redeliveryMessages contains messages that correspond to the same hash as the entry to be dispatched
        // do not send those messages for order guarantee
        if (readType == ReadType.Normal && redeliveryMessages.containsStickyKeyHash(stickyKeyHash)) {
            return false;
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
                log.debug("[{}] replay of entry at position {} doesn't contain sticky key hash.", name, position);
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
            // check if the entry position can be replayed to a recently joined consumer
            Position maxLastSentPosition =
                    resolveMaxLastSentPositionForRecentlyJoinedConsumer(consumer, ReadType.Replay);
            if (maxLastSentPosition != null && position.compareTo(maxLastSentPosition) > 0) {
                return false;
            }
            // the position can be replayed,
            // decrement the available permits for the consumer which is tracked for the duration of the filter usage
            // this is an approximation, the actual permits are decremented when the entry is dispatched
            int requiredPermits = Math.max(consumer.getAvgMessagesPerEntry(), 1);
            availablePermits.add(-requiredPermits);
            return true;
        }
    }

    /**
     * Contains the logic to resolve the max last sent position for a consumer
     * when the consumer has recently joined. This is only applicable for key shared mode when
     * allowOutOfOrderDelivery=false.
     */
    private Position resolveMaxLastSentPositionForRecentlyJoinedConsumer(Consumer consumer, ReadType readType) {
        if (recentlyJoinedConsumers == null) {
            return null;
        }
        removeConsumersFromRecentJoinedConsumers();
        Position maxLastSentPosition = recentlyJoinedConsumers.get(consumer);
        // At this point, all the old messages were already consumed and this consumer
        // is now ready to receive any message
        if (maxLastSentPosition == null) {
            // The consumer has not recently joined, so we can send all messages
            return null;
        }

        // If the read type is Replay, we should avoid send messages that hold by other consumer to the new consumers,
        // For example, we have 10 messages [0,1,2,3,4,5,6,7,8,9]
        // If the consumer0 get message 0 and 1, and does not acked message 0, then consumer1 joined,
        // when consumer1 get message 2,3, the broker will not dispatch messages to consumer1
        // because of the mark delete position did not move forward.
        // So message 2,3 will stored in the redeliver tracker.
        // Now, consumer2 joined, it will read new messages from the cursor,
        // so the recentJoinedPosition is 4 for consumer2
        // Because of there are messages need to redeliver, so the broker will read the redelivery message first [2,3]
        // message [2,3] is lower than the recentJoinedPosition 4,
        // so the message [2,3] will dispatched to the consumer2
        // But the message [2,3] should not dispatch to consumer2.

        if (readType == ReadType.Replay) {
            Position minLastSentPositionForRecentJoinedConsumer = recentlyJoinedConsumers.values().iterator().next();
            if (minLastSentPositionForRecentJoinedConsumer != null
                    && minLastSentPositionForRecentJoinedConsumer.compareTo(maxLastSentPosition) < 0) {
                maxLastSentPosition = minLastSentPositionForRecentJoinedConsumer;
            }
        }

        return maxLastSentPosition;
    }


    @Override
    public void markDeletePositionMoveForward() {
        // Execute the notification in different thread to avoid a mutex chain here
        // from the delete operation that was completed
        topic.getBrokerService().getTopicOrderedExecutor().execute(() -> {
            synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                if (recentlyJoinedConsumers != null && !recentlyJoinedConsumers.isEmpty()
                        && removeConsumersFromRecentJoinedConsumers()) {
                    // After we process acks, we need to check whether the mark-delete position was advanced and we
                    // can finally read more messages. It's safe to call readMoreEntries() multiple times.
                    readMoreEntries();
                }
            }
        });
    }

    private boolean removeConsumersFromRecentJoinedConsumers() {
        if (MapUtils.isEmpty(recentlyJoinedConsumers)) {
            return false;
        }
        Iterator<Map.Entry<Consumer, Position>> itr = recentlyJoinedConsumers.entrySet().iterator();
        boolean hasConsumerRemovedFromTheRecentJoinedConsumers = false;
        Position mdp = cursor.getMarkDeletedPosition();
        if (mdp != null) {
            while (itr.hasNext()) {
                Map.Entry<Consumer, Position> entry = itr.next();
                if (entry.getValue().compareTo(mdp) <= 0) {
                    itr.remove();
                    hasConsumerRemovedFromTheRecentJoinedConsumers = true;
                } else {
                    break;
                }
            }
        }
        return hasConsumerRemovedFromTheRecentJoinedConsumers;
    }

    @Nullable
    private synchronized Position updateIfNeededAndGetLastSentPosition() {
        if (lastSentPosition == null) {
            return null;
        }
        final Position mdp = cursor.getMarkDeletedPosition();
        if (mdp != null && mdp.compareTo(lastSentPosition) > 0) {
            lastSentPosition = mdp;
        }
        return lastSentPosition;
    }

    /**
     * The dispatcher will skip replaying messages when all messages in the replay queue are filtered out when
     * isDispatcherStuckOnReplays=true. The flag gets resetted after the call.
     *
     * If we're stuck on replay, we want to move forward reading on the topic (until the configured look ahead
     * limits kick in), instead of keep replaying the same old messages, since the consumer that these
     * messages are routing to might be busy at the moment.
     *
     * Please see {@link ServiceConfiguration#keySharedLookAheadMsgInReplayThresholdPerConsumer},
     * {@link ServiceConfiguration#keySharedLookAheadMsgInReplayThresholdPerSubscription} and
     * {@link ServiceConfiguration#keySharedLookAheadEnabledWhenRecentlyJoinedConsumersExist} for configuring
     * the limits.
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
        int availablePermits = Math.max(c.getAvailablePermits(), 0);
        if (c.getMaxUnackedMessages() > 0) {
            // Avoid negative number
            int remainUnAckedMessages = Math.max(c.getMaxUnackedMessages() - c.getUnackedMessages(), 0);
            availablePermits = Math.min(availablePermits, remainUnAckedMessages);
        }
        return availablePermits;
    }

    /**
     * In Key_Shared mode, the consumer will not receive any entries from a normal reading if it is included in
     * {@link #recentlyJoinedConsumers}, they can only receive entries from replay reads.
     * If all entries in {@link #redeliveryMessages} have been filtered out due to the order guarantee mechanism,
     * Broker need a normal read to make the consumers not included in @link #recentlyJoinedConsumers} will not be
     * stuck. See https://github.com/apache/pulsar/pull/7105.
     */
    @Override
    protected boolean hasConsumersNeededNormalRead() {
        // The variable "hashesToBeBlocked" and "recentlyJoinedConsumers" will be null if "isAllowOutOfOrderDelivery()",
        // So the method "filterOutEntriesWillBeDiscarded" will filter out nothing, just return "true" here.
        if (isAllowOutOfOrderDelivery()) {
            return true;
        }
        for (Consumer consumer : consumerList) {
            if (consumer == null || consumer.isBlocked()) {
                continue;
            }
            if (recentlyJoinedConsumers.containsKey(consumer)) {
                continue;
            }
            if (consumer.getAvailablePermits() > 0) {
                return true;
            }
        }
        return false;
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

    public LinkedHashMap<Consumer, Position> getRecentlyJoinedConsumers() {
        return recentlyJoinedConsumers;
    }

    public synchronized String getLastSentPosition() {
        if (lastSentPosition == null) {
            return null;
        }
        return lastSentPosition.toString();
    }

    @VisibleForTesting
    public Position getLastSentPositionField() {
        return lastSentPosition;
    }

    public synchronized String getIndividuallySentPositions() {
        if (individuallySentPositions == null) {
            return null;
        }
        return individuallySentPositions.toString();
    }

    @VisibleForTesting
    public LongPairRangeSet<Position> getIndividuallySentPositionsField() {
        return individuallySentPositions;
    }

    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return selector.getConsumerKeyHashRanges();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);
}
