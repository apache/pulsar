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
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.PositionImplRecyclable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    private final boolean allowOutOfOrderDelivery;
    private final StickyKeyConsumerSelector selector;

    private boolean isDispatcherStuckOnReplays = false;
    private final KeySharedMode keySharedMode;

    /**
     * When a consumer joins, it will be added to this map with the current last sent position per the message key.
     * This means that, in order to preserve ordering per the message key, new consumers can only receive old
     * messages, until the mark-delete position will move past this point in the key. New consumers can receive
     * any new messages with the message key that is not in the last sent position.
     */
    private final LinkedHashMap<Consumer, LastSentPositions> recentlyJoinedConsumers;
    // The lastSentPosition is not thread-safe
    private final LastSentPositions lastSentPositions;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        super(topic, cursor, subscription, ksm.isAllowOutOfOrderDelivery());

        this.allowOutOfOrderDelivery = ksm.isAllowOutOfOrderDelivery();
        this.recentlyJoinedConsumers = allowOutOfOrderDelivery ? null : new LinkedHashMap<>();
        lastSentPositions = allowOutOfOrderDelivery ? null : new LastSentPositions();
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

    protected static class LastSentPositions implements Cloneable, AutoCloseable {
        /**
         * Should hold not sticky key hashes but sticky keys as map key,
         * because the dispatcher guarantees message ordering by not sticky key hashes but sticky keys.
         */
        private Map<ByteBuffer, PositionImplRecyclable> positionMap;

        public LastSentPositions() {
            positionMap = new HashMap<>();
        }

        public synchronized boolean removePreviousPositionsFromHash(final long ledgerId, final long entryId) {
            if (positionMap == null) {
                return false;
            }
            final Iterator<Map.Entry<ByteBuffer, PositionImplRecyclable>> itr = positionMap.entrySet().iterator();
            boolean hasPositionRemovedFromLastSentPosition = false;
            while (itr.hasNext()) {
                final Map.Entry<ByteBuffer, PositionImplRecyclable> e = itr.next();
                if (e.getValue().compareTo(ledgerId, entryId) <= 0) {
                    hasPositionRemovedFromLastSentPosition = true;
                    e.getValue().recycle();
                    itr.remove();
                }
            }
            return hasPositionRemovedFromLastSentPosition;
        }

        public synchronized void updateOrPutPosition(final ByteBuffer stickyKey,
                                                     final long ledgerId, final long entryId) {
            if (positionMap == null) {
                return;
            }
            positionMap.compute(stickyKey, (k, currentPosition) -> {
                if (currentPosition == null) {
                    final PositionImplRecyclable newPosition = PositionImplRecyclable.create();
                    newPosition.setLedgerId(ledgerId);
                    newPosition.setEntryId(entryId);
                    return newPosition;
                }
                if (currentPosition.compareTo(ledgerId, entryId) < 0) {
                    currentPosition.setLedgerId(ledgerId);
                    currentPosition.setEntryId(entryId);
                }
                return currentPosition;
            });
        }

        public synchronized int compareToLastSentPosition(final ByteBuffer stickyKey,
                                                          final long ledgerId, final long entryId) {
            final PositionImplRecyclable lastSentPosition = positionMap == null ? null : positionMap.get(stickyKey);
            if (lastSentPosition == null) {
                // The dispatcher can dispatch any messages that is not contained in the last sent position.
                return 1;
            }
            return lastSentPosition.compareTo(ledgerId, entryId);
        }

        public synchronized boolean isEmpty() {
            return positionMap == null || positionMap.isEmpty();
        }

        @Override
        public synchronized LastSentPositions clone() {
            final LastSentPositions clone = new LastSentPositions();
            if (positionMap != null) {
                positionMap.forEach((k, v) -> {
                    final PositionImplRecyclable position = PositionImplRecyclable.create();
                    position.setLedgerId(v.getLedgerId());
                    position.setEntryId(v.getEntryId());
                    clone.positionMap.put(k, position);
                });
            }
            return clone;
        }

        @Override
        public synchronized String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("{");
            if (positionMap != null) {
                stringBuilder.append(positionMap.entrySet().stream()
                        .map(e -> new String(e.getKey().array()) + "=" + e.getValue())
                        .collect(Collectors.joining(", ")));
            }
            stringBuilder.append("}");
            return stringBuilder.toString();
        }

        public synchronized String toPositionSetString() {
            return positionMap == null ? null : positionMap.values().toString();
        }

        @Override
        public synchronized void close() {
            if (positionMap != null && !positionMap.isEmpty()) {
                positionMap.values().forEach(PositionImplRecyclable::recycle);
                positionMap.clear();
            }
            positionMap = null;
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        final CompletableFuture<Void> future = super.close();
        if (!allowOutOfOrderDelivery) {
            future.thenRun(() -> {
                // Make sure all PositionImplRecyclable instances are recycled.
                synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                    if (lastSentPositions != null) {
                        lastSentPositions.close();
                    }
                    if (recentlyJoinedConsumers != null) {
                        recentlyJoinedConsumers.values().forEach(LastSentPositions::close);
                    }
                }
            });
        }
        return future;
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
            if (lastSentPositions != null) {
                synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                    final PositionImpl mdp = (PositionImpl) cursor.getMarkDeletedPosition();
                    if (mdp != null) {
                        lastSentPositions.removePreviousPositionsFromHash(mdp.getLedgerId(), mdp.getEntryId());
                    }
                    final LastSentPositions lastSentPositionsWhenJoining = lastSentPositions.clone();
                    consumer.setLastSentPositionsWhenJoiningString(lastSentPositionsWhenJoining.toPositionSetString());

                    // If this was the 1st consumer, or if all the messages are already acked, then we
                    // don't need to do anything special
                    if (recentlyJoinedConsumers != null
                            && consumerList.size() > 1
                            && cursor.getNumberOfEntriesSinceFirstNotAckedMessage() > 1) {
                        recentlyJoinedConsumers.put(consumer, lastSentPositionsWhenJoining);
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
        if (recentlyJoinedConsumers != null) {
            final LastSentPositions lastSentPositionsForC = recentlyJoinedConsumers.remove(consumer);
            if (lastSentPositionsForC != null) {
                lastSentPositionsForC.close();
            }
            if (consumerList.size() == 1) {
                recentlyJoinedConsumers.values().forEach(LastSentPositions::close);
                recentlyJoinedConsumers.clear();
            }
            if (removeConsumersFromRecentJoinedConsumers() || !redeliveryMessages.isEmpty()) {
                readMoreEntries();
            }
        }
    }

    private static final FastThreadLocal<Map<Consumer, List<Entry>>> localGroupedEntries =
            new FastThreadLocal<>() {
                @Override
                protected Map<Consumer, List<Entry>> initialValue() throws Exception {
                    return new HashMap<>();
                }
            };
    private static final FastThreadLocal<Map<Consumer, Set<Integer>>> localConsumerStickyKeyHashesMap =
            new FastThreadLocal<>() {
                @Override
                protected Map<Consumer, Set<Integer>> initialValue() throws Exception {
                    return new HashMap<>();
                }
            };

    @Override
    protected synchronized boolean trySendMessagesToConsumers(ReadType readType, List<Entry> entries) {
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

        // A corner case that we have to retry a readMoreEntries in order to preserver order delivery.
        // This may happen when consumer closed. See issue #12885 for details.
        if (!allowOutOfOrderDelivery) {
            NavigableSet<PositionImpl> messagesToReplayNow = this.getMessagesToReplayNow(1);
            if (messagesToReplayNow != null && !messagesToReplayNow.isEmpty()) {
                PositionImpl replayPosition = messagesToReplayNow.first();

                // We have received a message potentially from the delayed tracker and, since we're not using it
                // right now, it needs to be added to the redelivery tracker or we won't attempt anymore to
                // resend it (until we disconnect consumer).
                redeliveryMessages.add(replayPosition.getLedgerId(), replayPosition.getEntryId());

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
        }

        if (recentlyJoinedConsumers != null) {
            removeConsumersFromRecentJoinedConsumers();
        }

        final Map<Consumer, List<Entry>> groupedEntries = localGroupedEntries.get();
        groupedEntries.clear();
        final Map<Consumer, Set<Integer>> consumerStickyKeyHashesMap = localConsumerStickyKeyHashesMap.get();
        consumerStickyKeyHashesMap.clear();

        for (Entry entry : entries) {
            final ByteBuffer stickyKey = ByteBuffer.wrap(peekStickyKey(entry.getDataBuffer()));
            int stickyKeyHash = getStickyKeyHash(stickyKey.array());
            Consumer c = selector.select(stickyKeyHash);

            final LastSentPositions lastSentPositions = recentlyJoinedConsumers == null
                    ? null : recentlyJoinedConsumers.get(c);
            if (c != null && (lastSentPositions == null
                                || lastSentPositions
                    .compareToLastSentPosition(stickyKey, entry.getLedgerId(), entry.getEntryId()) >= 0)) {
                groupedEntries.computeIfAbsent(c, k -> new ArrayList<>()).add(entry);
                consumerStickyKeyHashesMap.computeIfAbsent(c, k -> new HashSet<>()).add(stickyKeyHash);
            } else {
                addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                entry.release();
            }
        }

        AtomicInteger keyNumbers = new AtomicInteger(groupedEntries.size());

        int currentThreadKeyNumber = groupedEntries.size();
        if (currentThreadKeyNumber == 0) {
            currentThreadKeyNumber = -1;
        }
        for (Map.Entry<Consumer, List<Entry>> current : groupedEntries.entrySet()) {
            Consumer consumer = current.getKey();
            assert consumer != null; // checked when added to groupedEntries
            List<Entry> entriesWithinSameRange = current.getValue();
            int entriesWithinSameRangeCount = entriesWithinSameRange.size();
            int availablePermits = Math.max(consumer.getAvailablePermits(), 0);
            if (consumer.getMaxUnackedMessages() > 0) {
                int remainUnAckedMessages =
                        // Avoid negative number
                        Math.max(consumer.getMaxUnackedMessages() - consumer.getUnackedMessages(), 0);
                availablePermits = Math.min(availablePermits, remainUnAckedMessages);
            }
            int maxMessagesForC = Math.min(entriesWithinSameRangeCount, availablePermits);
            int messagesForC = getRestrictedMaxEntriesForConsumer(maxMessagesForC, readType,
                    consumerStickyKeyHashesMap.get(consumer));

            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer.consumerName(), messagesForC, readType);
            }

            if (messagesForC < entriesWithinSameRangeCount) {
                // We are not able to push all the messages with given key to its consumer,
                // so we discard for now and mark them for later redelivery
                for (int i = messagesForC; i < entriesWithinSameRangeCount; i++) {
                    Entry entry = entriesWithinSameRange.get(i);
                    long stickyKeyHash = getStickyKeyHash(entry);
                    addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                    entry.release();
                    entriesWithinSameRange.set(i, null);
                }
            }

            if (messagesForC > 0) {
                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    for (int i = 0; i < messagesForC; i++) {
                        Entry entry = entriesWithinSameRange.get(i);
                        redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                    }
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                EntryBatchSizes batchSizes = EntryBatchSizes.get(messagesForC);
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(messagesForC);
                totalEntries += filterEntriesForConsumer(entriesWithinSameRange, batchSizes, sendMessageInfo,
                        batchIndexesAcks, cursor, readType == ReadType.Replay, consumer);

                if (lastSentPositions != null) {
                    entriesWithinSameRange.forEach(e -> {
                        if (e == null) {
                            return;
                        }
                        final ByteBuffer stickyKey = ByteBuffer.wrap(peekStickyKey(e.getDataBuffer()));
                        lastSentPositions.updateOrPutPosition(stickyKey, e.getLedgerId(), e.getEntryId());
                    });
                }

                consumer.sendMessages(entriesWithinSameRange, batchSizes, batchIndexesAcks,
                        sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                        getRedeliveryTracker()).addListener(future -> {
                    if (future.isDone() && keyNumbers.decrementAndGet() == 0) {
                        readMoreEntries();
                    }
                });
                TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this,
                        -(sendMessageInfo.getTotalMessages() - batchIndexesAcks.getTotalAckedIndexCount()));
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();
            } else {
                currentThreadKeyNumber = keyNumbers.decrementAndGet();
            }
        }

        // acquire message-dispatch permits for already delivered messages
        acquirePermitsForDeliveredMessages(topic, cursor, totalEntries, totalMessagesSent, totalBytesSent);

        if (totalMessagesSent == 0) {
            // This means, that all the messages we've just read cannot be dispatched right now.
            // This condition can only happen when:
            //  1. We have consumers ready to accept messages (otherwise the would not haven been triggered)
            //  2. All keys in the current set of messages are routing to consumers that are currently busy
            //
            // The solution here is to move on and read next batch of messages which might hopefully contain
            // also keys meant for other consumers.
            isDispatcherStuckOnReplays = true;
            return true;
        }
        return currentThreadKeyNumber == 0;
    }

    private int getRestrictedMaxEntriesForConsumer(int maxMessages, ReadType readType, Set<Integer> stickyKeyHashes) {
        if (readType == ReadType.Normal && stickyKeyHashes != null
                && redeliveryMessages.containsStickyKeyHashes(stickyKeyHashes)) {
            // If redeliveryMessages contains messages that correspond to the same hash as the messages
            // that the dispatcher is trying to send, do not send those messages for order guarantee
            return 0;
        }

        return maxMessages;
    }

    @Override
    public void markDeletePositionMoveForward() {
        // Execute the notification in different thread to avoid a mutex chain here
        // from the delete operation that was completed
        topic.getBrokerService().getTopicOrderedExecutor().execute(() -> {
            synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                if (lastSentPositions != null) {
                    final PositionImpl mdp = (PositionImpl) cursor.getMarkDeletedPosition();
                    if (mdp != null) {
                        lastSentPositions.removePreviousPositionsFromHash(mdp.getLedgerId(), mdp.getEntryId());
                    }
                }
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
        Iterator<Map.Entry<Consumer, LastSentPositions>> itr = recentlyJoinedConsumers.entrySet().iterator();
        boolean hasPositionRemovedFromLastSentPositionForTheConsumer = false;
        final PositionImpl mdp = (PositionImpl) cursor.getMarkDeletedPosition();
        if (mdp != null) {
            while (itr.hasNext()) {
                LastSentPositions lastSentPositionsForC = itr.next().getValue();
                if (lastSentPositionsForC.removePreviousPositionsFromHash(mdp.getLedgerId(), mdp.getEntryId())) {
                    hasPositionRemovedFromLastSentPositionForTheConsumer = true;
                    if (lastSentPositionsForC.isEmpty()) {
                        itr.remove();
                    }
                } else {
                    break;
                }
            }
        }
        return hasPositionRemovedFromLastSentPositionForTheConsumer;
    }

    @Override
    protected synchronized NavigableSet<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (isDispatcherStuckOnReplays) {
            // If we're stuck on replay, we want to move forward reading on the topic (until the overall max-unacked
            // messages kicks in), instead of keep replaying the same old messages, since the consumer that these
            // messages are routing to might be busy at the moment
            this.isDispatcherStuckOnReplays = false;
            return Collections.emptyNavigableSet();
        } else {
            return super.getMessagesToReplayNow(maxMessagesToRead);
        }
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

    public LinkedHashMap<Consumer, LastSentPositions> getRecentlyJoinedConsumers() {
        return recentlyJoinedConsumers;
    }

    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return selector.getConsumerKeyHashRanges();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);

}
