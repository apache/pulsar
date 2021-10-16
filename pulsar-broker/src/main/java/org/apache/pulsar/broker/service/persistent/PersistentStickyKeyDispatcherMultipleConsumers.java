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

import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    private final boolean allowOutOfOrderDelivery;
    private final StickyKeyConsumerSelector selector;

    private boolean isDispatcherStuckOnReplays = false;
    private final KeySharedMode keySharedMode;

    /**
     * When a consumer joins, it will be added to this map with the current read position.
     * This means that, in order to preserve ordering, new consumers can only receive old
     * messages, until the mark-delete position will move past this point.
     */
    private final LinkedHashMap<Consumer, PositionImpl> recentlyJoinedConsumers;

    private final Set<Consumer> stuckConsumers;
    private final Set<Consumer> nextStuckConsumers;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        super(topic, cursor, subscription, ksm.isAllowOutOfOrderDelivery());

        this.allowOutOfOrderDelivery = ksm.isAllowOutOfOrderDelivery();
        this.recentlyJoinedConsumers = allowOutOfOrderDelivery ? null : new LinkedHashMap<>();
        this.stuckConsumers = new HashSet<>();
        this.nextStuckConsumers = new HashSet<>();
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

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        super.addConsumer(consumer);
        try {
            selector.addConsumer(consumer);
        } catch (BrokerServiceException e) {
            consumerSet.removeAll(consumer);
            consumerList.remove(consumer);
            throw e;
        }

        PositionImpl readPositionWhenJoining = (PositionImpl) cursor.getReadPosition();
        consumer.setReadPositionWhenJoining(readPositionWhenJoining);
        // If this was the 1st consumer, or if all the messages are already acked, then we
        // don't need to do anything special
        if (!allowOutOfOrderDelivery
                && recentlyJoinedConsumers != null
                && consumerList.size() > 1
                && cursor.getNumberOfEntriesSinceFirstNotAckedMessage() > 1) {
            recentlyJoinedConsumers.put(consumer, readPositionWhenJoining);
        }
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
            recentlyJoinedConsumers.remove(consumer);
            if (consumerList.size() == 1) {
                recentlyJoinedConsumers.clear();
            }
            if (removeConsumersFromRecentJoinedConsumers() || !redeliveryMessages.isEmpty()) {
                readMoreEntries();
            }
        }
    }

    private static final FastThreadLocal<Map<Consumer, List<Entry>>> localGroupedEntries =
            new FastThreadLocal<Map<Consumer, List<Entry>>>() {
                @Override
                protected Map<Consumer, List<Entry>> initialValue() throws Exception {
                    return new HashMap<>();
                }
            };

    @Override
    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        int entriesCount = entries.size();

        // Trigger read more messages
        if (entriesCount == 0) {
            readMoreEntries();
            return;
        }

        if (consumerSet.isEmpty()) {
            entries.forEach(Entry::release);
            cursor.rewind();
            return;
        }

        nextStuckConsumers.clear();

        final Map<Consumer, List<Entry>> groupedEntries = localGroupedEntries.get();
        groupedEntries.clear();
        final Map<Consumer, Set<Integer>> consumerStickyKeyHashesMap = new HashMap<>();

        for (Entry entry : entries) {
            int stickyKeyHash = getStickyKeyHash(entry);
            Consumer c = selector.select(stickyKeyHash);
            if (c != null) {
                groupedEntries.computeIfAbsent(c, k -> new ArrayList<>()).add(entry);
                consumerStickyKeyHashesMap.computeIfAbsent(c, k -> new HashSet<>()).add(stickyKeyHash);
            } else {
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
            List<Entry> entriesWithSameKey = current.getValue();
            int entriesWithSameKeyCount = entriesWithSameKey.size();
            final int availablePermits = consumer == null ? 0 : Math.max(consumer.getAvailablePermits(), 0);
            int maxMessagesForC = Math.min(entriesWithSameKeyCount, availablePermits);
            int messagesForC = getRestrictedMaxEntriesForConsumer(consumer, entriesWithSameKey, maxMessagesForC,
                    readType, consumerStickyKeyHashesMap.get(consumer));
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer == null ? "null" : consumer.consumerName(), messagesForC, readType);
            }

            if (messagesForC < entriesWithSameKeyCount) {
                // We are not able to push all the messages with given key to its consumer,
                // so we discard for now and mark them for later redelivery
                for (int i = messagesForC; i < entriesWithSameKeyCount; i++) {
                    Entry entry = entriesWithSameKey.get(i);
                    long stickyKeyHash = getStickyKeyHash(entry);
                    addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                    entry.release();
                    entriesWithSameKey.set(i, null);
                }
            }

            if (messagesForC > 0) {
                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    for (int i = 0; i < messagesForC; i++) {
                        Entry entry = entriesWithSameKey.get(i);
                        redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                    }
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                EntryBatchSizes batchSizes = EntryBatchSizes.get(messagesForC);
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(messagesForC);
                filterEntriesForConsumer(entriesWithSameKey, batchSizes, sendMessageInfo, batchIndexesAcks, cursor,
                        readType == ReadType.Replay);

                consumer.sendMessages(entriesWithSameKey, batchSizes, batchIndexesAcks,
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
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            if (topic.getDispatchRateLimiter().isPresent()) {
                topic.getDispatchRateLimiter().get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }

            if (dispatchRateLimiter.isPresent()) {
                dispatchRateLimiter.get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
            }
        }

        stuckConsumers.clear();

        if (totalMessagesSent == 0 && (recentlyJoinedConsumers == null || recentlyJoinedConsumers.isEmpty())) {
            // This means, that all the messages we've just read cannot be dispatched right now.
            // This condition can only happen when:
            //  1. We have consumers ready to accept messages (otherwise the would not haven been triggered)
            //  2. All keys in the current set of messages are routing to consumers that are currently busy
            //     and stuck is not caused by stuckConsumers
            //
            // The solution here is to move on and read next batch of messages which might hopefully contain
            // also keys meant for other consumers.
            //
            // We do it unless that are "recently joined consumers". In that case, we would be looking
            // ahead in the stream while the new consumers are not ready to accept the new messages,
            // therefore would be most likely only increase the distance between read-position and mark-delete
            // position.
            if (!nextStuckConsumers.isEmpty()) {
                isDispatcherStuckOnReplays = true;
                stuckConsumers.addAll(nextStuckConsumers);
            }
            // readMoreEntries should run regardless whether or not stuck is caused by
            // stuckConsumers for avoid stopping dispatch.
            readMoreEntries();
        }  else if (currentThreadKeyNumber == 0) {
            topic.getBrokerService().executor().schedule(() -> {
                synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                    readMoreEntries();
                }
            }, 100, TimeUnit.MILLISECONDS);
        }
    }

    private int getRestrictedMaxEntriesForConsumer(Consumer consumer, List<Entry> entries, int maxMessages,
            ReadType readType, Set<Integer> stickyKeyHashes) {
        if (maxMessages == 0) {
            // the consumer was stuck
            nextStuckConsumers.add(consumer);
            return 0;
        }
        if (readType == ReadType.Normal && stickyKeyHashes != null
                && redeliveryMessages.containsStickyKeyHashes(stickyKeyHashes)) {
            // If redeliveryMessages contains messages that correspond to the same hash as the messages
            // that the dispatcher is trying to send, do not send those messages for order guarantee
            return 0;
        }
        if (recentlyJoinedConsumers == null) {
            return maxMessages;
        }
        removeConsumersFromRecentJoinedConsumers();
        PositionImpl maxReadPosition = recentlyJoinedConsumers.get(consumer);
        // At this point, all the old messages were already consumed and this consumer
        // is now ready to receive any message
        if (maxReadPosition == null) {
            // stop to dispatch by stuckConsumers
            if (stuckConsumers.contains(consumer)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] stop to dispatch by stuckConsumers, consumer: {}", name, consumer);
                }
                return 0;
            }
            // The consumer has not recently joined, so we can send all messages
            return maxMessages;
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
            PositionImpl minReadPositionForRecentJoinedConsumer = recentlyJoinedConsumers.values().iterator().next();
            if (minReadPositionForRecentJoinedConsumer != null
                    && minReadPositionForRecentJoinedConsumer.compareTo(maxReadPosition) < 0) {
                maxReadPosition = minReadPositionForRecentJoinedConsumer;
            }
        }
        // Here, the consumer is one that has recently joined, so we can only send messages that were
        // published before it has joined.
        for (int i = 0; i < maxMessages; i++) {
            if (((PositionImpl) entries.get(i).getPosition()).compareTo(maxReadPosition) >= 0) {
                // We have already crossed the divider line. All messages in the list are now
                // newer than what we can currently dispatch to this consumer
                return i;
            }
        }

        return maxMessages;
    }

    @Override
    public synchronized void markDeletePositionMoveForward() {
        if (recentlyJoinedConsumers != null && !recentlyJoinedConsumers.isEmpty()
                && removeConsumersFromRecentJoinedConsumers()) {
            // After we process acks, we need to check whether the mark-delete position was advanced and we can finally
            // read more messages. It's safe to call readMoreEntries() multiple times.
            readMoreEntries();
        }
    }

    private boolean removeConsumersFromRecentJoinedConsumers() {
        Iterator<Map.Entry<Consumer, PositionImpl>> itr = recentlyJoinedConsumers.entrySet().iterator();
        boolean hasConsumerRemovedFromTheRecentJoinedConsumers = false;
        PositionImpl mdp = (PositionImpl) cursor.getMarkDeletedPosition();
        if (mdp != null) {
            PositionImpl nextPositionOfTheMarkDeletePosition =
                    ((ManagedLedgerImpl) cursor.getManagedLedger()).getNextValidPosition(mdp);
            while (itr.hasNext()) {
                Map.Entry<Consumer, PositionImpl> entry = itr.next();
                if (entry.getValue().compareTo(nextPositionOfTheMarkDeletePosition) <= 0) {
                    itr.remove();
                    hasConsumerRemovedFromTheRecentJoinedConsumers = true;
                } else {
                    break;
                }
            }
        }
        return hasConsumerRemovedFromTheRecentJoinedConsumers;
    }

    @Override
    protected synchronized Set<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        if (isDispatcherStuckOnReplays) {
            // If we're stuck on replay, we want to move forward reading on the topic (until the overall max-unacked
            // messages kicks in), instead of keep replaying the same old messages, since the consumer that these
            // messages are routing to might be busy at the moment
            this.isDispatcherStuckOnReplays = false;
            return Collections.emptySet();
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

    public LinkedHashMap<Consumer, PositionImpl> getRecentlyJoinedConsumers() {
        return recentlyJoinedConsumers;
    }

    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return selector.getConsumerKeyHashRanges();
    }

    public boolean isAllowOutOfOrderDelivery() {
        return allowOutOfOrderDelivery;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);

}
