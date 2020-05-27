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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.pulsar.common.util.collections.ConcurrentSortedLongPairSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    private final StickyKeyConsumerSelector selector;

    // The fenced message means that the messages can't be delivered at this time. In Key_Shared subscription,
    // the new consumer need to wait all messages before the first message that deliver to this consumer are acknowledged.
    // The `fencedMessagesContainer` maintains all fenced messages by a map (mark-delete-position -> fenced messages).
    // When the mark delete position of the cursor is greater than the mark-delete-position in the `fencedMessagesContainer`,
    // it means the fenced messages will be lifted.
    private final ConcurrentSkipListMap<PositionImpl, ConcurrentSortedLongPairSet> fencedMessagesContainer;

    // Max fenced messages of the dispatcher. If the current fenced messages exceeds
    // the max fenced messages, the dispatcher never add more fenced messages to the `fencedMessagesContainer`.
    // The dispatcher will replay the new messages until the `fencedMessagesContainer` have place to add the new fenced messages.
    private final int maxFencedMessages;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
           Subscription subscription, StickyKeyConsumerSelector selector, int maxFencedMessages) {
        super(topic, cursor, subscription);
        this.selector = selector;
        fencedMessagesContainer = new ConcurrentSkipListMap<>();
        this.maxFencedMessages = maxFencedMessages;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        // Set the fence position for the consumer. Only the positions of the entry are lower that the fence position
        // or current mark delete position of the cursor is greater than the fence position of the consumer,
        // the entries can be deliver to this consumer.
        if (selector instanceof HashRangeAutoSplitStickyKeyConsumerSelector) {
            if (consumerList.isEmpty()) {
                consumer.setFencePositionForKeyShared(PositionImpl.latest);
            } else {
                consumer.setFencePositionForKeyShared((PositionImpl) cursor.getReadPosition());
            }
        }
        super.addConsumer(consumer);
        selector.addConsumer(consumer);
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        super.removeConsumer(consumer);
        selector.removeConsumer(consumer);
    }

    @Override
    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        // Trigger read more messages
        if (entries.size() == 0) {
            readMoreEntries();
            return;
        }

        PositionImpl mdPosition = (PositionImpl) cursor.getMarkDeletedPosition();
        if (checkAndLiftMessages(mdPosition)) {
            if (ReadType.Replay == readType) {
                entries.forEach(entry -> {
                    messagesToRedeliver.add(entry.getLedgerId(), entry.getEntryId());
                    entry.release();
                });
            } else {
                cursor.seek(entries.get(0).getPosition());
                entries.forEach(Entry::release);
            }
            readMoreEntries();
            return;
        }

        // If the fenced messages are exceeds max fenced messages, stop reads more entries and reset the read position.
        if (getFencedMessages() >= maxFencedMessages && ReadType.Replay != readType) {
            cursor.seek(entries.get(0).getPosition());
            entries.forEach(Entry::release);
            return;
        }

        final Map<Integer, List<Entry>> groupedEntries = new HashMap<>();
        for (Entry entry : entries) {
            int key = Murmur3_32Hash.getInstance().makeHash(peekStickyKey(entry.getDataBuffer())) % selector.getRangeSize();
            groupedEntries.putIfAbsent(key, new ArrayList<>());
            groupedEntries.get(key).add(entry);
        }
        final Iterator<Map.Entry<Integer, List<Entry>>> iterator = groupedEntries.entrySet().iterator();
        AtomicInteger keyNumbers = new AtomicInteger(groupedEntries.size());
        CompletableFuture<Void> readMoreFuture = new CompletableFuture<>();
        while (iterator.hasNext() && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            final Map.Entry<Integer, List<Entry>> entriesWithSameKey = iterator.next();
            //TODO: None key policy
            Consumer consumer = selector.selectByIndex(entriesWithSameKey.getKey());
            if (consumer == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found for key {} from total {}", name,
                        entriesWithSameKey.getKey(), consumerList.size());
                entriesWithSameKey.getValue().forEach(Entry::release);
                cursor.seek(entries.get(0).getPosition());
                readMoreEntries();
                return;
            }

            int availablePermits = consumer.isWritable() ? consumer.getAvailablePermits() : 1;
            if (log.isDebugEnabled() && !consumer.isWritable()) {
                log.debug("[{}-{}] consumer is not writable. dispatching only 1 message to {} ", topic.getName(), name,
                        consumer);
            }
            int messagesForC = Math.min(entriesWithSameKey.getValue().size(), availablePermits);
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} for key {} with messages num {}, read type is {}",
                        name, consumer.consumerName(), entriesWithSameKey.getKey(), messagesForC, readType);
            }
            if (messagesForC > 0) {
                // remove positions first from replay list first : sendMessages recycles entries
                List<Entry> subList = new ArrayList<>(entriesWithSameKey.getValue().subList(0, messagesForC));
                if (readType == ReadType.Replay) {
                    subList.forEach(entry -> messagesToRedeliver.remove(entry.getLedgerId(), entry.getEntryId()));
                }
                if (selector instanceof HashRangeAutoSplitStickyKeyConsumerSelector) {
                    PositionImpl startPosition = (PositionImpl) subList.get(0).getPosition();

                    // To fence messages if current mark delete position of the cursor is lower than fence position of the consumer.
                    if (consumer.getFencePositionForKeyShared() != null && mdPosition.getNext().compareTo(consumer.getFencePositionForKeyShared()) < 0) {
                        if (startPosition.compareTo(consumer.getFencePositionForKeyShared()) >= 0) {
                            fencedMessagesContainer.putIfAbsent(consumer.getFencePositionForKeyShared(), new ConcurrentSortedLongPairSet(128, 2));
                            subList.forEach(entry -> fencedMessagesContainer.get(consumer.getFencePositionForKeyShared()).add(entry.getLedgerId(), entry.getEntryId()));
                            entriesWithSameKey.getValue().removeAll(subList);
                            if (entriesWithSameKey.getValue().size() == 0) {
                                iterator.remove();
                            }
                            if (keyNumbers.decrementAndGet() == 0) {
                                readMoreFuture.complete(null);
                            }
                            continue;
                        } else {
                            // Only deliver the entries that the position of the entries are lower that fence position of the consumer
                            // This is usually caused by the message redelivery(consumer disconnect from the broker).
                            subList = subList.stream().filter(entry ->
                                    ((PositionImpl) entry.getPosition()).compareTo(consumer.getFencePositionForKeyShared()) <= 0)
                                    .collect(Collectors.toList());
                        }
                    }
                }

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                EntryBatchSizes batchSizes = EntryBatchSizes.get(subList.size());
                filterEntriesForConsumer(subList, batchSizes, sendMessageInfo);

                consumer.sendMessages(subList, batchSizes, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), getRedeliveryTracker()).addListener(future -> {
                    if (keyNumbers.decrementAndGet() == 0) {
                        readMoreFuture.complete(null);
                    }
                });

                for (int i = 0; i < messagesForC; i++) {
                    entriesWithSameKey.getValue().remove(0);
                }

                TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this, -sendMessageInfo.getTotalMessages());
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();

                if (entriesWithSameKey.getValue().size() == 0) {
                    iterator.remove();
                }
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

        if (groupedEntries.size() > 0) {
            int laterReplay = 0;
            for (List<Entry> entryList : groupedEntries.values()) {
                laterReplay += entryList.size();
                entryList.forEach(entry -> {
                    messagesToRedeliver.add(entry.getLedgerId(), entry.getEntryId());
                    entry.release();
                });
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                        laterReplay);
            }
        }

        readMoreFuture.thenAccept(v -> readMoreEntries());
    }

    @Override
    public SubType getType() {
        return SubType.Key_Shared;
    }

    @Override
    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);

    @Override
    public void onMarkDeletePositionChanged(PositionImpl markDeletePosition) {
        boolean hasLifted = checkAndLiftMessages(markDeletePosition);
        if (hasLifted) {
            readMoreEntries();
        }
    }

    private boolean checkAndLiftMessages(PositionImpl markDeletePosition) {
        boolean hasLifted = false;
        if (selector instanceof HashRangeAutoSplitStickyKeyConsumerSelector) {
            // If the mark delete position of the fenced messages is lower than current mark delete position of the cursor.
            // the fenced messages should be lifted. To lift fenced messages, it's simpler to move the fenced messages to
            // messagesToRedeliver and reset the read position. When new read entries triggered, the fenced messages will be
            // deliver to consumers.
            if (!fencedMessagesContainer.isEmpty()) {
                Iterator<Map.Entry<PositionImpl, ConcurrentSortedLongPairSet>> iterator = fencedMessagesContainer.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<PositionImpl, ConcurrentSortedLongPairSet> entry = iterator.next();
                    if (markDeletePosition.getNext().compareTo(entry.getKey()) >= 0) {
                        entry.getValue().forEach((ledgerId, entryId) -> messagesToRedeliver.add(ledgerId, entryId));
                        iterator.remove();
                        hasLifted = true;
                    } else {
                        break;
                    }
                }
            }
        }
        return hasLifted;
    }

    private int getFencedMessages() {
        int size = 0;
        for (ConcurrentSortedLongPairSet value : fencedMessagesContainer.values()) {
            size += value.size();
        }
        return size;
    }
}
