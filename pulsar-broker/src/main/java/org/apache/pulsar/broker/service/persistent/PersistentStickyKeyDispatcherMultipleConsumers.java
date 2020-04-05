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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    private final StickyKeyConsumerSelector selector;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
           Subscription subscription, StickyKeyConsumerSelector selector) {
        super(topic, cursor, subscription);
        this.selector = selector;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
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
        final Map<Integer, List<Entry>> groupedEntries = new HashMap<>();
        for (Entry entry : entries) {
            int key = Murmur3_32Hash.getInstance().makeHash(peekStickyKey(entry.getDataBuffer())) % selector.getRangeSize();
            groupedEntries.putIfAbsent(key, new ArrayList<>());
            groupedEntries.get(key).add(entry);
        }
        final Iterator<Map.Entry<Integer, List<Entry>>> iterator = groupedEntries.entrySet().iterator();
        AtomicInteger keyNumbers = new AtomicInteger(groupedEntries.size());
        while (iterator.hasNext() && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
            final Map.Entry<Integer, List<Entry>> entriesWithSameKey = iterator.next();
            //TODO: None key policy
            Consumer consumer = selector.selectByIndex(entriesWithSameKey.getKey());
            if (consumer == null) {
                // Do nothing, cursor will be rewind at reconnection
                log.info("[{}] rewind because no available consumer found for key {} from total {}", name,
                        entriesWithSameKey.getKey(), consumerList.size());
                entriesWithSameKey.getValue().forEach(Entry::release);
                cursor.rewind();
                return;
            }

            int messagesForC = Math.min(entriesWithSameKey.getValue().size(), consumer.getAvailablePermits());
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

                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                EntryBatchSizes batchSizes = EntryBatchSizes.get(subList.size());
                filterEntriesForConsumer(subList, batchSizes, sendMessageInfo);

                consumer.sendMessages(subList, batchSizes, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), getRedeliveryTracker()).addListener(future -> {
                            if (future.isSuccess() && keyNumbers.decrementAndGet() == 0) {
                                readMoreEntries();
                            }
                });
                entriesWithSameKey.getValue().removeAll(subList);

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

}
