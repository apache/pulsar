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
package org.apache.pulsar.broker.service.nonpersistent;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NonPersistentStickyKeyDispatcherMultipleConsumers extends NonPersistentDispatcherMultipleConsumers {

    private final StickyKeyConsumerSelector selector;

    public NonPersistentStickyKeyDispatcherMultipleConsumers(NonPersistentTopic topic, Subscription subscription,
                                                             StickyKeyConsumerSelector selector) {
        super(topic, subscription);
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
    public SubType getType() {
        return SubType.Key_Shared;
    }

    @Override
    public void sendMessages(List<Entry> entries) {
        if (entries.size() > 0) {
            final Map<Integer, List<Entry>> groupedEntries = new HashMap<>();
            for (Entry entry : entries) {
                int key = Murmur3_32Hash.getInstance().makeHash(peekStickyKey(entry.getDataBuffer())) % selector.getRangeSize();
                groupedEntries.putIfAbsent(key, new ArrayList<>());
                groupedEntries.get(key).add(entry);
            }
            final Iterator<Map.Entry<Integer, List<Entry>>> iterator = groupedEntries.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Integer, List<Entry>> entriesWithSameKey = iterator.next();
                //TODO: None key policy
                Consumer consumer = selector.selectByIndex(entriesWithSameKey.getKey());
                if (consumer != null) {
                    SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                    EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesWithSameKey.getValue().size());
                    filterEntriesForConsumer(entriesWithSameKey.getValue(), batchSizes, sendMessageInfo);
                    consumer.sendMessages(entriesWithSameKey.getValue(), batchSizes, sendMessageInfo.getTotalMessages(),
                            sendMessageInfo.getTotalBytes(), getRedeliveryTracker());
                    TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -sendMessageInfo.getTotalMessages());
                } else {
                    entries.forEach(entry -> {
                        int totalMsgs = Commands.getNumberOfMessagesInBatch(entry.getDataBuffer(), subscription.toString(), -1);
                        if (totalMsgs > 0) {
                            msgDrop.recordEvent(totalMsgs);
                        }
                        entry.release();
                    });
                }
            }
        }
    }
}
