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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonPersistentStickyKeyDispatcherMultipleConsumers extends NonPersistentDispatcherMultipleConsumers {

    private final StickyKeyConsumerSelector selector;
    private final KeySharedMode keySharedMode;

    public NonPersistentStickyKeyDispatcherMultipleConsumers(NonPersistentTopic topic, Subscription subscription,
                                                             KeySharedMeta ksm) {
        super(topic, subscription);
        this.keySharedMode = ksm.getKeySharedMode();
        switch (this.keySharedMode) {
            case STICKY:
                this.selector = new HashRangeExclusiveStickyKeyConsumerSelector();
                break;

            case AUTO_SPLIT:
            default:
                ServiceConfiguration conf = topic.getBrokerService().getPulsar().getConfiguration();
                if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                    this.selector = new ConsistentHashingStickyKeyConsumerSelector(
                            conf.getSubscriptionKeySharedConsistentHashingReplicaPoints());
                } else {
                    this.selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
                }
                break;
        }
    }

    @VisibleForTesting
    NonPersistentStickyKeyDispatcherMultipleConsumers(NonPersistentTopic topic, Subscription subscription,
                                                             StickyKeyConsumerSelector selector) {
        super(topic, subscription);
        if (selector instanceof HashRangeExclusiveStickyKeyConsumerSelector) {
            keySharedMode = KeySharedMode.STICKY;
        } else if (selector instanceof ConsistentHashingStickyKeyConsumerSelector
                || selector instanceof HashRangeAutoSplitStickyKeyConsumerSelector) {
            keySharedMode = KeySharedMode.AUTO_SPLIT;
        } else {
            keySharedMode = null;
        }
        this.selector = selector;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
            consumer.disconnect();
            return;
        }
        super.addConsumer(consumer);
        try {
            selector.addConsumer(consumer);
        } catch (BrokerServiceException e) {
            consumerSet.removeAll(consumer);
            consumerList.remove(consumer);
            throw e;
        }
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

    private static final FastThreadLocal<Map<Consumer, List<Entry>>> localGroupedEntries =
            new FastThreadLocal<Map<Consumer, List<Entry>>>() {
                @Override
                protected Map<Consumer, List<Entry>> initialValue() throws Exception {
                    return new HashMap<>();
                }
            };

    @Override
    public void sendMessages(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        if (consumerSet.isEmpty()) {
            entries.forEach(Entry::release);
            return;
        }

        final Map<Consumer, List<Entry>> groupedEntries = localGroupedEntries.get();
        groupedEntries.clear();

        for (Entry entry : entries) {
            Consumer consumer = selector.select(peekStickyKey(entry.getDataBuffer()));
            if (consumer != null) {
                groupedEntries.computeIfAbsent(consumer, k -> new ArrayList<>()).add(entry);
            } else {
                entry.release();
            }
        }

        for (Map.Entry<Consumer, List<Entry>> entriesByConsumer : groupedEntries.entrySet()) {
            Consumer consumer = entriesByConsumer.getKey();
            List<Entry> entriesForConsumer = entriesByConsumer.getValue();

            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entriesForConsumer.size());
            filterEntriesForConsumer(entriesForConsumer, batchSizes, sendMessageInfo, null, null, false, consumer);

            if (consumer.getAvailablePermits() > 0 && consumer.isWritable()) {
                consumer.sendMessages(entriesForConsumer, batchSizes, null, sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                        getRedeliveryTracker());
                TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -sendMessageInfo.getTotalMessages());
            } else {
                entriesForConsumer.forEach(e -> {
                    int totalMsgs = Commands.getNumberOfMessagesInBatch(e.getDataBuffer(), subscription.toString(), -1);
                    if (totalMsgs > 0) {
                        msgDrop.recordEvent(totalMsgs);
                    }
                    e.release();
                });
            }
        }
    }

    public KeySharedMode getKeySharedMode() {
        return keySharedMode;
    }

    public boolean hasSameKeySharedPolicy(KeySharedMeta ksm) {
        return (ksm.getKeySharedMode() == this.keySharedMode);
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentStickyKeyDispatcherMultipleConsumers.class);
}
