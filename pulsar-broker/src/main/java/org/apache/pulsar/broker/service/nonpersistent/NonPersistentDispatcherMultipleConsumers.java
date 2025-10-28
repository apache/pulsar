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
package org.apache.pulsar.broker.service.nonpersistent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class NonPersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers
        implements NonPersistentDispatcher {

    private final NonPersistentTopic topic;
    protected final Subscription subscription;

    private CompletableFuture<Void> closeFuture = null;
    protected final String name;
    protected final Rate msgDrop;
    protected static final AtomicIntegerFieldUpdater<NonPersistentDispatcherMultipleConsumers>
            TOTAL_AVAILABLE_PERMITS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(NonPersistentDispatcherMultipleConsumers.class, "totalAvailablePermits");
    @SuppressWarnings("unused")
    private volatile int totalAvailablePermits = 0;

    private final RedeliveryTracker redeliveryTracker;

    public NonPersistentDispatcherMultipleConsumers(NonPersistentTopic topic, Subscription subscription) {
        super(subscription, topic.getBrokerService().pulsar().getConfiguration());
        this.topic = topic;
        this.subscription = subscription;
        this.name = topic.getName() + " / " + subscription.getName();
        this.msgDrop = new Rate();
        this.redeliveryTracker = RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", name, consumer);
            consumer.disconnect();
            return CompletableFuture.completedFuture(null);
        }

        if (isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit", name);
            return FutureUtil.failedFuture(new ConsumerBusyException("Subscription reached max consumers limit"));
        }

        consumerList.add(consumer);
        consumerSet.add(consumer);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected boolean isConsumersExceededOnSubscription() {
        return isConsumersExceededOnSubscription(topic, consumerList.size());
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        if (consumerSet.removeAll(consumer) == 1) {
            consumerList.remove(consumer);
            log.info("Removed consumer {}", consumer);
            if (consumerList.isEmpty()) {
                if (closeFuture != null) {
                    log.info("[{}] All consumers removed. Subscription is disconnected", name);
                    closeFuture.complete(null);
                }
                TOTAL_AVAILABLE_PERMITS_UPDATER.set(this, 0);
            } else {
                TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -consumer.getAvailablePermits());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
            }
            TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, -consumer.getAvailablePermits());
        }
    }

    @Override
    public boolean isConsumerConnected() {
        return !consumerList.isEmpty();
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumerList;
    }

    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return consumerList.size() == 1 && consumerSet.contains(consumer);
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);
        return disconnectAllConsumers();
    }

    @Override
    public synchronized void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        if (!consumerSet.contains(consumer)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring flow control from disconnected consumer {}", name, consumer);
            }
            return;
        }

        TOTAL_AVAILABLE_PERMITS_UPDATER.addAndGet(this, additionalNumberOfMessages);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Trigger new read after receiving flow control message", consumer);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
            consumerList.forEach(Consumer::disconnect);
        }
        return closeFuture;
    }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        return disconnectAllConsumers(isResetCursor);
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    @Override
    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public RedeliveryTracker getRedeliveryTracker() {
        return redeliveryTracker;
    }

    @Override
    public synchronized void sendMessages(List<Entry> entries) {
        Consumer consumer = TOTAL_AVAILABLE_PERMITS_UPDATER.get(this) > 0 ? getNextConsumer() : null;
        if (consumer != null) {
            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
            filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false, consumer);
            consumer.sendMessages(entries, batchSizes, null, sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), getRedeliveryTracker());

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

    @Override
    public boolean hasPermits() {
        return TOTAL_AVAILABLE_PERMITS_UPDATER.get(this) > 0;
    }

    @Override
    public Rate getMessageDropRate() {
        return msgDrop;
    }

    @Override
    public boolean isConsumerAvailable(Consumer consumer) {
        return consumer != null && consumer.getAvailablePermits() > 0 && consumer.isWritable();
    }

    @Override
    protected void reScheduleRead() {
        // No-op
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentDispatcherMultipleConsumers.class);

}
