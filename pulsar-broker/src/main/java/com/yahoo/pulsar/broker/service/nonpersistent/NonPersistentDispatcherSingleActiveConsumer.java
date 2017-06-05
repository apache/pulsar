/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service.nonpersistent;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.bookkeeper.mledger.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.service.BrokerServiceException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import com.yahoo.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

public final class NonPersistentDispatcherSingleActiveConsumer implements NonPersistentDispatcher {

    private final NonPersistentTopic topic;
    private static final AtomicReferenceFieldUpdater<NonPersistentDispatcherSingleActiveConsumer, Consumer> ACTIVE_CONSUMER_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(NonPersistentDispatcherSingleActiveConsumer.class, Consumer.class, "activeConsumer");
    private volatile Consumer activeConsumer = null;
    private final CopyOnWriteArrayList<Consumer> consumers;
    private CompletableFuture<Void> closeFuture = null;
    private final int partitionIndex;

    // This dispatcher supports both the Exclusive and Failover subscription types
    private final SubType subscriptionType;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<NonPersistentDispatcherSingleActiveConsumer> IS_CLOSED_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(NonPersistentDispatcherSingleActiveConsumer.class, "isClosed");
    private volatile int isClosed = FALSE;

    public NonPersistentDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
            NonPersistentTopic topic) {
        this.topic = topic;
        this.consumers = new CopyOnWriteArrayList<>();
        this.partitionIndex = partitionIndex;
        this.subscriptionType = subscriptionType;
        ACTIVE_CONSUMER_UPDATER.set(this, null);
    }

    private void pickAndScheduleActiveConsumer() {
        checkArgument(!consumers.isEmpty());

        consumers.sort((c1, c2) -> c1.consumerName().compareTo(c2.consumerName()));

        int index = partitionIndex % consumers.size();
        Consumer prevConsumer = ACTIVE_CONSUMER_UPDATER.getAndSet(this, consumers.get(index));

        if (prevConsumer == ACTIVE_CONSUMER_UPDATER.get(this)) {
            // Active consumer did not change. Do nothing at this point
            return;
        }

    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer ", this.topic.getName(), consumer);
            consumer.disconnect();
        }
        if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
            throw new ConsumerBusyException("Exclusive consumer is already connected");
        }

        consumers.add(consumer);

        // Pick an active consumer and start it
        pickAndScheduleActiveConsumer();

    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        log.info("Removing consumer {}", consumer);
        if (!consumers.remove(consumer)) {
            throw new ServerMetadataException("Consumer was not connected");
        }

        if (consumers.isEmpty()) {
            ACTIVE_CONSUMER_UPDATER.set(this, null);
        }

        if (closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
            return;
        }

        if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
            // Control reaches here only when closeFuture is created
            // and no more connected consumers left.
            closeFuture.complete(null);
        }
    }

    /**
     * Handle unsubscribe command from the client API For failover subscription, if consumer is connected consumer, we
     * can unsubscribe.
     *
     * @param consumer
     *            Calling consumer object
     */
    @Override
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return (consumers.size() == 1) && Objects.equals(consumer, ACTIVE_CONSUMER_UPDATER.get(this));
    }

    @Override
    public CompletableFuture<Void> close() {
        IS_CLOSED_UPDATER.set(this, TRUE);
        return disconnectAllConsumers();
    }

    /**
     * Disconnect all consumers on this dispatcher (server side close). This triggers channelInactive on the inbound
     * handler which calls dispatcher.removeConsumer(), where the closeFuture is completed
     *
     * @return
     */
    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers() {
        closeFuture = new CompletableFuture<>();

        if (!consumers.isEmpty()) {
            consumers.forEach(Consumer::disconnect);
        } else {
            // no consumer connected, complete disconnect immediately
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    @Override
    public void reset() {
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public boolean isConsumerConnected() {
        return ACTIVE_CONSUMER_UPDATER.get(this) != null;
    }

    @Override
    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumers;
    }

    @Override
    public SubType getType() {
        return subscriptionType;
    }

    public Consumer getActiveConsumer() {
        return ACTIVE_CONSUMER_UPDATER.get(this);
    }

    @Override
    public void sendMessages(List<Entry> entries) {
        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (currentConsumer != null) {
            currentConsumer.sendMessages(entries, false);
        } else {
            entries.forEach(Entry::release);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentDispatcherSingleActiveConsumer.class);

}
