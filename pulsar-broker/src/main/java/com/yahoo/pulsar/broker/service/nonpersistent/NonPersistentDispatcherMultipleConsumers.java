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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.bookkeeper.mledger.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.yahoo.pulsar.broker.service.BrokerServiceException;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

/**
 */
public class NonPersistentDispatcherMultipleConsumers implements NonPersistentDispatcher {

    private final NonPersistentTopic topic;
    private final CopyOnWriteArrayList<Consumer> consumerList = new CopyOnWriteArrayList<>();
    private final ObjectSet<Consumer> consumerSet = new ObjectHashSet<>();
    private int currentConsumerRoundRobinIndex = 0;

    private CompletableFuture<Void> closeFuture = null;

    private final String name;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<NonPersistentDispatcherMultipleConsumers> IS_CLOSED_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(NonPersistentDispatcherMultipleConsumers.class, "isClosed");
    private volatile int isClosed = FALSE;

    public NonPersistentDispatcherMultipleConsumers(NonPersistentTopic topic, String dispatcherName) {
        this.name = topic.getName() + " / " + dispatcherName;
        this.topic = topic;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer ", name, consumer);
            consumer.disconnect();
            return;
        }

        consumerList.add(consumer);
        consumerSet.add(consumer);
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
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Trying to remove a non-connected consumer: {}", name, consumer);
            }
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
    public synchronized CompletableFuture<Void> disconnectAllConsumers() {
        closeFuture = new CompletableFuture<>();
        if (consumerList.isEmpty()) {
            closeFuture.complete(null);
        } else {
            consumerList.forEach(Consumer::disconnect);
        }
        return closeFuture;
    }

    @Override
    public void reset() {
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    @Override
    public SubType getType() {
        return SubType.Shared;
    }

    @Override
    public void sendMessages(List<Entry> entries) {
        Consumer consumer = getNextConsumer();
        if (consumer != null) {
            consumer.sendMessages(entries, false);
        } else {
            entries.forEach(Entry::release);
        }
    }

    private Consumer getNextConsumer() {
        if (consumerList.isEmpty() || closeFuture != null) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (currentConsumerRoundRobinIndex >= consumerList.size()) {
            currentConsumerRoundRobinIndex = 0;
        }

        // find next available consumer
        int availableConsumerIndex = currentConsumerRoundRobinIndex;
        do {
            if (consumerList.get(availableConsumerIndex).getAvailablePermits() > 0) {
                currentConsumerRoundRobinIndex = availableConsumerIndex;
                return consumerList.get(currentConsumerRoundRobinIndex++);
            }
            if (++availableConsumerIndex >= consumerList.size()) {
                availableConsumerIndex = 0;
            }
        } while (availableConsumerIndex != currentConsumerRoundRobinIndex);

        // not found any consumer
        return null;
    }

    private static final Logger log = LoggerFactory.getLogger(NonPersistentDispatcherMultipleConsumers.class);

}
