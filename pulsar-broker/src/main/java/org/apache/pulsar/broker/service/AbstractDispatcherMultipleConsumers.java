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
package org.apache.pulsar.broker.service;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;

/**
 *
 */
public abstract class AbstractDispatcherMultipleConsumers extends AbstractBaseDispatcher {

    // Use the membership helpers below for mutations so the selector priority counts stay consistent.
    protected final CopyOnWriteArrayList<Consumer> consumerList = new CopyOnWriteArrayList<>();
    private final ConsumerPrioritySelector<Consumer> consumerPrioritySelector =
            new ConsumerPrioritySelector<>(consumerList, Consumer::getPriorityLevel, this::isConsumerAvailable);
    private final ObjectHashSet<Consumer> consumerSetImpl = new ObjectHashSet<>();
    protected final ObjectSet<Consumer> consumerSet = consumerSetImpl;
    protected volatile int currentConsumerRoundRobinIndex = 0;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    protected static final AtomicIntegerFieldUpdater<AbstractDispatcherMultipleConsumers> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater
                    .newUpdater(AbstractDispatcherMultipleConsumers.class, "isClosed");
    private volatile int isClosed = FALSE;

    protected AbstractDispatcherMultipleConsumers(Subscription subscription, ServiceConfiguration serviceConfig) {
        super(subscription, serviceConfig);
    }

    // Keep membership and priority counts under the same monitor used for selection.
    protected final synchronized void addConsumerToList(Consumer consumer) {
        consumerPrioritySelector.add(consumer);
    }

    protected final synchronized void removeConsumerFromList(Consumer consumer) {
        consumerPrioritySelector.remove(consumer);
    }

    protected final synchronized void removeConsumersFromList(Predicate<Consumer> predicate) {
        consumerPrioritySelector.removeIf(predicate);
    }

    public boolean isConsumerConnected() {
        return !consumerList.isEmpty();
    }

    public CopyOnWriteArrayList<Consumer> getConsumers() {
        return consumerList;
    }

    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return consumerList.size() == 1 && consumerSet.contains(consumer);
    }

    /**
     * Checks whether the exact Consumer instance is still connected.
     *
     * <p>This differs from {@link ObjectSet#contains(Object)}, which uses {@link Consumer#equals(Object)} and can
     * match a replacement Consumer that reuses the same protocol identity.
     * The caller must hold the dispatcher monitor while checking membership and acting on the result.
     */
    protected final boolean containsConsumerInstance(Consumer consumer) {
        int index = consumerSetImpl.indexOf(consumer);
        return consumerSetImpl.indexExists(index) && consumerSetImpl.indexGet(index) == consumer;
    }

    public boolean isClosed() {
        return isClosed == TRUE;
    }

    public SubType getType() {
        return SubType.Shared;
    }

    public abstract boolean isConsumerAvailable(Consumer consumer);

    /**
     * Cancel a possible pending read that is a Managed Cursor waiting to be notified for more entries.
     * This won't cancel any other pending reads that are currently in progress.
     */
    protected void cancelPendingRead() {}

    /**
     * <pre>
     * Broker gives more priority while dispatching messages. Here, broker follows descending priorities. (eg:
     * 0=max-priority, 1, 2,..)
     * <p>
     * Broker will first dispatch messages to max priority-level consumers if they
     * have permits, else broker will consider next priority level consumers.
     * Also on the same priority-level, it selects consumer in round-robin manner.
     * <p>
     * If subscription has consumer-A with  priorityLevel 1 and Consumer-B with priorityLevel 2
     * then broker will dispatch
     * messages to only consumer-A until it runs out permit and then broker starts dispatching messages to Consumer-B.
     * <p>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Result of getNextConsumer(): C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <pre>
     * <b>Algorithm:</b>
     * 1. consumerList: it stores consumers in sorted-list: max-priority stored first
     * 2. currentConsumerRoundRobinIndex: it always stores last served consumer-index
     *
     * Each time getNextConsumer() is called:<p>
     * 1. It always starts to traverse from the max-priority consumer (first element) from sorted-list
     * 2. Consumers on same priority-level will be treated equally and it tries to pick one of them in
     *    round-robin manner
     * 3. If consumer is not available on given priority-level then only it will go to the next lower priority-level
     *    consumers
     * 4. Returns null in case it doesn't find any available consumer
     * </pre>
     *
     * @return nextAvailableConsumer
     */
    public synchronized Consumer getNextConsumer() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (currentConsumerRoundRobinIndex >= consumerList.size()) {
            currentConsumerRoundRobinIndex = 0;
        }

        int availableConsumerIndex = consumerPrioritySelector.select(currentConsumerRoundRobinIndex);
        if (availableConsumerIndex != -1) {
            currentConsumerRoundRobinIndex = availableConsumerIndex + 1;
            return consumerList.get(availableConsumerIndex);
        }
        return null;
    }

    /**
     * Get random consumer from consumerList.
     *
     * @return null if no consumer available, else return random consumer from consumerList
     */
    public Consumer getRandomConsumer() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected of if disconnect is initiated
            return null;
        }

        return consumerList.get(ThreadLocalRandom.current().nextInt(consumerList.size()));
    }


}
