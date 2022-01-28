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
package org.apache.pulsar.broker.service;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class AbstractDispatcherMultipleConsumers extends AbstractBaseDispatcher {

    protected final CopyOnWriteArrayList<Consumer> consumerList = new CopyOnWriteArrayList<>();
    protected final ObjectSet<Consumer> consumerSet = new ObjectHashSet<>();
    protected volatile int currentConsumerRoundRobinIndex = 0;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    protected static final AtomicIntegerFieldUpdater<AbstractDispatcherMultipleConsumers> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater
                    .newUpdater(AbstractDispatcherMultipleConsumers.class, "isClosed");
    private volatile int isClosed = FALSE;

    private Random random = new Random(42);

    protected AbstractDispatcherMultipleConsumers(Subscription subscription, ServiceConfiguration serviceConfig) {
        super(subscription, serviceConfig);
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

    public boolean isClosed() {
        return isClosed == TRUE;
    }

    public SubType getType() {
        return SubType.Shared;
    }

    public abstract boolean isConsumerAvailable(Consumer consumer);

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
    public Consumer getNextConsumer() {
        if (consumerList.isEmpty() || IS_CLOSED_UPDATER.get(this) == TRUE) {
            // abort read if no consumers are connected or if disconnect is initiated
            return null;
        }

        if (currentConsumerRoundRobinIndex >= consumerList.size()) {
            currentConsumerRoundRobinIndex = 0;
        }

        int currentRoundRobinConsumerPriority = consumerList.get(currentConsumerRoundRobinIndex).getPriorityLevel();

        // first find available-consumer on higher level unless currentIndex is not on highest level which is 0
        if (currentRoundRobinConsumerPriority != 0) {
            int higherPriorityConsumerIndex = getConsumerFromHigherPriority(currentRoundRobinConsumerPriority);
            if (higherPriorityConsumerIndex != -1) {
                currentConsumerRoundRobinIndex = higherPriorityConsumerIndex + 1;
                return consumerList.get(higherPriorityConsumerIndex);
            }
        }

        // currentIndex is already on highest level or couldn't find consumer on higher level so, find consumer on same
        // or lower level
        int availableConsumerIndex = getNextConsumerFromSameOrLowerLevel(currentConsumerRoundRobinIndex);
        if (availableConsumerIndex != -1) {
            currentConsumerRoundRobinIndex = availableConsumerIndex + 1;
            return consumerList.get(availableConsumerIndex);
        }

        // couldn't find available consumer
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

        return consumerList.get(random.nextInt(consumerList.size()));
    }


    /**
     * Finds index of first available consumer which has higher priority then given targetPriority.
     *
     * @param targetPriority
     * @return -1 if couldn't find any available consumer
     */
    private int getConsumerFromHigherPriority(int targetPriority) {
        for (int i = 0; i < currentConsumerRoundRobinIndex; i++) {
            Consumer consumer = consumerList.get(i);
            if (consumer.getPriorityLevel() < targetPriority) {
                if (isConsumerAvailable(consumerList.get(i))) {
                    return i;
                }
            } else {
                break;
            }
        }
        return -1;
    }

    /**
     * Finds index of round-robin available consumer that present on same level as consumer on
     * currentRoundRobinIndex if doesn't find consumer on same level then it finds first available consumer on lower
     * priority level else returns
     * index=-1 if couldn't find any available consumer in the list.
     *
     * @param currentRoundRobinIndex
     * @return
     */
    private int getNextConsumerFromSameOrLowerLevel(int currentRoundRobinIndex) {

        int targetPriority = consumerList.get(currentRoundRobinIndex).getPriorityLevel();
        // use to do round-robin if can't find consumer from currentRR to last-consumer in list
        int scanIndex = currentRoundRobinIndex;
        int endPriorityLevelIndex = currentRoundRobinIndex;
        do {
            Consumer scanConsumer = scanIndex < consumerList.size() ? consumerList.get(scanIndex)
                    : null /* reached to last consumer of list */;

            // if reached to last consumer of list then check from beginning to currentRRIndex of the list
            if (scanConsumer == null || scanConsumer.getPriorityLevel() != targetPriority) {
                endPriorityLevelIndex = scanIndex; // last consumer on this level
                scanIndex = getFirstConsumerIndexOfPriority(targetPriority);
            } else {
                if (isConsumerAvailable(scanConsumer)) {
                    return scanIndex;
                }
                scanIndex++;
            }
        } while (scanIndex != currentRoundRobinIndex);

        // it means: didn't find consumer in the same priority-level so, check available consumer lower than this level
        for (int i = endPriorityLevelIndex; i < consumerList.size(); i++) {
            if (isConsumerAvailable(consumerList.get(i))) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds index of first consumer in list which has same priority as given targetPriority.
     *
     * @param targetPriority
     * @return
     */
    private int getFirstConsumerIndexOfPriority(int targetPriority) {
        for (int i = 0; i < consumerList.size(); i++) {
            if (consumerList.get(i).getPriorityLevel() == targetPriority) {
                return i;
            }
        }
        return -1;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);


}
