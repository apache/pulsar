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

import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Selects from consumers ordered by increasing priority number, with round-robin selection within a priority.
 * The caller owns the cursor and must prevent list mutations during selection, as in the dispatcher.
 * Package-private and generic so tests and microbenchmarks can use lightweight consumers.
 */
final class ConsumerPrioritySelector<T> {
    private final List<T> consumerList;
    private final ToIntFunction<T> priority;
    private final Predicate<T> available;

    ConsumerPrioritySelector(List<T> consumerList, ToIntFunction<T> priority, Predicate<T> available) {
        this.consumerList = consumerList;
        this.priority = priority;
        this.available = available;
    }

    // The caller supplies an in-range cursor and advances it only after a successful selection.
    int select(int currentConsumerRoundRobinIndex) {
        int currentRoundRobinConsumerPriority = priority.applyAsInt(consumerList.get(currentConsumerRoundRobinIndex));

        // first find available-consumer on higher level unless currentIndex is not on highest level which is 0
        if (currentRoundRobinConsumerPriority != 0) {
            int higherPriorityConsumerIndex = getConsumerFromHigherPriority(
                    currentRoundRobinConsumerPriority, currentConsumerRoundRobinIndex);
            if (higherPriorityConsumerIndex != -1) {
                return higherPriorityConsumerIndex;
            }
        }

        // currentIndex is already on highest level or couldn't find consumer on higher level so, find consumer on same
        // or lower level
        int availableConsumerIndex = getNextConsumerFromSameOrLowerLevel(currentConsumerRoundRobinIndex);
        if (availableConsumerIndex != -1) {
            return availableConsumerIndex;
        }

        // couldn't find available consumer
        return -1;
    }

    /**
     * Finds index of first available consumer which has higher priority then given targetPriority.
     *
     * @param targetPriority
     * @return -1 if couldn't find any available consumer
     */
    private int getConsumerFromHigherPriority(int targetPriority, int currentConsumerRoundRobinIndex) {
        for (int i = 0; i < currentConsumerRoundRobinIndex; i++) {
            T consumer = consumerList.get(i);
            if (priority.applyAsInt(consumer) < targetPriority) {
                if (available.test(consumerList.get(i))) {
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
        T currentRRConsumer = consumerList.get(currentRoundRobinIndex);
        if (available.test(currentRRConsumer)) {
            return currentRoundRobinIndex;
        }

        // scan the consumerList, if consumer in currentRoundRobinIndex is unavailable
        int targetPriority = priority.applyAsInt(currentRRConsumer);
        int scanIndex = currentRoundRobinIndex + 1;
        int endPriorityLevelIndex = currentRoundRobinIndex;
        do {
            T scanConsumer = scanIndex < consumerList.size() ? consumerList.get(scanIndex)
                    : null /* reached to last consumer of list */;

            // if reached to last consumer of list then check from beginning to currentRRIndex of the list
            if (scanConsumer == null || priority.applyAsInt(scanConsumer) != targetPriority) {
                endPriorityLevelIndex = scanIndex; // last consumer on this level
                scanIndex = getFirstConsumerIndexOfPriority(targetPriority);
            } else {
                if (available.test(scanConsumer)) {
                    return scanIndex;
                }
                scanIndex++;
            }
        } while (scanIndex != currentRoundRobinIndex);

        // it means: didn't find consumer in the same priority-level so, check available consumer lower than this level
        for (int i = endPriorityLevelIndex; i < consumerList.size(); i++) {
            if (available.test(consumerList.get(i))) {
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
            if (priority.applyAsInt(consumerList.get(i)) == targetPriority) {
                return i;
            }
        }
        return -1;
    }

}
