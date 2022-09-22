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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;

/***
 * When there are two consumers, users can specify the consumption behavior of each consumer by `Entry filter`:
 * - `case-1`: `consumer_1` can consume 60% of the messages, `consumer_2` can consume 60% of the messages, and there
 * is 10% intersection between `consumer_1` and `consumer_2`.
 * - `case-2`: `consumer_1` can consume 40% of the messages, `consumer_2` can consume 40% of the messages, and no
 * consumer can consume the remaining 20%.
 *
 * In case-1, when users use `FilterResult.RESCHEDULE `, and if the message that can only be consumed by `consumer_1`
 * is delivered to `consumer_2` all the time, and the message that can only be consumed by `consumer_2` is delivered
 * to `consumer_1` all the time, then the problem occurs:
 * - Both consumers can not receive messages anymore.
 * - The number of redeliveries of entries has been increasing ( redelivery by Entry Filter ).
 *
 * So {@link InMemoryAndPreventCycleFilterRedeliveryTracker} solve this problem by this way:
 * When a message is redelivered by the same consumer more than 3 times, the consumption of this message by that
 * consumer is paused for 1 second. Since tracking the consumption of all the messages cost memory too much, we trace
 * only the messages with the smallest position.
 */
public class InMemoryAndPreventCycleFilterRedeliveryTracker extends InMemoryRedeliveryTracker {

    /** The first redelivery record. **/
    @Getter
    private volatile Position redeliveryStartAt;
    /**
     * key: The consumer calls redelivery at {@link #redeliveryStartAt}.
     * value: The number of times the consumer calls redelivery.
     */
    private final ConcurrentHashMap<Consumer, AtomicInteger> earliestEntryRedeliveryCountMapping =
            new ConcurrentHashMap<>();
    /**
     *  key: paused consumer.
     *  value: pause because of which position.
     */
    private final ConcurrentHashMap<Consumer, PauseConsumerInformation> pausedConsumers = new ConcurrentHashMap<>();

    @Override
    public int incrementAndGetRedeliveryCount(Position position, Consumer consumer) {
        int newCount = super.incrementAndGetRedeliveryCount(position, consumer);
        // Diff with super implementation: record how many times this consumer calls "redelivery" at earliest position.
        if (position == null || consumer == null){
            return newCount;
        }
        Position originalEarliestPosition = redeliveryStartAt;
        Position actualEarliestPosition = null;
        if (originalEarliestPosition == null) {
            cleanEarliestInformation();
            actualEarliestPosition = position;
        } else {
            int isEarlier = comparePosition(originalEarliestPosition, position);
            if (isEarlier < 0) {
                return newCount;
            } else if (isEarlier > 0) {
                cleanEarliestInformation();
                actualEarliestPosition = position;
            } else {
                actualEarliestPosition = originalEarliestPosition;
            }
        }
        redeliveryStartAt = actualEarliestPosition;
        int redeliveryCount = earliestEntryRedeliveryCountMapping.computeIfAbsent(consumer, c -> new AtomicInteger())
                .incrementAndGet();
        if (redeliveryCount >= 3) {
            pausedConsumers.put(consumer, new PauseConsumerInformation(actualEarliestPosition));
        } else {
        }
        return newCount;
    }

    @Override
    public void remove(Position position, Position markDeletedPosition) {
        super.remove(position, markDeletedPosition);
        if (redeliveryStartAt == null || comparePosition(redeliveryStartAt, position) == 0
                || comparePosition(redeliveryStartAt, markDeletedPosition) >= 0) {
            cleanEarliestInformation();
        }
    }

    @Override
    public void clear() {
        super.clear();
        cleanEarliestInformation();
    }

    public boolean isConsumerPaused(Consumer consumer) {
        if (consumer == null) {
            return false;
        }
        PauseConsumerInformation pauseConsumerInformation = pausedConsumers.get(consumer);
        if (pauseConsumerInformation == null) {
            return false;
        }
        if (!pauseConsumerInformation.isValid(redeliveryStartAt)) {
            pausedConsumers.remove(consumer);
            return false;
        }
        return true;
    }

    public int pausedConsumerCount() {
        return (int) pausedConsumers.keySet().stream().map(this::isConsumerPaused).count();
    }

    private void cleanEarliestInformation() {
        redeliveryStartAt = null;
        // If consumer has been closed, remove this consumer.
        List<Consumer> closedConsumers = earliestEntryRedeliveryCountMapping.keySet().stream()
                .filter(Consumer::isClosed).toList();
        closedConsumers.forEach(earliestEntryRedeliveryCountMapping::remove);
        // Just reset counter to 0, because value will be removed if consumer is closed.
        earliestEntryRedeliveryCountMapping.values().forEach(i -> i.set(0));
        pausedConsumers.clear();
    }

    private static int comparePosition(Position pos1, Position pos2) {
        int ledgerCompare = Long.compare(pos1.getLedgerId(), pos2.getLedgerId());
        if (ledgerCompare != 0) {
            return ledgerCompare;
        }
        return Long.compare(pos1.getEntryId(), pos2.getEntryId());
    }

    private static class PauseConsumerInformation {

        private final Position cantConsumedPosition;

        private final long pauseTime;

        private PauseConsumerInformation(Position cantConsumedPosition) {
            this.cantConsumedPosition = cantConsumedPosition;
            this.pauseTime = System.currentTimeMillis();
        }

        /**
         * If "do pause consumer" and {@link #cleanEarliestInformation} concurrently, it is possible to pause
         * consumer that no longer needs to be paused. The way to distinguish is to determine whether
         * "cantConsumedPosition equal to currentRedeliveryStartAt".
         * {@link PauseConsumerInformation#isValid(Position)} resolved this problem.
         */
        boolean isValid(Position currentRedeliveryStartAt) {
            if (cantConsumedPosition != currentRedeliveryStartAt) {
                return false;
            }
            // Automatically becomes invalid after 1s, because users may use time to filter the Entry.
            return System.currentTimeMillis() - pauseTime < 1000;
        }
    }
}
