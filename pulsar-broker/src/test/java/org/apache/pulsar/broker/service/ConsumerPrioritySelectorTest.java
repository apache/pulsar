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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.testng.annotations.Test;

public class ConsumerPrioritySelectorTest {
    @Test
    public void selectionMatchesPriorityAndRoundRobinOrder() {
        // Every grouping of eight consumers, every availability mask, and every cursor.
        for (int boundaries = 0; boundaries < 128; boundaries++) {
            List<Slot> consumers = new CopyOnWriteArrayList<>();
            int priority = (boundaries & 1) == 0 ? 0 : 3;
            for (int i = 0; i < 8; i++) {
                if (i > 0 && (boundaries & (1 << (i - 1))) != 0) {
                    priority += 3;
                }
                consumers.add(new Slot(i, priority));
            }
            List<Integer> visited = new ArrayList<>();
            ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority, c -> {
                visited.add(c.index);
                return c.available;
            });
            for (int cursor = 0; cursor < consumers.size(); cursor++) {
                int start = cursor;
                int currentPriority = consumers.get(cursor).priority;
                List<Slot> ordered = new ArrayList<>(consumers);
                ordered.sort(Comparator.comparingInt((Slot c) -> c.priority)
                        .thenComparingInt(c -> c.priority == currentPriority && c.index < start ? 1 : 0)
                        .thenComparingInt(c -> c.index));
                for (int mask = 0; mask < 256; mask++) {
                    for (Slot c : consumers) {
                        c.available = (mask & (1 << c.index)) != 0;
                    }
                    List<Integer> expectedVisits = new ArrayList<>();
                    int expected = -1;
                    for (Slot c : ordered) {
                        expectedVisits.add(c.index);
                        if (c.available) {
                            expected = c.index;
                            break;
                        }
                    }
                    visited.clear();
                    assertThat(selector.select(cursor)).isEqualTo(expected);
                    assertThat(visited).isEqualTo(expectedVisits);
                }
            }
        }
    }

    @Test
    public void observesMembershipAndPermitChanges() {
        List<Slot> consumers = new CopyOnWriteArrayList<>();
        Slot first = new Slot(0, 0);
        Slot second = new Slot(1, 3);
        consumers.add(first);
        consumers.add(second);
        ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                c -> c.available);
        second.available = true;
        assertThat(selector.select(0)).isEqualTo(1);
        first.available = true;
        assertThat(selector.select(1)).isZero();
        selector.remove(first);
        assertThat(selector.select(0)).isZero();
        second.available = false;
        assertThat(selector.select(0)).isEqualTo(-1);
        selector.add(first);
        consumers.sort(Comparator.comparingInt(c -> c.priority));
        assertThat(selector.select(1)).isZero();
    }

    @Test
    public void zeroPriorityRetainsItsSpecialTreatment() {
        Slot negative = new Slot(0, -1);
        Slot zero = new Slot(1, 0);
        Slot lower = new Slot(2, 1);
        List<Slot> consumers = new CopyOnWriteArrayList<>(List.of(negative, zero, lower));
        ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                c -> c.available);
        negative.available = true;
        lower.available = true;
        // The original selector skips its higher-priority scan when the cursor priority is zero.
        assertThat(selector.select(1)).isEqualTo(2);
        lower.available = false;
        assertThat(selector.select(1)).isEqualTo(-1);
        zero.available = true;
        assertThat(selector.select(1)).isEqualTo(1);
        assertThat(selector.select(2)).isZero();
    }

    @Test
    public void fiftyConsumersAtTheSamePriority() {
        for (int priority : new int[]{0, 3}) {
            List<Slot> consumers = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 50; i++) {
                Slot consumer = new Slot(i, priority);
                consumer.available = true;
                consumers.add(consumer);
            }
            ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                    c -> c.available);
            int cursor = 25;
            for (int i = 0; i < 150; i++) {
                int expected = (25 + i) % 50;
                int selected = selector.select(cursor % consumers.size());
                assertThat(selected).isEqualTo(expected);
                cursor = selected + 1;
            }
            // Drain the group starting in its middle, including wrapping to earlier consumers.
            for (int i = 0; i < 50; i++) {
                int selected = selector.select(cursor % consumers.size());
                assertThat(selected).isEqualTo((25 + i) % 50);
                consumers.get(selected).available = false;
                cursor = selected + 1;
            }
            assertThat(selector.select(cursor)).isEqualTo(-1);
            consumers.get(7).available = true;
            assertThat(selector.select(cursor)).isEqualTo(7);
        }
    }

    @Test
    public void priorityCountsFollowActualRemovals() {
        List<Slot> consumers = new CopyOnWriteArrayList<>();
        ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                c -> c.available);
        Slot first = new Slot(0, 0);
        first.available = true;
        Slot second = new Slot(1, 3);
        Slot third = new Slot(2, 3);
        assertThat(selector.priorityLevelCount()).isZero();
        selector.add(first);
        selector.add(second);
        selector.add(third);
        assertThat(selector.priorityLevelCount()).isEqualTo(2);
        // Equal protocol identity does not imply equal priority: decrement the registered instance's count.
        selector.remove(new Slot(1, 99));
        assertThat(selector.priorityLevelCount()).isEqualTo(2);
        selector.removeIf(c -> c.priority == 3);
        assertThat(selector.priorityLevelCount()).isEqualTo(1);
        assertThat(selector.select(0)).isZero();
        selector.remove(first);
        assertThat(selector.priorityLevelCount()).isZero();
        selector.remove(first);
        assertThat(selector.priorityLevelCount()).isZero();
        selector.add(second);
        selector.add(second);
        selector.remove(second);
        assertThat(selector.priorityLevelCount()).isEqualTo(1);
        selector.removeIf(c -> true);
        assertThat(selector.priorityLevelCount()).isZero();
    }

    @Test
    public void bulkRemovalRepairsInconsistentMembership() {
        List<Slot> consumers = new CopyOnWriteArrayList<>();
        ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                c -> c.available);
        Slot consumer = new Slot(0, 0);
        selector.add(consumer);
        // Model the existing dispatcher recovery path for a list/set mismatch.
        consumers.add(consumer);
        selector.remove(consumer);
        selector.removeIf(c -> c.equals(consumer));
        assertThat(consumers).isEmpty();
        assertThat(selector.priorityLevelCount()).isZero();
    }

    @Test
    public void failedBulkRemovalPreservesCounts() {
        List<Slot> consumers = new CopyOnWriteArrayList<>();
        ConsumerPrioritySelector<Slot> selector = new ConsumerPrioritySelector<>(consumers, c -> c.priority,
                c -> c.available);
        selector.add(new Slot(0, 0));
        selector.add(new Slot(1, 3));
        assertThatThrownBy(() -> selector.removeIf(c -> {
            if (c.index == 1) {
                throw new IllegalStateException("predicate failed");
            }
            return true;
        })).isInstanceOf(IllegalStateException.class);
        assertThat(consumers).hasSize(2);
        assertThat(selector.priorityLevelCount()).isEqualTo(2);
        selector.removeIf(c -> true);
        assertThat(selector.priorityLevelCount()).isZero();
    }

    private static final class Slot {
        private final int index;
        private final int priority;
        private boolean available;

        private Slot(int index, int priority) {
            this.index = index;
            this.priority = priority;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Slot slot && index == slot.index;
        }

        @Override
        public int hashCode() {
            return index;
        }
    }
}
