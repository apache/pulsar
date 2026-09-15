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
        consumers.remove(first);
        assertThat(selector.select(0)).isZero();
        second.available = false;
        assertThat(selector.select(0)).isEqualTo(-1);
        consumers.add(0, first);
        assertThat(selector.select(1)).isZero();
    }

    private static final class Slot {
        private final int index;
        private final int priority;
        private boolean available;

        private Slot(int index, int priority) {
            this.index = index;
            this.priority = priority;
        }
    }
}
