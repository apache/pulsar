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
package org.apache.pulsar.broker.transaction.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeSet;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionTimeoutQueueTest {

    @Test
    public void testDeadlineAndSequenceOrdering() {
        TransactionTimeoutQueue queue = new TransactionTimeoutQueue();
        queue.add(Long.MAX_VALUE, Long.MAX_VALUE);
        queue.add(0, 10);
        queue.add(2, 10);
        queue.add(Long.MIN_VALUE, Long.MIN_VALUE);
        queue.add(1, 5);

        assertHeadAndPop(queue, Long.MIN_VALUE, Long.MIN_VALUE);
        assertHeadAndPop(queue, 1, 5);
        assertHeadAndPop(queue, 0, 10);
        assertHeadAndPop(queue, 2, 10);
        assertHeadAndPop(queue, Long.MAX_VALUE, Long.MAX_VALUE);
        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.size()).isZero();
        assertThatThrownBy(queue::peekSequenceId).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(queue::peekTimeout).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(queue::pop).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testArbitraryRemovalRepairsHeap() {
        TransactionTimeoutQueue queue = new TransactionTimeoutQueue();
        long[] deadlines = {1, 10, 2, 11, 12, 3, 4, 13, 14, 15, 16, 5, 6, 7, 8};
        for (long deadline : deadlines) {
            queue.add(deadline, deadline);
        }

        // Replacing 13 with the final entry (8) requires moving it above its parent (11).
        assertThat(queue.remove(13)).isTrue();
        // Replacing 2 requires moving its replacement below its children.
        assertThat(queue.remove(2)).isTrue();
        assertThat(queue.remove(13)).isFalse();
        assertThat(queue.remove(100)).isFalse();
        for (long deadline : new long[]{1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 14, 15, 16}) {
            assertHeadAndPop(queue, deadline, deadline);
        }
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testUpdatingAnExistingSequenceDoesNotAddAnEntry() {
        TransactionTimeoutQueue queue = new TransactionTimeoutQueue();
        queue.add(0, 20);
        queue.add(Long.MAX_VALUE, 10);
        queue.add(0, 5);
        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.peekSequenceId()).isZero();
        assertThat(queue.peekTimeout()).isEqualTo(5);

        queue.add(0, 30);
        queue.add(0, 30);
        assertThat(queue.size()).isEqualTo(2);
        assertHeadAndPop(queue, Long.MAX_VALUE, 10);
        assertThat(queue.remove(0)).isTrue();
        assertThat(queue.remove(0)).isFalse();
        queue.add(0, 1);
        assertHeadAndPop(queue, 0, 1);
    }

    @Test
    public void testCapacityShrinksAndClearResetsTheQueue() {
        TransactionTimeoutQueue queue = new TransactionTimeoutQueue();
        int initialCapacity = queue.capacity();
        for (int i = 0; i < 4096; i++) {
            queue.add(i, i);
        }
        assertThat(queue.capacity()).isGreaterThan(initialCapacity);
        for (int i = 0; i < 4096; i++) {
            assertThat(queue.remove(i)).isTrue();
        }
        assertThat(queue.capacity()).isEqualTo(initialCapacity);

        for (int i = 0; i < 4096; i++) {
            queue.add(i, i);
        }
        queue.clear();
        assertThat(queue.capacity()).isEqualTo(initialCapacity);
        assertThat(queue.size()).isZero();
        assertThat(queue.remove(100)).isFalse();
        queue.add(100, 200);
        assertHeadAndPop(queue, 100, 200);
    }

    @Test
    public void testRandomOperationsAgainstOrderedReference() {
        Random random = new Random(104729);
        TransactionTimeoutQueue queue = new TransactionTimeoutQueue();
        TreeSet<Entry> ordered = new TreeSet<>(Comparator.comparingLong(Entry::timeout)
                .thenComparingLong(Entry::sequenceId));
        Map<Long, Entry> bySequence = new HashMap<>();
        for (int i = 0; i < 50000; i++) {
            long sequenceId = random.nextInt(1000);
            switch (random.nextInt(4)) {
                case 0, 1 -> {
                    Entry entry = new Entry(sequenceId, random.nextInt(1000));
                    Entry previous = bySequence.put(sequenceId, entry);
                    if (previous != null) {
                        ordered.remove(previous);
                    }
                    ordered.add(entry);
                    queue.add(entry.sequenceId(), entry.timeout());
                }
                case 2 -> {
                    Entry previous = bySequence.remove(sequenceId);
                    assertThat(queue.remove(sequenceId)).isEqualTo(previous != null);
                    if (previous != null) {
                        ordered.remove(previous);
                    }
                }
                case 3 -> {
                    if (!ordered.isEmpty()) {
                        Entry first = ordered.pollFirst();
                        bySequence.remove(first.sequenceId());
                        assertHeadAndPop(queue, first.sequenceId(), first.timeout());
                    }
                }
                default -> throw new AssertionError("Unexpected operation");
            }
            assertThat(queue.size()).as("queue size after operation %s", i).isEqualTo(ordered.size());
            assertThat(queue.isEmpty()).isEqualTo(ordered.isEmpty());
            if (!ordered.isEmpty()) {
                assertThat(queue.peekSequenceId()).isEqualTo(ordered.first().sequenceId());
                assertThat(queue.peekTimeout()).isEqualTo(ordered.first().timeout());
            }
        }
        for (Entry entry : ordered) {
            assertHeadAndPop(queue, entry.sequenceId(), entry.timeout());
        }
        assertThat(queue.isEmpty()).isTrue();
    }

    private static void assertHeadAndPop(TransactionTimeoutQueue queue, long sequenceId, long timeout) {
        assertThat(queue.peekSequenceId()).isEqualTo(sequenceId);
        assertThat(queue.peekTimeout()).isEqualTo(timeout);
        queue.pop();
    }

    private record Entry(long sequenceId, long timeout) {
    }
}
