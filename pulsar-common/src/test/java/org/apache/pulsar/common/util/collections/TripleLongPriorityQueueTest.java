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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.Reporter;
import org.testng.annotations.Test;

public class TripleLongPriorityQueueTest {

    @Test
    public void testQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int num = 1000;

        for (int i = num; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), num);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= num; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), num - i);
        }

        pq.close();
    }

    @Test
    public void testLargeQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int num = 3_000_000;

        for (int i = num; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), num);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= num; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), num - i);
        }

        pq.clear();
        pq.close();
    }


    @Test
    public void testCheckForEmpty() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        try {
            pq.peekN1();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.peekN2();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.peekN3();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            pq.pop();
            fail("Should fail");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        pq.close();
    }

    @Test
    public void testCompareWithSamePrefix() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        pq.add(10, 20, 30);
        pq.add(20, 10, 10);
        pq.add(10, 20, 10);
        pq.add(10, 30, 10);
        pq.add(10, 20, 5);

        assertEquals(pq.size(), 5);

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 5);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 20);
        assertEquals(pq.peekN3(), 30);
        pq.pop();

        assertEquals(pq.peekN1(), 10);
        assertEquals(pq.peekN2(), 30);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.peekN1(), 20);
        assertEquals(pq.peekN2(), 10);
        assertEquals(pq.peekN3(), 10);
        pq.pop();

        assertEquals(pq.size(), 0);
        assertTrue(pq.isEmpty());

        pq.close();
    }

    @Test
    public void testRandomizedInterleavedOperations() {
        Comparator<long[]> tupleComparator = Comparator
                .comparingLong((long[] tuple) -> tuple[0])
                .thenComparingLong(tuple -> tuple[1])
                .thenComparingLong(tuple -> tuple[2]);
        PriorityQueue<long[]> expected = new PriorityQueue<>(tupleComparator);
        long seed = randomSeed("testRandomizedInterleavedOperations");
        Random random = new Random(seed);

        try (TripleLongPriorityQueue actual = new TripleLongPriorityQueue(4)) {
            for (int i = 0; i < 20_000; i++) {
                String context = "seed=" + seed + " iteration=" + i;
                boolean add = expected.isEmpty() || random.nextInt(100) < 65;
                if (add) {
                    long[] tuple = {
                            randomLongWithEdgeCases(random, 512),
                            randomLongWithEdgeCases(random, 32),
                            randomLongWithEdgeCases(random, 512)
                    };
                    actual.add(tuple[0], tuple[1], tuple[2]);
                    expected.add(tuple);
                } else {
                    assertQueueHead(expected, actual, context);
                    expected.poll();
                    actual.pop();
                }

                assertEquals(actual.size(), expected.size(), context);
                if (!expected.isEmpty() && (i & 7) == 0) {
                    assertQueueHead(expected, actual, context);
                }
            }

            int drainIteration = 0;
            while (!expected.isEmpty()) {
                assertQueueHead(expected, actual, "seed=" + seed + " drainIteration=" + drainIteration++);
                expected.poll();
                actual.pop();
            }

            assertTrue(actual.isEmpty());
        }
    }

    @Test
    public void testShrink() throws Exception {
        int initialCapacity = 20;
        int tupleSize = 3 * 8;
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue(initialCapacity, 0.5f);
        pq.add(0, 0, 0);
        assertEquals(pq.size(), 1);
        assertEquals(pq.bytesCapacity(), initialCapacity * tupleSize);

        // Scale out to capacity * 2
        triggerScaleOut(initialCapacity, pq);
        int scaleCapacity = initialCapacity * 2;
        assertEquals(pq.bytesCapacity(), scaleCapacity * tupleSize);
        // Trigger shrinking
        for (int i = 0; i < initialCapacity / 2 + 2; i++) {
             pq.pop();
        }
        int capacity = scaleCapacity - (int) ((scaleCapacity) * 0.5f * 0.9f);
        assertTrue(pq.bytesCapacity() < scaleCapacity * tupleSize);
        // Scale out to capacity * 2
        triggerScaleOut(initialCapacity, pq);
        scaleCapacity = capacity * 2;
        // Trigger shrinking
        pq.clear();
        capacity = scaleCapacity - (int) (scaleCapacity * 0.5f * 0.9f);
        pq.close();
    }

    private void triggerScaleOut(int initialCapacity, TripleLongPriorityQueue pq) {
        for (long i = 0; i < initialCapacity + 1; i++) {
            pq.add(i, i, i);
        }
    }

    @Test
    public void testDifferentialRandomPriorityQueue() {
        Comparator<long[]> cmp = Comparator.comparingLong((long[] t) -> t[0])
                .thenComparingLong(t -> t[1])
                .thenComparingLong(t -> t[2]);

        for (int trial = 0; trial < 10; trial++) {
            Random rng = new Random(42 + trial);
            PriorityQueue<long[]> oracle = new PriorityQueue<>(cmp);
            try (TripleLongPriorityQueue pq = new TripleLongPriorityQueue()) {
                int ops = 10_000 + rng.nextInt(10_000);
                for (int i = 0; i < ops; i++) {
                    boolean doAdd = oracle.isEmpty() || rng.nextBoolean();
                    if (doAdd) {
                        // ~10% chance of same-prefix (small n1 range) to exercise tie-breaking
                        long n1 = rng.nextInt(100) < 10 ? rng.nextLong(20) : rng.nextLong(1_000_000);
                        long n2 = rng.nextLong(100);
                        long n3 = rng.nextLong(1_000_000);
                        oracle.add(new long[]{n1, n2, n3});
                        pq.add(n1, n2, n3);
                    } else {
                        long[] expected = oracle.poll();
                        assertNotNull(expected);
                        assertEquals(pq.peekN1(), expected[0], "n1 mismatch at op " + i);
                        assertEquals(pq.peekN2(), expected[1], "n2 mismatch at op " + i);
                        assertEquals(pq.peekN3(), expected[2], "n3 mismatch at op " + i);
                        pq.pop();
                    }
                    assertEquals(pq.size(), oracle.size(), "size mismatch at op " + i);
                }
                // drain remaining
                while (!oracle.isEmpty()) {
                    long[] expected = oracle.poll();
                    assertEquals(pq.peekN1(), expected[0]);
                    assertEquals(pq.peekN2(), expected[1]);
                    assertEquals(pq.peekN3(), expected[2]);
                    pq.pop();
                }
                assertTrue(pq.isEmpty());
            }
        }
    }

    private static long randomSeed(String testName) {
        String configuredSeed = System.getProperty("pulsar.collections.randomSeed");
        long seed = configuredSeed != null ? Long.parseLong(configuredSeed) : ThreadLocalRandom.current().nextLong();
        String message = TripleLongPriorityQueueTest.class.getSimpleName() + "." + testName + " seed=" + seed;
        Reporter.log(message, true);
        return seed;
    }

    private static long randomLongWithEdgeCases(Random random, int bound) {
        switch (random.nextInt(64)) {
            case 0:
                return Long.MIN_VALUE;
            case 1:
                return Long.MAX_VALUE;
            default:
                return random.nextInt(bound) - bound / 2L;
        }
    }

    private static void assertQueueHead(PriorityQueue<long[]> expected, TripleLongPriorityQueue actual,
                                        String context) {
        long[] tuple = expected.peek();
        assertEquals(actual.peekN1(), tuple[0], context);
        assertEquals(actual.peekN2(), tuple[1], context);
        assertEquals(actual.peekN3(), tuple[2], context);
    }
}
