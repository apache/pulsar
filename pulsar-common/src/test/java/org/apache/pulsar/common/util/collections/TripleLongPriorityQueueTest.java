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
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class TripleLongPriorityQueueTest {

    @Test
    public void testQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int N = 1000;

        for (int i = N; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), N);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= N; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), N - i);
        }

        pq.close();
    }

    @Test
    public void testLargeQueue() {
        TripleLongPriorityQueue pq = new TripleLongPriorityQueue();
        assertEquals(pq.size(), 0);

        final int N = 3_000_000;

        for (int i = N; i > 0; i--) {
            pq.add(i, i * 2L, i * 3L);
        }

        assertEquals(pq.size(), N);
        assertFalse(pq.isEmpty());

        for (int i = 1; i <= N; i++) {
            assertEquals(pq.peekN1(), i);
            assertEquals(pq.peekN2(), i * 2);
            assertEquals(pq.peekN3(), i * 3);

            pq.pop();

            assertEquals(pq.size(), N - i);
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
        int capacity = scaleCapacity - (int)((scaleCapacity ) * 0.5f * 0.9f);
        assertTrue(pq.bytesCapacity() < scaleCapacity * tupleSize);
        // Scale out to capacity * 2
        triggerScaleOut(initialCapacity, pq);
        scaleCapacity = capacity * 2;
        // Trigger shrinking
        pq.clear();
        capacity = scaleCapacity - (int)(scaleCapacity * 0.5f * 0.9f);
    }

    private void triggerScaleOut(int initialCapacity, TripleLongPriorityQueue pq) {
        for (long i = 0; i < initialCapacity + 1; i++) {
            pq.add(i, i, i);
        }
    }
}
