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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class GrowableArrayBlockingQueueTest {

    @Test
    public void simple() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertNull(queue.poll());

        assertEquals(queue.remainingCapacity(), Integer.MAX_VALUE);
        assertEquals(queue.toString(), "[]");

        try {
            queue.element();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            queue.iterator();
            fail("Should have thrown exception");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        // Test index rollover
        for (int i = 0; i < 100; i++) {
            queue.add(i);

            assertEquals(queue.take().intValue(), i);
        }

        queue.offer(1);
        assertEquals(queue.toString(), "[1]");
        queue.offer(2);
        assertEquals(queue.toString(), "[1, 2]");
        queue.offer(3);
        assertEquals(queue.toString(), "[1, 2, 3]");
        queue.offer(4);
        assertEquals(queue.toString(), "[1, 2, 3, 4]");

        AtomicInteger value = new AtomicInteger(1);
        queue.forEach(v -> {
            assertEquals(v.intValue(), value.get());
            value.incrementAndGet();
        });

        assertEquals(queue.size(), 4);

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list, 3);

        assertEquals(queue.size(), 1);
        assertEquals(list, Lists.newArrayList(1, 2, 3));
        assertEquals(queue.toString(), "[4]");
        assertEquals(queue.peek().intValue(), 4);

        assertEquals(queue.element().intValue(), 4);
        assertEquals(queue.remove().intValue(), 4);
        try {
            queue.remove();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    @Test(timeOut = 10000)
    public void blockingTake() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>();

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                int expected = 0;

                for (int i = 0; i < 100; i++) {
                    int n = queue.take();

                    assertEquals(n, expected++);
                }

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        int n = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                queue.put(n);
                ++n;
            }

            // Wait until all the entries are consumed
            while (!queue.isEmpty()) {
                Thread.sleep(1);
            }
        }

        latch.await();
    }

    @Test
    public void growArray() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertNull(queue.poll());

        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));
        assertTrue(queue.offer(5));

        assertEquals(queue.size(), 5);

        queue.clear();
        assertEquals(queue.size(), 0);

        assertTrue(queue.offer(1, 1, TimeUnit.SECONDS));
        assertTrue(queue.offer(2, 1, TimeUnit.SECONDS));
        assertTrue(queue.offer(3, 1, TimeUnit.SECONDS));
        assertEquals(queue.size(), 3);

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list);
        assertEquals(queue.size(), 0);

        assertEquals(list, Lists.newArrayList(1, 2, 3));
    }

    @Test(timeOut = 10000)
    public void pollTimeout() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        queue.put(1);
        assertEquals(queue.poll(1, TimeUnit.MILLISECONDS).intValue(), 1);

        // 0 timeout should not block
        assertNull(queue.poll(0, TimeUnit.HOURS));

        queue.put(2);
        queue.put(3);
        assertEquals(queue.poll(1, TimeUnit.HOURS).intValue(), 2);
        assertEquals(queue.poll(1, TimeUnit.HOURS).intValue(), 3);
    }

    @Test(timeOut = 10000)
    public void pollTimeout2() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>();

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                queue.poll(1, TimeUnit.HOURS);

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Make sure background thread is waiting on poll
        Thread.sleep(100);
        queue.put(1);

        latch.await();
    }

    @Test
    public void removeTest() {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertNull(queue.poll());

        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertTrue(queue.offer(3));

        assertEquals(queue.size(), 3);

        assertFalse(queue.remove(4));
        assertEquals(queue.size(), 3);
        assertEquals(queue.toString(), "[1, 2, 3]");

        assertTrue(queue.remove(2));
        assertEquals(queue.size(), 2);
        assertEquals(queue.toString(), "[1, 3]");

        assertTrue(queue.remove(3));
        assertEquals(queue.size(), 1);
        assertEquals(queue.toString(), "[1]");

        assertTrue(queue.remove(1));
        assertEquals(queue.size(), 0);
        assertEquals(queue.toString(), "[]");

        // Test queue rollover
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        assertEquals(queue.poll().intValue(), 1);
        assertEquals(queue.poll().intValue(), 2);

        assertTrue(queue.offer(4));
        assertTrue(queue.offer(5));

        assertTrue(queue.remove(5));
        assertEquals(queue.size(), 2);
        assertEquals(queue.toString(), "[3, 4]");

        queue.offer(6);
        queue.offer(7);
        assertTrue(queue.remove(6));
        assertEquals(queue.size(), 3);
        assertEquals(queue.toString(), "[3, 4, 7]");

        queue.offer(8);
        assertTrue(queue.remove(8));
        assertEquals(queue.size(), 3);
        assertEquals(queue.toString(), "[3, 4, 7]");

        queue.offer(8);
        assertEquals(queue.toString(), "[3, 4, 7, 8]");

        assertTrue(queue.remove(4));
        assertEquals(queue.size(), 3);
        assertEquals(queue.toString(), "[3, 7, 8]");

        assertTrue(queue.remove(8));
        assertEquals(queue.size(), 2);
        assertEquals(queue.toString(), "[3, 7]");

        assertTrue(queue.remove(7));
        assertEquals(queue.size(), 1);
        assertEquals(queue.toString(), "[3]");
    }
}
