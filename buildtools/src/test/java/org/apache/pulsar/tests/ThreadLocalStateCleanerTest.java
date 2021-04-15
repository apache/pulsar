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
package org.apache.pulsar.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class ThreadLocalStateCleanerTest {
    final ThreadLocal<Integer> magicNumberThreadLocal = ThreadLocal.withInitial(() -> 42);

    @Test
    public void testThreadLocalStateCleanupInCurrentThread() {
        magicNumberThreadLocal.set(44);
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);
        ThreadLocalStateCleaner.INSTANCE.cleanupThreadLocal(magicNumberThreadLocal, Thread.currentThread(), null);
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
    }

    private static class ThreadValueEntry {
        private final Thread thread;
        private final Object value;

        private ThreadValueEntry(Thread thread, Object value) {
            this.thread = thread;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ThreadValueEntry that = (ThreadValueEntry) o;
            return Objects.equals(thread, that.thread) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(thread, value);
        }
    }


    @Test
    public void testThreadLocalStateCleanupInCurrentAndOtherThread() throws InterruptedException, ExecutionException {
        magicNumberThreadLocal.set(44);
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);

        CountDownLatch numberHasBeenSet = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        CompletableFuture<Integer> valueAfterReset = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                magicNumberThreadLocal.set(45);
                assertEquals(magicNumberThreadLocal.get().intValue(), 45);
                numberHasBeenSet.countDown();
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                valueAfterReset.complete(magicNumberThreadLocal.get());
            }
        });
        thread.start();
        numberHasBeenSet.await();
        Set<ThreadValueEntry> replacedValues = new HashSet<>();
        ThreadLocalStateCleaner.INSTANCE.cleanupThreadLocal(magicNumberThreadLocal, (t, currentValue) -> {
            replacedValues.add(new ThreadValueEntry(t, currentValue));
        });
        shutdownLatch.countDown();
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
        assertEquals(valueAfterReset.get().intValue(), 42);
        assertEquals(replacedValues.size(), 2);
        assertTrue(replacedValues.contains(new ThreadValueEntry(thread, 45)));
        assertTrue(replacedValues.contains(new ThreadValueEntry(Thread.currentThread(), 44)));
    }
}