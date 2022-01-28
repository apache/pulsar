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
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class FastThreadLocalStateCleanerTest {
    private static final class MagicNumberWrapper {
        private final int value;

        private MagicNumberWrapper(int value) {
            this.value = value;
        }

        public int intValue() {
            return value;
        }
    }

    final FastThreadLocal<MagicNumberWrapper> magicNumberThreadLocal = new FastThreadLocal<MagicNumberWrapper>() {
        @Override
        protected MagicNumberWrapper initialValue() throws Exception {
            return new MagicNumberWrapper(42);
        }
    };
    final FastThreadLocalStateCleaner cleaner = new FastThreadLocalStateCleaner(object ->
            object.getClass() == MagicNumberWrapper.class);

    @Test
    public void testThreadLocalStateCleanupInCurrentThread() {
        magicNumberThreadLocal.set(new MagicNumberWrapper(44));
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);
        cleaner.cleanupAllFastThreadLocals(Thread.currentThread(), ((thread, o) -> {
            System.out.println("Cleaning up " + thread + " value " + o);
        }));
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
    }

    @Test
    public void testThreadLocalStateCleanupInCurrentAndOtherThread() throws InterruptedException, ExecutionException {
        magicNumberThreadLocal.set(new MagicNumberWrapper(44));
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);

        CountDownLatch numberHasBeenSet = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        CompletableFuture<MagicNumberWrapper> valueAfterReset = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                magicNumberThreadLocal.set(new MagicNumberWrapper(45));
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
        Set<Thread> cleanedThreads = new HashSet<>();
        cleaner.cleanupAllFastThreadLocals((t, currentValue) -> {
            cleanedThreads.add(t);
        });
        shutdownLatch.countDown();
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
        assertEquals(valueAfterReset.get().intValue(), 42);
        assertEquals(cleanedThreads.size(), 2);
        assertTrue(cleanedThreads.contains(thread));
        assertTrue(cleanedThreads.contains(Thread.currentThread()));
    }

    @Test
    public void testThreadLocalStateCleanupInFastThreadLocalThread() throws InterruptedException, ExecutionException {
        CountDownLatch numberHasBeenSet = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        CompletableFuture<MagicNumberWrapper> valueAfterReset = new CompletableFuture<>();
        Thread thread = new FastThreadLocalThread(() -> {
            try {
                magicNumberThreadLocal.set(new MagicNumberWrapper(45));
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
        Set<Thread> cleanedThreads = new HashSet<>();
        cleaner.cleanupAllFastThreadLocals((t, currentValue) -> {
            cleanedThreads.add(t);
        });
        shutdownLatch.countDown();
        assertEquals(valueAfterReset.get().intValue(), 42);
        assertTrue(cleanedThreads.contains(thread));
    }

}