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
package org.apache.pulsar.tests;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ThreadUtils;
import org.testng.annotations.Test;

public class ThreadLeakDetectorListenerTest {

    @Test
    void testSkipsCleanerThreads() {
        Cleaner cleaner = Cleaner.create();
        List<Thread> cleanerThreads = threadsNamed(name -> name.startsWith("Cleaner-"));
        assertFalse(cleanerThreads.isEmpty());
        for (Thread thread : cleanerThreads) {
            assertTrue(ThreadLeakDetectorListener.shouldSkipThread(thread), thread.toString());
        }
        Reference.reachabilityFence(cleaner);
    }

    @Test
    void testSkipsCompletableFutureDelayScheduler() throws InterruptedException {
        CountDownLatch delayedTaskRan = new CountDownLatch(1);
        CompletableFuture.delayedExecutor(1, TimeUnit.MILLISECONDS).execute(delayedTaskRan::countDown);
        assertTrue(delayedTaskRan.await(10, TimeUnit.SECONDS));
        // CompletableFutureDelayScheduler up to Java 24, ForkJoinPool.commonPool-delayScheduler since Java 25
        List<Thread> schedulerThreads = threadsNamed(
                name -> name.equals("CompletableFutureDelayScheduler") || name.endsWith("-delayScheduler"));
        assertFalse(schedulerThreads.isEmpty());
        for (Thread thread : schedulerThreads) {
            assertTrue(ThreadLeakDetectorListener.shouldSkipThread(thread), thread.toString());
        }
    }

    @Test
    void testReportsOtherThreads() {
        assertFalse(ThreadLeakDetectorListener.shouldSkipThread(new Thread(() -> { }, "test-thread")));
        // a thread outside the JDK's InnocuousThreadGroup isn't a Cleaner thread, whatever its name
        assertFalse(ThreadLeakDetectorListener.shouldSkipThread(new Thread(() -> { }, "Cleaner-0")));
    }

    private static List<Thread> threadsNamed(Predicate<String> namePredicate) {
        return ThreadUtils.getAllThreads().stream()
                .filter(thread -> namePredicate.test(thread.getName()))
                .collect(Collectors.toList());
    }
}
