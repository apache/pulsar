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
package org.apache.pulsar.client.util;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class ExecutorProviderTest {

    @Test
    public void testShutdownNowRunsDiscardedCleanupTasksOnceTheExecutorStopped() throws Exception {
        final ExecutorProvider executorProvider = new ExecutorProvider(1, "executor-provider-test");
        final ExecutorService executor = executorProvider.getExecutor();
        // Occupy the executor until shutdownNow() interrupts it, so the tasks below are still queued then.
        final CountDownLatch executorBusy = new CountDownLatch(1);
        executor.execute(() -> {
            executorBusy.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(executorBusy.await(10, TimeUnit.SECONDS)).isTrue();
        final AtomicBoolean taskRan = new AtomicBoolean();
        executor.execute(() -> taskRan.set(true));
        final AtomicReference<Boolean> terminatedWhenCleanupRan = new AtomicReference<>();
        executor.execute((ExecutorProvider.CleanupTask) () -> terminatedWhenCleanupRan.set(executor.isTerminated()));

        executorProvider.shutdownNow();

        assertThat(taskRan).as("discarded task ran").isFalse();
        assertThat(terminatedWhenCleanupRan.get()).as("executor terminated when the cleanup task ran").isTrue();
    }
}
