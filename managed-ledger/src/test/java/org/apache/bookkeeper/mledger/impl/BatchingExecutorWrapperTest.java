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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BatchingExecutorWrapperTest {
    private static final int QUEUE_CHUNK_SIZE = 16;
    private static final Consumer<Throwable> FAIL_ON_TASK_FAILURE = t -> {
        throw new AssertionError("Unexpected task failure", t);
    };

    /**
     * An executor that keeps the submitted tasks until the test runs them, and rejects them while {@link #rejecting}.
     */
    private static class ManualExecutor implements java.util.concurrent.Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();
        boolean rejecting;

        @Override
        public void execute(Runnable command) {
            if (rejecting) {
                throw new RejectedExecutionException("rejected");
            }
            tasks.add(command);
        }

        void runNext() {
            tasks.remove().run();
        }

        void runAll() {
            while (!tasks.isEmpty()) {
                runNext();
            }
        }
    }

    @Test
    public void testMaxBatchSizeMustBeGreaterThanOne() {
        for (int handoverMaxBatchSize : new int[] {-1, 0, 1}) {
            assertThatThrownBy(() -> new BatchingExecutorWrapper(new ManualExecutor(), QUEUE_CHUNK_SIZE,
                    handoverMaxBatchSize, FAIL_ON_TASK_FAILURE)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testTasksQueuedBeforeTheBatchRunsShareOneBatch() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024, FAIL_ON_TASK_FAILURE);
        List<Integer> ran = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int task = i;
            wrapper.execute(() -> ran.add(task));
        }

        assertThat(delegate.tasks).hasSize(1);
        delegate.runNext();
        assertThat(ran).containsExactly(0, 1, 2);
        assertThat(delegate.tasks).isEmpty();
    }

    @Test
    public void testBatchRunsAtMostMaxBatchSizeTasksAndSchedulesTheRest() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 2, FAIL_ON_TASK_FAILURE);
        List<Integer> ran = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            int task = i;
            wrapper.execute(() -> ran.add(task));
        }

        delegate.runNext();

        // The rest is left to a new batch, so that tasks submitted to the delegate in between can run.
        assertThat(ran).containsExactly(0, 1);
        assertThat(delegate.tasks).hasSize(1);
        delegate.runNext();
        assertThat(ran).containsExactly(0, 1, 2, 3);
        delegate.runAll();
        assertThat(ran).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void testTaskQueuedWhileABatchRunsIsNotLeftBehind() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024, FAIL_ON_TASK_FAILURE);
        List<String> ran = new ArrayList<>();
        wrapper.execute(() -> {
            ran.add("first");
            wrapper.execute(() -> ran.add("second"));
        });

        delegate.runAll();

        assertThat(ran).containsExactly("first", "second");
    }

    @Test
    public void testTaskFailureIsProcessedAndTheBatchContinues() {
        ManualExecutor delegate = new ManualExecutor();
        List<Throwable> failures = new ArrayList<>();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024, failures::add);
        RuntimeException failure = new RuntimeException("task failed");
        List<Integer> ran = new ArrayList<>();
        wrapper.execute(() -> ran.add(0));
        wrapper.execute(() -> {
            throw failure;
        });
        wrapper.execute(() -> ran.add(2));

        delegate.runAll();

        assertThat(failures).containsExactly(failure);
        assertThat(ran).containsExactly(0, 2);
    }

    @Test
    public void testRejectedHandoverFailsTheCallerAndALaterTaskRetriesScheduling() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024, FAIL_ON_TASK_FAILURE);
        List<Integer> ran = new ArrayList<>();
        delegate.rejecting = true;

        assertThatThrownBy(() -> wrapper.execute(() -> ran.add(0))).isInstanceOf(RejectedExecutionException.class);

        delegate.rejecting = false;
        wrapper.execute(() -> ran.add(1));
        assertThat(delegate.tasks).hasSize(1);
        delegate.runAll();
        assertThat(ran).contains(1);
    }

    @DataProvider
    public Object[][] handoverMaxBatchSizes() {
        return new Object[][] {{2}, {1024}};
    }

    @Test(timeOut = 30000, dataProvider = "handoverMaxBatchSizes")
    public void testConcurrentSubmittersKeepPerThreadOrder(int handoverMaxBatchSize) throws Exception {
        int threads = 8;
        int tasksPerThread = 10000;
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        BatchingExecutorWrapper wrapper =
                new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, handoverMaxBatchSize, failures::add);
        // Written only by the delegate's single thread; the latch publishes the results to the test thread.
        List<List<Integer>> ranByThread = new ArrayList<>();
        CountDownLatch completed = new CountDownLatch(threads * tasksPerThread);
        CyclicBarrier start = new CyclicBarrier(threads);
        try {
            for (int t = 0; t < threads; t++) {
                List<Integer> ran = new ArrayList<>();
                ranByThread.add(ran);
                submitters.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < tasksPerThread; i++) {
                            int task = i;
                            wrapper.execute(() -> {
                                ran.add(task);
                                completed.countDown();
                            });
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                });
            }
            assertThat(completed.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            submitters.shutdownNow();
            delegate.shutdownNow();
        }
        assertThat(failures).isEmpty();
        // Tasks from one thread run in the order that thread submitted them.
        for (List<Integer> ran : ranByThread) {
            assertThat(ran).hasSize(tasksPerThread).isSorted();
        }
    }
}
