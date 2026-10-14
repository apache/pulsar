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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BatchingExecutorWrapperTest {
    private static final int QUEUE_CHUNK_SIZE = 16;
    private static final long NO_WEIGHT_LIMIT = Long.MAX_VALUE;
    private static final Consumer<Throwable> FAIL_ON_TASK_FAILURE = t -> {
        throw new AssertionError("Unexpected task failure", t);
    };
    private static final BiConsumer<Runnable, RuntimeException> FAIL_ON_REJECTED_TASK = (task, e) -> {
        throw new AssertionError("Unexpected rejected task", e);
    };

    /**
     * An executor that keeps the submitted tasks until the test runs them, and rejects them while {@link #rejecting}.
     */
    private static class ManualExecutor implements Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();
        boolean rejecting;
        // Runs before a rejection is thrown, standing in for a thread that queues a task meanwhile.
        Runnable beforeRejecting = () -> { };

        @Override
        public void execute(Runnable command) {
            if (rejecting) {
                beforeRejecting.run();
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

    private static BatchingExecutorWrapper newWrapper(Executor delegate, int maxItems, long maxWeight) {
        return new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, maxItems, maxWeight, FAIL_ON_TASK_FAILURE,
                FAIL_ON_REJECTED_TASK);
    }

    /**
     * Returns a task of the given weight that runs {@code action}.
     */
    private static BatchingExecutorWrapper.WeightedRunnable weighted(long weight, Runnable action) {
        return new BatchingExecutorWrapper.WeightedRunnable() {
            @Override
            public long getWeight() {
                return weight;
            }

            @Override
            public void run() {
                action.run();
            }
        };
    }

    @Test
    public void testMaxItemsMustBeGreaterThanOne() {
        for (int maxItems : new int[] {-1, 0, 1}) {
            assertThatThrownBy(() -> newWrapper(new ManualExecutor(), maxItems, NO_WEIGHT_LIMIT))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testMaxWeightMustBePositive() {
        for (long maxWeight : new long[] {-1, 0}) {
            assertThatThrownBy(() -> newWrapper(new ManualExecutor(), 1024, maxWeight))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testBatchStopsTakingTasksOnceTheirWeightsReachMaxWeight() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, 10);
        List<Integer> ran = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            int task = i;
            wrapper.execute(weighted(4, () -> ran.add(task)));
        }

        delegate.runNext();

        // 4 + 4 is below the limit, so the batch takes a third task; 12 reaches it, so the batch stops there.
        assertThat(ran).containsExactly(0, 1, 2);
        assertThat(delegate.tasks).hasSize(1);
        delegate.runNext();
        assertThat(ran).containsExactly(0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testTaskHeavierThanMaxWeightRunsInABatchOfItsOwn() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, 10);
        List<Integer> ran = new ArrayList<>();
        wrapper.execute(weighted(100, () -> ran.add(0)));
        wrapper.execute(weighted(1, () -> ran.add(1)));

        delegate.runNext();

        assertThat(ran).containsExactly(0);
        delegate.runNext();
        assertThat(ran).containsExactly(0, 1);
    }

    @Test
    public void testTasksWithoutWeightAreLimitedOnlyByMaxItems() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 3, 1);
        List<Integer> ran = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int task = i;
            wrapper.execute(() -> ran.add(task));
        }

        delegate.runNext();

        assertThat(ran).containsExactly(0, 1, 2);
    }

    @Test
    public void testTasksQueuedBeforeTheBatchRunsShareOneBatch() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, NO_WEIGHT_LIMIT);
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
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 2, NO_WEIGHT_LIMIT);
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
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, NO_WEIGHT_LIMIT);
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
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024,
                NO_WEIGHT_LIMIT, failures::add, FAIL_ON_REJECTED_TASK);
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
    public void testQueueIsCreatedByTheFirstTask() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, NO_WEIGHT_LIMIT);
        assertThat(wrapper.hasHandoverQueue()).isFalse();

        wrapper.execute(() -> { });

        assertThat(wrapper.hasHandoverQueue()).isTrue();
    }

    @Test
    public void testRejectedHandoverFailsTheCallerAndALaterTaskRetriesScheduling() {
        ManualExecutor delegate = new ManualExecutor();
        BatchingExecutorWrapper wrapper = newWrapper(delegate, 1024, NO_WEIGHT_LIMIT);
        List<Integer> ran = new ArrayList<>();
        delegate.rejecting = true;

        assertThatThrownBy(() -> wrapper.execute(() -> ran.add(0))).isInstanceOf(RejectedExecutionException.class);

        delegate.rejecting = false;
        wrapper.execute(() -> ran.add(1));
        assertThat(delegate.tasks).hasSize(1);
        delegate.runAll();
        // The rejected task was taken out of the queue, so it does not run after its caller was failed.
        assertThat(ran).containsExactly(1);
    }

    @Test
    public void testRejectedHandoverPassesTasksQueuedByOtherThreadsToTheHandler() {
        ManualExecutor delegate = new ManualExecutor();
        List<Runnable> rejected = new ArrayList<>();
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 1024,
                NO_WEIGHT_LIMIT, FAIL_ON_TASK_FAILURE, (task, e) -> rejected.add(task));
        List<Integer> ran = new ArrayList<>();
        Runnable queuedMeanwhile = () -> ran.add(1);
        // Queued while the first caller's handover is being submitted, so its own execute() returns normally.
        delegate.beforeRejecting = () -> {
            delegate.beforeRejecting = () -> { };
            wrapper.execute(queuedMeanwhile);
        };
        delegate.rejecting = true;

        assertThatThrownBy(() -> wrapper.execute(() -> ran.add(0))).isInstanceOf(RejectedExecutionException.class);

        assertThat(rejected).containsExactly(queuedMeanwhile);
        assertThat(ran).isEmpty();
        assertThat(delegate.tasks).isEmpty();
    }

    @Test
    public void testRejectedFollowUpBatchPassesTheRemainingTasksToTheHandler() {
        ManualExecutor delegate = new ManualExecutor();
        List<Runnable> rejected = new ArrayList<>();
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, 2,
                NO_WEIGHT_LIMIT, FAIL_ON_TASK_FAILURE, (task, e) -> rejected.add(task));
        List<Integer> ran = new ArrayList<>();
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int task = i;
            tasks.add(() -> ran.add(task));
            wrapper.execute(tasks.get(i));
        }
        delegate.rejecting = true;

        // The batch runs two tasks, and the delegate rejects the batch for the other two.
        delegate.runNext();

        assertThat(ran).containsExactly(0, 1);
        assertThat(rejected).containsExactly(tasks.get(2), tasks.get(3));
        delegate.rejecting = false;
        wrapper.execute(() -> ran.add(4));
        delegate.runAll();
        assertThat(ran).containsExactly(0, 1, 4);
    }

    @Test(timeOut = 30000)
    public void testConcurrentSubmittersToARejectingDelegateLeaveNoTaskBehind() throws Exception {
        int threads = 8;
        int tasksPerThread = 10000;
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        AtomicInteger rejected = new AtomicInteger();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(command -> {
            throw new RejectedExecutionException("rejected");
        }, QUEUE_CHUNK_SIZE, 1024, NO_WEIGHT_LIMIT, failures::add, (task, e) -> rejected.incrementAndGet());
        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch submitted = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                submitters.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < tasksPerThread; i++) {
                            try {
                                wrapper.execute(() -> failures.add(new AssertionError("A rejected task ran")));
                            } catch (RejectedExecutionException e) {
                                rejected.incrementAndGet();
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    } finally {
                        submitted.countDown();
                    }
                });
            }
            assertThat(submitted.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            submitters.shutdownNow();
        }
        assertThat(failures).isEmpty();
        // Each task was either failed to its caller or passed to the handler, and none was left in the queue.
        assertThat(rejected.get()).isEqualTo(threads * tasksPerThread);
    }

    @Test(timeOut = 30000)
    public void testConcurrentSubmittersToASometimesRejectingDelegateHandleEachTaskOnce() throws Exception {
        int threads = 8;
        int tasksPerThread = 10000;
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        AtomicInteger handovers = new AtomicInteger();
        // How many times each task ran or was rejected, which must be exactly once.
        AtomicIntegerArray outcomes = new AtomicIntegerArray(threads * tasksPerThread);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(command -> {
            if (handovers.incrementAndGet() % 3 == 0) {
                throw new RejectedExecutionException("rejected");
            }
            delegate.execute(command);
        }, QUEUE_CHUNK_SIZE, 16, NO_WEIGHT_LIMIT, failures::add,
                (task, e) -> ((IndexedTask) task).record());
        CyclicBarrier start = new CyclicBarrier(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int thread = t;
                submitters.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < tasksPerThread; i++) {
                            IndexedTask task = new IndexedTask(thread * tasksPerThread + i, outcomes);
                            try {
                                wrapper.execute(task);
                            } catch (RejectedExecutionException e) {
                                task.record();
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                });
            }
            Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(failures).isEmpty();
                for (int i = 0; i < outcomes.length(); i++) {
                    assertThat(outcomes.get(i)).as("outcomes of task %d", i).isEqualTo(1);
                }
            });
        } finally {
            submitters.shutdownNow();
            delegate.shutdownNow();
        }
    }

    /**
     * A task that records in {@code outcomes} that it ran, or that it was rejected.
     */
    private record IndexedTask(int index, AtomicIntegerArray outcomes) implements Runnable {
        @Override
        public void run() {
            record();
        }

        void record() {
            outcomes.incrementAndGet(index);
        }
    }

    @DataProvider
    public Object[][] batchLimits() {
        return new Object[][] {{2, NO_WEIGHT_LIMIT}, {1024, NO_WEIGHT_LIMIT}, {1024, 3L}};
    }

    @Test(timeOut = 30000, dataProvider = "batchLimits")
    public void testConcurrentSubmittersKeepPerThreadOrder(int maxItems, long maxWeight) throws Exception {
        int threads = 8;
        int tasksPerThread = 10000;
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        BatchingExecutorWrapper wrapper = new BatchingExecutorWrapper(delegate, QUEUE_CHUNK_SIZE, maxItems,
                maxWeight, failures::add, FAIL_ON_REJECTED_TASK);
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
                            wrapper.execute(weighted(1, () -> {
                                ran.add(task);
                                completed.countDown();
                            }));
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
