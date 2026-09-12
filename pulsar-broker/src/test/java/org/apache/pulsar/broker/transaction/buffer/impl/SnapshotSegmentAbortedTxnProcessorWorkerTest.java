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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.SnapshotSegmentAbortedTxnProcessorImpl.PersistentWorker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SnapshotSegmentAbortedTxnProcessorWorkerTest {

    private ExecutorService executor;
    private SnapshotSegmentAbortedTxnProcessorImpl processor;
    private PersistentWorker worker;

    @BeforeMethod
    public void setup() {
        PersistentTopic topic = mock(PersistentTopic.class, RETURNS_DEEP_STUBS);
        when(topic.getName()).thenReturn("persistent://public/default/snapshot-worker");
        executor = MoreExecutors.newDirectExecutorService();
        when(topic.getBrokerService().getPulsar().getTransactionExecutorProvider().getExecutor(any(Object.class)))
                .thenReturn(executor);
        processor = new SnapshotSegmentAbortedTxnProcessorImpl(topic);
        worker = processor.new PersistentWorker(topic);
    }

    @Test(timeOut = 10_000)
    public void testQueuedTasksContinueWhenExecutorRunsImmediately() throws Exception {
        CompletableFuture<Void> firstWrite = new CompletableFuture<>();
        CompletableFuture<Void> secondWrite = new CompletableFuture<>();
        AtomicBoolean secondStarted = new AtomicBoolean();
        AtomicBoolean thirdStarted = new AtomicBoolean();
        CompletableFuture<Void> firstResult = worker.appendTask(PersistentWorker.OperationType.WriteSegment,
                () -> firstWrite);
        CompletableFuture<Void> secondResult = worker.appendTask(PersistentWorker.OperationType.WriteSegment, () -> {
            secondStarted.set(true);
            return secondWrite;
        });
        assertFalse(secondStarted.get());

        firstWrite.complete(null);
        firstResult.get(5, TimeUnit.SECONDS);
        assertTrue(secondStarted.get(), "The queued write must start when the previous write completes");

        CompletableFuture<Void> thirdResult = worker.appendTask(PersistentWorker.OperationType.WriteSegment, () -> {
            thirdStarted.set(true);
            return CompletableFuture.completedFuture(null);
        });
        assertFalse(thirdStarted.get(), "The next write must wait for the in-flight write");
        secondWrite.complete(null);
        secondResult.get(5, TimeUnit.SECONDS);
        thirdResult.get(5, TimeUnit.SECONDS);
        assertTrue(thirdStarted.get());
    }

    @Test(timeOut = 10_000)
    public void testQueueDrainedBeforeAcquiringOperation() throws Exception {
        AtomicInteger stage = new AtomicInteger();
        AtomicReference<CompletableFuture<Void>> appendedWhileOperating = new AtomicReference<>();
        worker.taskQueue = new ConcurrentLinkedDeque<>() {
            @Override
            public boolean isEmpty() {
                boolean empty = super.isEmpty();
                if (!empty && stage.compareAndSet(0, 1)) {
                    // Another caller drains the queue after the initial snapshot, before ownership is acquired.
                    worker.appendTask(PersistentWorker.OperationType.WriteSegment,
                            () -> CompletableFuture.completedFuture(null));
                    stage.set(2);
                } else if (empty && stage.compareAndSet(2, 3)) {
                    // An append observes Operating after the second empty snapshot, before ownership is released.
                    appendedWhileOperating.set(worker.appendTask(PersistentWorker.OperationType.WriteSegment,
                            () -> CompletableFuture.completedFuture(null)));
                }
                return empty;
            }
        };
        worker.appendTask(PersistentWorker.OperationType.WriteSegment,
                () -> CompletableFuture.completedFuture(null)).get(5, TimeUnit.SECONDS);
        assertEquals(stage.get(), 3, "Both queue observations must be exercised");
        appendedWhileOperating.get().get(5, TimeUnit.SECONDS);
        worker.appendTask(PersistentWorker.OperationType.WriteSegment,
                () -> CompletableFuture.completedFuture(null)).get(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        worker.closeAsync().get(5, TimeUnit.SECONDS);
        processor.closeAsync().get(5, TimeUnit.SECONDS);
        executor.shutdownNow();
    }
}
