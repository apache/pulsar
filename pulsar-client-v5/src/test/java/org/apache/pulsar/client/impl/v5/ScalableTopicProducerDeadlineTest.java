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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.v5.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Payload cleanup when exclusive producer creation stays pending after the logical send ends. */
public class ScalableTopicProducerDeadlineTest {

    @DataProvider(name = "sendModes")
    public Object[][] sendModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "sendModes")
    public void timeoutClearsTheQueuedPayloadWithoutCancellingSharedProducerCreation(boolean synchronous)
            throws Exception {
        try (var harness = new Harness()) {
            var send = harness.send(synchronous);
            assertThat(harness.queued).hasSize(1);
            assertThat(harness.queued.get(0).get()).isNotNull();
            assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
            assertThat(harness.memory.currentUsage()).isGreaterThan(ScalableTopicProducer.PER_MESSAGE_OVERHEAD_BYTES);

            harness.tasks.get(0).run(harness.timeouts.get(0));
            assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
            assertThat(harness.queued.get(0).get()).isNull();
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.creation).isNotDone();
            assertThat(harness.memory.currentUsage()).isZero();
            verify(harness.timeouts.get(0)).cancel();

            // A stale continuation still attached to the shared creation future cannot enqueue the
            // expired message. Its caller-visible result must stay a timeout after creation succeeds.
            var segment = harness.completeCreation();
            verify(segment, never()).newMessage();
            assertThat(send).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
        }
    }

    @Test(dataProvider = "sendModes")
    public void closeClearsQueuedPayloadsAndLeavesSharedProducerCreationPending(boolean synchronous) throws Exception {
        try (var harness = new Harness()) {
            var first = harness.send(synchronous);
            var second = harness.send();
            assertThat(harness.queued).hasSize(2).allSatisfy(ref -> assertThat(ref.get()).isNotNull());

            assertThat(harness.producer.closeAsync()).isCompleted();
            assertThat(first).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(AlreadyClosedException.class);
            assertThat(second).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(AlreadyClosedException.class);
            assertThat(harness.queued).allSatisfy(ref -> assertThat(ref.get()).isNull());
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.creation).isNotDone();
            assertThat(harness.memory.currentUsage()).isZero();
            for (var timeout : harness.timeouts) {
                verify(timeout).cancel();
            }

            var segment = harness.completeCreation();
            verify(segment, never()).newMessage();
        }
    }

    @Test(dataProvider = "sendModes")
    public void timeBlockedOnTheMemoryLimitDoesNotCountAgainstTheSendTimeout(boolean synchronous) throws Exception {
        long sendTimeoutMs = 50;
        long sendTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(sendTimeoutMs);
        try (var harness = new Harness(sendTimeoutMs, true)) {
            // One reservation may go over the limit, so go past it for the next send to block.
            long overLimit = harness.memory.memoryLimit() + 1;
            harness.memory.forceReserveMemory(overLimit);
            var send = harness.sendFromCaller(synchronous);
            Thread caller = await().until(harness.callingThread::get, Objects::nonNull);
            await().until(() -> caller.getState() == Thread.State.WAITING);
            // Keep the caller blocked on the memory limit for longer than the send timeout.
            long blockedAt = System.nanoTime();
            await().until(() -> System.nanoTime() - blockedAt > 2 * sendTimeoutNanos);
            assertThat(send).isNotDone();
            // Still waiting for admission: nothing has been dispatched yet.
            assertThat(harness.creationRequested.getCount()).isEqualTo(1);
            assertThat(harness.producer.queuedDispatchCount()).isZero();

            harness.memory.releaseMemory(overLimit);
            await().until(() -> send.isDone() || harness.creationRequested.getCount() == 0);
            assertThat(send).isNotDone();
            // The admitted send gets its full timeout, and is then still waiting on producer creation.
            assertThat(harness.delaysNanos).containsExactly(sendTimeoutNanos);
            assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);

            harness.tasks.get(0).run(harness.timeouts.get(0));
            assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    @Test
    public void interruptingASynchronousSendRemovesItsWaiterBeforeProducerCreationFinishes() throws Exception {
        try (var harness = new Harness()) {
            var send = harness.send(true);
            assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
            harness.callingThread.get().interrupt();
            assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(PulsarClientException.class);
            assertThat(harness.interruptedAtReturn).isTrue();
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.queued).allSatisfy(ref -> assertThat(ref.get()).isNull());
            assertThat(harness.memory.currentUsage()).isZero();
            assertThat(harness.creation).isNotDone();
            Producer<byte[]> createdAfterInterruption = harness.completeCreation();
            verify(createdAfterInterruption, never()).newMessage();
            assertThat(harness.producer.inFlightSendCount()).isZero();
        }
    }

    @Test
    public void repeatedTimeoutsRemoveWaitersWhileSharedProducerCreationNeverCompletes() throws Exception {
        try (var harness = new Harness()) {
            int creationDependents = -1;
            for (int i = 0; i < 1000; i++) {
                var send = harness.send();
                if (creationDependents == -1) {
                    creationDependents = harness.creation.getNumberOfDependents();
                }
                assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
                harness.tasks.get(i).run(harness.timeouts.get(i));
                assertThat(send).isCompletedExceptionally();
                assertThat(harness.producer.queuedDispatchCount()).isZero();
                assertThat(harness.memory.currentUsage()).isZero();
                assertThat(harness.creation).isNotDone();
                assertThat(harness.creation.getNumberOfDependents()).isEqualTo(creationDependents);
            }
            assertThat(harness.producer.segmentProducersCreated.get()).isEqualTo(1);
            assertThat(harness.queued).allSatisfy(ref -> assertThat(ref.get()).isNull());
        }
    }

    @Test
    public void disabledTimeoutKeepsPendingWaitersBoundedByTheClientMemoryLimit() {
        try (var harness = new Harness(0)) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                sends.add(harness.send());
            }
            assertThat(harness.tasks).isEmpty();
            int waiting = harness.producer.queuedDispatchCount();
            assertThat(waiting).isPositive().isLessThan(sends.size());
            assertThat(sends.stream().filter(send -> !send.isDone()).count()).isEqualTo(waiting);
            assertThat(harness.producer.segmentProducersCreated.get()).isEqualTo(1);
            assertThat(harness.creation).isNotDone();

            assertThat(harness.producer.closeAsync()).isCompleted();
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.memory.currentUsage()).isZero();
            assertThat(sends).allMatch(CompletableFuture::isCompletedExceptionally);
        }
    }

    @Test
    public void aProducerCreatedAfterCloseStillClosesWithoutADispatchExecutor() {
        try (var harness = new Harness()) {
            AtomicReference<CompletableFuture<Void>> closed = new AtomicReference<>();
            doAnswer(__ -> {
                // The underlying creation call has started, but its future has not been registered
                // in the logical producer's map yet. Close cannot snapshot this creation attempt.
                closed.set(harness.producer.closeAsync());
                assertThat(closed.get()).isCompleted();
                harness.dispatch.shutdownNow();
                return harness.creation;
            }).when(harness.v4).createSegmentProducerAsync(any(), any());

            var send = harness.send();
            assertThat(closed.get()).isCompleted();
            assertThat(send).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(AlreadyClosedException.class);
            assertThat(harness.memory.currentUsage()).isZero();
            var segment = harness.completeCreation();
            verify(segment).closeAsync();
            verify(segment, never()).newMessage();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void closeWaitsForAnExecutingCreationBatchAndFailsItsRemainingSends() throws Exception {
        try (var harness = new Harness();
                var worker = Executors.newSingleThreadExecutor(new DefaultThreadFactory("producer-close-gate"))) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            Producer<byte[]> segment = mock(Producer.class);
            TypedMessageBuilderImpl<byte[]> builder = mock(TypedMessageBuilderImpl.class, RETURNS_SELF);
            when(segment.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            when(segment.newMessage()).thenAnswer(__ -> {
                entered.countDown();
                assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
                return builder;
            });
            when(builder.sendAsync()).thenReturn(new CompletableFuture<>());
            var first = harness.send();
            var second = harness.send();
            var created = worker.submit(() -> harness.creation.complete(segment));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                var closed = harness.producer.closeAsync();
                assertThat(closed).isNotDone();
                resume.countDown();
                created.get(10, TimeUnit.SECONDS);
                assertThat(closed).succeedsWithin(10, TimeUnit.SECONDS);
                assertThat(first).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(AlreadyClosedException.class);
                assertThat(second).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(AlreadyClosedException.class);
                assertThat(harness.memory.currentUsage()).isZero();
                verify(builder, never()).sendAsync();
                verify(segment).closeAsync();
            } finally {
                resume.countDown();
            }
        }
    }

    private static final class Harness implements AutoCloseable {
        final CompletableFuture<Producer<byte[]>> creation = new CompletableFuture<>();
        final MemoryLimitController memory = new MemoryLimitController(4 * 1024 * 1024);
        final List<AtomicReference<?>> queued = new ArrayList<>();
        final List<TimerTask> tasks = new ArrayList<>();
        final List<Timeout> timeouts = new ArrayList<>();
        final List<Long> delaysNanos = new ArrayList<>();
        final ExecutorService dispatch = MoreExecutors.newDirectExecutorService();
        final ExecutorService completion = MoreExecutors.newDirectExecutorService();
        final ExecutorService caller = Executors.newSingleThreadExecutor(new DefaultThreadFactory("deadline-sync"));
        final CountDownLatch creationRequested = new CountDownLatch(1);
        final AtomicReference<Thread> callingThread = new AtomicReference<>();
        final AtomicBoolean interruptedAtReturn = new AtomicBoolean();
        final ScalableTopicProducer<byte[]> producer;
        final PulsarClientImpl v4;

        Harness() {
            this(1000);
        }

        Harness(long sendTimeoutMs) {
            this(sendTimeoutMs, false);
        }

        Harness(long sendTimeoutMs, boolean blockIfQueueFull) {
            v4 = mock(PulsarClientImpl.class);
            when(v4.getMemoryLimitController()).thenReturn(memory);
            when(v4.getInternalExecutorService()).thenReturn(dispatch);
            EventLoopGroup eventLoops = mock(EventLoopGroup.class);
            when(eventLoops.iterator()).thenAnswer(__ -> Collections.emptyIterator());
            when(v4.eventLoopGroup()).thenReturn(eventLoops);
            ExecutorProvider external = mock(ExecutorProvider.class);
            when(external.getExecutor()).thenReturn(completion);
            when(v4.externalExecutorProvider()).thenReturn(external);
            doAnswer(__ -> {
                creationRequested.countDown();
                return creation;
            }).when(v4).createSegmentProducerAsync(any(), any());
            Timer timer = mock(Timer.class);
            when(v4.timer()).thenReturn(timer);
            when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
                tasks.add(invocation.getArgument(0));
                delaysNanos.add(invocation.<TimeUnit>getArgument(2).toNanos(invocation.getArgument(1)));
                Timeout timeout = mock(Timeout.class);
                timeouts.add(timeout);
                return timeout;
            });

            TopicName topic = TopicName.get("topic://public/default/queued-deadline-test");
            ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(0);
            dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(0);
            DagWatchClient watch = mock(DagWatchClient.class);
            when(watch.topicName()).thenReturn(topic);
            ProducerConfigurationData conf = new ProducerConfigurationData();
            conf.setTopicName(topic.toString());
            conf.setAccessMode(ProducerAccessMode.WaitForExclusive);
            conf.setSendTimeoutMs(sendTimeoutMs);
            conf.setBlockIfQueueFull(blockIfQueueFull);
            producer = new ScalableTopicProducer<>(new PulsarClientV5(v4, "test", null), Schema.bytes(),
                    conf, watch, ClientSegmentLayout.fromProto(dag, topic));
            producer.queuedDispatchHook = queued::add;
        }

        CompletableFuture<MessageId> send() {
            return producer.sendInternalAsync("key", new byte[128 * 1024],
                    null, null, null, null, null, null, null);
        }

        CompletableFuture<MessageId> send(boolean synchronous) throws InterruptedException {
            if (!synchronous) {
                return send();
            }
            CompletableFuture<MessageId> result = sendFromCaller(true);
            assertThat(creationRequested.await(10, TimeUnit.SECONDS)).isTrue();
            return result;
        }

        /** Issues the send on {@link #caller}, which may block there on the client memory limit. */
        CompletableFuture<MessageId> sendFromCaller(boolean synchronous) {
            CompletableFuture<MessageId> result = new CompletableFuture<>();
            caller.execute(() -> {
                callingThread.set(Thread.currentThread());
                if (!synchronous) {
                    send().whenComplete((id, failure) -> {
                        if (failure == null) {
                            result.complete(id);
                        } else {
                            result.completeExceptionally(failure);
                        }
                    });
                    return;
                }
                try {
                    MessageId id = producer.sendInternal("key", new byte[128 * 1024],
                            null, null, null, null, null, null, null);
                    interruptedAtReturn.set(Thread.currentThread().isInterrupted());
                    result.complete(id);
                } catch (Throwable failure) {
                    interruptedAtReturn.set(Thread.currentThread().isInterrupted());
                    result.completeExceptionally(failure);
                }
            });
            return result;
        }

        @SuppressWarnings("unchecked")
        Producer<byte[]> completeCreation() {
            Producer<byte[]> segment = mock(Producer.class);
            when(segment.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            creation.complete(segment);
            return segment;
        }

        @Override
        public void close() {
            producer.closeAsync().join();
            caller.shutdownNow();
            dispatch.shutdownNow();
            completion.shutdownNow();
        }
    }
}
