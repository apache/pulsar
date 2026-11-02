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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.ScheduledFuture;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.v5.auth.EncryptionKey;
import org.apache.pulsar.client.api.v5.auth.PublicKeyProvider;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** The same bounded retry sequence for blocking sends and asynchronous sends. */
public class ScalableTopicProducerRetryContractTest {
    private static final TopicName TOPIC = TopicName.get("topic://public/default/retry-contract");

    @DataProvider(name = "sendModes")
    public Object[][] sendModes() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider(name = "sendModesAndTimeouts")
    public Object[][] sendModesAndTimeouts() {
        return new Object[][] {{false, 0L}, {true, 0L}, {false, 120_000L}, {true, 120_000L}};
    }

    @DataProvider(name = "closeModes")
    public Object[][] closeModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "sendModesAndTimeouts")
    public void tenAttemptsRemainTheLimitAcrossSuccessiveLayouts(boolean synchronous, long timeoutMs)
            throws Exception {
        try (var harness = new Harness(timeoutMs)) {
            var send = harness.send(synchronous);
            TopicTerminatedException lastFailure = null;
            for (int attempt = 1; attempt <= 10; attempt++) {
                Attempt dispatched = harness.nextAttempt();
                assertThat(dispatched.segmentId()).isEqualTo(attempt - 1);
                lastFailure = new TopicTerminatedException("sealed segment " + (attempt - 1));
                dispatched.fail(lastFailure);
                if (attempt < 10) {
                    assertThat(send).isNotDone();
                    harness.applyLayout(attempt);
                }
            }
            assertRetryExhausted(send, lastFailure);
            assertThat(harness.attempts).hasSize(10);
            assertThat(harness.memory.currentUsage()).isZero();
            assertThat(harness.producer.waitingSendCount()).isZero();
            assertThat(harness.producer.inFlightSendCount()).isZero();
            assertThat(harness.timeouts).hasSize(timeoutMs == 0 ? 0 : 1);

            harness.applyLayout(10);
            assertThat(harness.pendingAttempts).isEmpty();
        }
    }

    @Test(dataProvider = "sendModesAndTimeouts")
    public void waitingForAMissingLayoutHasABoundedRetirementBudget(boolean synchronous, long timeoutMs)
            throws Exception {
        try (var harness = new Harness(timeoutMs)) {
            var send = harness.send(synchronous);
            var terminated = new TopicTerminatedException("sealed without a successor layout");
            harness.nextAttempt().fail(terminated);
            for (int check = 1; check < 10; check++) {
                harness.producer.checkRetirement(0, check);
                assertThat(send).isNotDone();
                assertThat(harness.attempts).hasSize(1);
            }
            harness.producer.checkRetirement(0, 10);
            assertRetryExhausted(send, terminated);
            assertThat(harness.timeouts).hasSize(timeoutMs == 0 ? 0 : 1);
            assertThat(harness.memory.currentUsage()).isZero();
            assertThat(harness.producer.waitingSendCount()).isZero();
            harness.applyLayout(1);
            assertThat(harness.pendingAttempts).isEmpty();
        }
    }

    @Test(dataProvider = "sendModes")
    public void aShortDeadlineWinsBeforeTheRetryBudgetIsExhausted(boolean synchronous) throws Exception {
        try (var harness = new Harness(100)) {
            var send = harness.send(synchronous);
            var terminated = new TopicTerminatedException("sealed");
            harness.nextAttempt().fail(terminated);
            harness.producer.checkRetirement(0, 1);
            assertThat(send).isNotDone();
            assertThat(harness.timeouts).hasSize(1);
            harness.timeouts.get(0).fire();
            assertTimeout(send, terminated);
            harness.producer.checkRetirement(0, 10);
            harness.applyLayout(1);
            assertThat(harness.attempts).hasSize(1);
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    @Test(dataProvider = "sendModes")
    public void reRoutingKeepsTheOriginalLogicalDeadline(boolean synchronous) throws Exception {
        try (var harness = new Harness(1000)) {
            var send = harness.send(synchronous);
            var terminated = new TopicTerminatedException("sealed");
            harness.nextAttempt().fail(terminated);
            TestTimeout original = harness.timeouts.get(0);
            harness.applyLayout(1);
            Attempt successor = harness.nextAttempt();
            assertThat(successor.segmentId()).isEqualTo(1);
            assertThat(harness.timeouts).containsExactly(original);
            assertThat(original.delayNanos).isPositive().isLessThanOrEqualTo(TimeUnit.SECONDS.toNanos(1));
            original.fire();
            assertTimeout(send, terminated);
            successor.result().complete(MessageId.earliest);
            assertTimeout(send, terminated);
            assertThat(harness.memory.currentUsage()).isZero();
            assertThat(harness.producer.inFlightSendCount()).isZero();
        }
    }

    @Test
    public void aSynchronousSendInsideTheCompletionExecutorDoesNotWaitForThatExecutor() throws Exception {
        ExecutorService completion = Executors.newSingleThreadExecutor(new DefaultThreadFactory("sync-send-callback"));
        try (var harness = new Harness(1000, completion)) {
            CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send = new CompletableFuture<>();
            completion.execute(() -> {
                try {
                    var id = harness.producer.sendInternal("key", "value", null, null, null, null, null, null, null);
                    // finish must untrack the blocking send before releasing its private result.
                    // A flush here cannot await a tracking completion queued behind this callback.
                    harness.producer.flushAsync().get(10, TimeUnit.SECONDS);
                    harness.producer.closeAsync().get(10, TimeUnit.SECONDS);
                    send.complete(id);
                } catch (Throwable failure) {
                    send.completeExceptionally(failure);
                }
            });
            Attempt dispatched = harness.nextAttempt();
            dispatched.result().complete(MessageId.earliest);
            assertThat(send).succeedsWithin(10, TimeUnit.SECONDS);
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    @Test
    public void aSynchronousSendFlushesInsteadOfWaitingForTheBatchingDelay() throws Exception {
        try (var harness = new Harness(1000)) {
            harness.acknowledgeOnFlush = true;
            var send = harness.send(true);
            Attempt dispatched = harness.nextAttempt();
            assertThat(harness.flushed.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(send).succeedsWithin(10, TimeUnit.SECONDS);
            assertThat(dispatched.result()).isCompleted();
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    @Test
    public void aFlushAlreadyWaitingForASynchronousSendSurvivesDroppedExternalCompletions() throws Exception {
        var completion = new DiscardingExecutor();
        try (var harness = new Harness(1000, completion)) {
            var send = harness.send(true);
            harness.nextAttempt();
            CompletableFuture<Void> flushing = harness.producer.flushAsync();
            assertThat(flushing).isNotDone();
            AtomicReference<Thread> deliveredOn = new AtomicReference<>();
            var observed = flushing.handle((__, failure) -> {
                deliveredOn.set(Thread.currentThread());
                return failure;
            });

            // This call stands for the timer thread, which cannot execute a flush continuation.
            Thread timerThread = Thread.currentThread();
            harness.timeouts.get(0).fire();
            // Client shutdown discards queued work. A flush which already captured this send
            // must still settle; its completion belongs to the blocking application caller.
            completion.shutdownNow();
            assertThat(observed).succeedsWithin(10, TimeUnit.SECONDS);
            assertThat(flushing).isCompletedExceptionally();
            assertThat(observed.join()).isNotNull();
            assertThat(deliveredOn.get()).isNotSameAs(timerThread).isSameAs(harness.synchronousCaller.get());
            assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
            assertThat(harness.producer.flushAsync()).isCompleted();
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    @Test(dataProvider = "closeModes")
    public void closeDrainsAcceptedAsynchronousTimeoutCompletions(boolean closeClient) throws Exception {
        var completion = new DiscardingExecutor();
        try (var harness = new Harness(1000, completion)) {
            CompletableFuture<Producer<String>> creation = new CompletableFuture<>();
            doReturn(creation).when(harness.v4).createSegmentProducerAsync(any(), any());
            when(harness.v4.closeAsync()).thenAnswer(__ -> {
                completion.shutdownNow();
                return CompletableFuture.completedFuture(null);
            });
            var send = harness.send(false);
            var flushing = harness.producer.flushAsync();
            assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
            assertThat(flushing).isNotDone();

            harness.timeouts.get(0).fire();
            assertThat(completion.pendingCount()).isEqualTo(1);
            assertThat(send).isNotDone();
            assertThat(flushing).isNotDone();
            assertThat(harness.producer.queuedDispatchCount()).isZero();
            assertThat(harness.memory.currentUsage()).isZero();

            var closed = closeClient ? harness.client.closeAsync() : harness.producer.closeAsync();
            assertThat(closed).succeedsWithin(10, TimeUnit.SECONDS);
            // A client close drops the accepted completion rather than running it. A standalone
            // producer close must also have delivered the terminal result before that can happen.
            completion.shutdownNow();
            assertThat(completion.pendingCount()).isZero();
            assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
            assertThat(flushing).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(TimeoutException.class);
            assertThat(harness.producer.flushAsync()).isCompleted();
            assertThat(creation).isNotDone();
        }
    }

    @Test
    public void closeDrainsATimeoutWhileItsAcceptedExecutorSubmissionHasNotReturned() throws Exception {
        var completion = new GatedSubmissionExecutor();
        try (var harness = new Harness(1000, completion);
                var timerThread = Executors.newSingleThreadExecutor(new DefaultThreadFactory("completion-timer"))) {
            CompletableFuture<Producer<String>> creation = new CompletableFuture<>();
            doReturn(creation).when(harness.v4).createSegmentProducerAsync(any(), any());
            var send = harness.send(false);
            var flushing = harness.producer.flushAsync();
            var timerReturned = timerThread.submit(() -> {
                harness.timeouts.get(0).fire();
                return null;
            });
            try {
                assertThat(completion.accepted.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(send).isNotDone();
                assertThat(flushing).isNotDone();
                assertThat(harness.memory.currentUsage()).isZero();
                assertThat(harness.producer.closeAsync()).succeedsWithin(10, TimeUnit.SECONDS);
                assertThat(completion.shutdownNow()).hasSize(1);
                assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(flushing).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(creation).isNotDone();
            } finally {
                completion.returnFromSubmission.countDown();
            }
            timerReturned.get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "closeModes")
    public void closeDrainedTimeoutCallbackCanSynchronouslyCloseTheProducerOrClient(boolean closeClient)
            throws Exception {
        var completion = new DiscardingExecutor();
        try (var harness = new Harness(1000, completion);
                var externalCloser = Executors.newSingleThreadExecutor(new DefaultThreadFactory("reentrant-close"))) {
            CompletableFuture<Producer<String>> creation = new CompletableFuture<>();
            doReturn(creation).when(harness.v4).createSegmentProducerAsync(any(), any());
            var send = harness.send(false);
            var flushing = harness.producer.flushAsync();
            CountDownLatch callbackEntered = new CountDownLatch(1);
            CountDownLatch allowCallbackClose = new CountDownLatch(1);
            CompletableFuture<Void> callbackClosed = new CompletableFuture<>();
            send.whenComplete((__, failure) -> {
                callbackEntered.countDown();
                try {
                    assertThat(allowCallbackClose.await(10, TimeUnit.SECONDS)).isTrue();
                    if (closeClient) {
                        harness.client.close();
                    } else {
                        harness.producer.close();
                    }
                    callbackClosed.complete(null);
                } catch (Throwable closeFailure) {
                    callbackClosed.completeExceptionally(closeFailure);
                }
            });
            harness.timeouts.get(0).fire();
            assertThat(completion.pendingCount()).isEqualTo(1);
            assertThat(send).isNotDone();
            assertThat(flushing).isNotDone();
            var closed = externalCloser.submit(() -> {
                if (closeClient) {
                    harness.client.close();
                } else {
                    harness.producer.close();
                }
                return null;
            });
            try {
                assertThat(callbackEntered.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(callbackClosed).isNotDone();
                assertThat(closed.isDone()).isFalse();
                // The outer close is delivering this callback. Its synchronous nested close
                // must not wait for a close future whose owner is paused inside the callback.
                allowCallbackClose.countDown();
                assertThat(callbackClosed).succeedsWithin(10, TimeUnit.SECONDS);
                closed.get(10, TimeUnit.SECONDS);
                assertThat(completion.shutdownNow()).hasSize(1);
                assertThat(send).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(flushing).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(harness.memory.currentUsage()).isZero();
                assertThat(creation).isNotDone();
            } finally {
                allowCallbackClose.countDown();
                // An incorrect close implementation can leave the worker in its nested get().
                // Interrupt that wait before the executor's AutoCloseable cleanup joins it.
                externalCloser.shutdownNow();
            }
        }
    }

    @Test
    public void aBlockedWarmDispatchCannotDelayAColdSegmentsLogicalTimeout() throws Exception {
        ExecutorService dispatch = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-crypto-dispatch"));
        try (var harness = new Harness(120_000, MoreExecutors.newDirectExecutorService(), dispatch);
                var warmCaller = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-crypto-caller"));
                var followingCaller = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-next-caller"));
                var timerThread = Executors.newSingleThreadExecutor(new DefaultThreadFactory("cold-logical-timer"))) {
            ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(1);
            dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0x7fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(0);
            dag.addSegment().setSegmentId(1).setHashStart(0x8000).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(1);
            harness.applyLayout(ClientSegmentLayout.fromProto(dag, TOPIC));
            String warmKey = keyForSegment(harness.current, 0);
            String coldKey = keyForSegment(harness.current, 1);
            CompletableFuture<EncryptionKey> publicKey = new CompletableFuture<>();
            CompletableFuture<Producer<String>> coldCreation = new CompletableFuture<>();
            CountDownLatch keyRequested = new CountDownLatch(1);
            AtomicInteger requests = new AtomicInteger();
            ProducerImpl<String> physical = remoteKeyProducer(harness, name -> {
                assertThat(name).isEqualTo("remote-key");
                if (requests.incrementAndGet() == 2) {
                    keyRequested.countDown();
                    return publicKey;
                }
                return CompletableFuture.completedFuture(EncryptionKey.of(new byte[0]));
            });
            doAnswer(invocation -> {
                ProducerConfigurationData conf = invocation.getArgument(0);
                return harness.segmentIds.get(conf.getTopicName()) == 0
                        ? CompletableFuture.completedFuture(physical) : coldCreation;
            }).when(harness.v4).createSegmentProducerAsync(any(), any());
            try {
                // The first real encryption attempt fails immediately, but leaves a completed
                // chain head. The executor barrier also waits for the head's final completion.
                var warmup = harness.send(warmKey);
                assertThat(warmup).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(PulsarClientException.CryptoException.class);
                dispatch.submit(() -> { }).get(10, TimeUnit.SECONDS);
                assertThat(requests).hasValue(1);
                assertThat(harness.memory.currentUsage()).isZero();

                List<AtomicReference<?>> queued = new CopyOnWriteArrayList<>();
                CountDownLatch followingQueued = new CountDownLatch(1);
                harness.producer.queuedDispatchHook = ref -> {
                    queued.add(ref);
                    if (queued.size() == 3) {
                        followingQueued.countDown();
                    }
                };
                var cold = harness.send(coldKey);
                assertThat(harness.timeouts).hasSize(2);
                assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
                assertThat(queued.get(0).get()).isNotNull();
                long coldReservation = harness.memory.currentUsage();
                assertThat(coldReservation).isPositive();
                var blockedSubmission = warmCaller.submit(() -> harness.send(warmKey));
                assertThat(keyRequested.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(publicKey).isNotDone();
                var followingSubmission = followingCaller.submit(() -> harness.send(warmKey));
                assertThat(followingQueued.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(requests).hasValue(2);
                long beforeTimeout = harness.memory.currentUsage();

                var timerReturned = timerThread.submit(() -> {
                    harness.timeouts.get(1).fire();
                    return null;
                });
                timerReturned.get(10, TimeUnit.SECONDS);
                assertThat(cold).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(queued.get(0).get()).isNull();
                var following = followingSubmission.get(10, TimeUnit.SECONDS);
                assertThat(harness.producer.queuedDispatchCount()).isEqualTo(1);
                assertThat(harness.memory.currentUsage()).isEqualTo(beforeTimeout - coldReservation);
                assertThat(timerThread.submit(() -> true).get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(publicKey).isNotDone();
                assertThat(coldCreation).isNotDone();
                assertThat(requests).hasValue(2);

                // Neither the timeout nor the queued successor recalls the already claimed V4
                // handoff. Releasing its actual key lookup lets both warm sends finish in order.
                publicKey.complete(EncryptionKey.of(new byte[0]));
                assertThat(blockedSubmission.get(10, TimeUnit.SECONDS))
                        .failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(PulsarClientException.CryptoException.class);
                assertThat(following)
                        .failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(PulsarClientException.CryptoException.class);
                assertThat(requests).hasValue(3);
                assertThat(harness.memory.currentUsage()).isZero();
                assertThat(harness.producer.closeAsync()).succeedsWithin(10, TimeUnit.SECONDS);
                assertThat(coldCreation).isNotDone();
            } finally {
                publicKey.complete(EncryptionKey.of(new byte[0]));
                warmCaller.submit(() -> { }).get(10, TimeUnit.SECONDS);
                followingCaller.submit(() -> { }).get(10, TimeUnit.SECONDS);
                dispatch.submit(() -> { }).get(10, TimeUnit.SECONDS);
                physical.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void timedOutWarmWaitersAreRemovedWhileThePhysicalHandoffRemainsBlocked() throws Exception {
        try (var fixture = new BlockedWarmDispatch();
                var followers = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-timeout-followers"));
                var timer = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-timeout-timer"))) {
            try {
                var blocked = fixture.block();
                long baseline = fixture.harness.memory.currentUsage();
                assertThat(baseline).isPositive();
                List<AtomicReference<?>> retained = new CopyOnWriteArrayList<>();
                fixture.harness.producer.queuedDispatchHook = retained::add;
                for (int wave = 0; wave < 3; wave++) {
                    int firstTimeout = fixture.harness.timeouts.size();
                    int sequence = 2 + wave * 64;
                    var queued = followers.submit(() -> {
                        List<CompletableFuture<org.apache.pulsar.client.api.v5.MessageId>> sends = new ArrayList<>();
                        for (int i = 0; i < 64; i++) {
                            sends.add(fixture.send(sequence + i));
                        }
                        return sends;
                    }).get(10, TimeUnit.SECONDS);
                    assertThat(fixture.harness.producer.queuedDispatchCount()).isEqualTo(64);
                    assertThat(fixture.harness.memory.currentUsage()).isGreaterThan(baseline);
                    timer.submit(() -> {
                        for (int i = firstTimeout; i < firstTimeout + 64; i++) {
                            fixture.harness.timeouts.get(i).fire();
                        }
                        return null;
                    }).get(10, TimeUnit.SECONDS);
                    for (var send : queued) {
                        assertThat(send).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().havingCause()
                                .isInstanceOf(TimeoutException.class);
                    }
                    assertThat(fixture.harness.producer.queuedDispatchCount()).isZero();
                    assertThat(retained).allSatisfy(reference -> assertThat(reference.get()).isNull());
                    assertThat(fixture.harness.memory.currentUsage()).isEqualTo(baseline);
                    assertThat(fixture.requests).hasValue(2);
                    assertThat(fixture.publicKey).isNotDone();
                }
                fixture.release();
                assertThat(blocked.get(10, TimeUnit.SECONDS)).failsWithin(10, TimeUnit.SECONDS)
                        .withThrowableThat().havingCause().isInstanceOf(PulsarClientException.CryptoException.class);
                assertThat(fixture.requests).hasValue(2);
                assertThat(fixture.harness.memory.currentUsage()).isZero();
            } finally {
                // A baseline sender may be waiting for dispatchLock; release the actual crypto
                // handoff before AutoCloseable joins either application/timer executor.
                fixture.release();
            }
        }
    }

    @Test
    public void aLargeWarmBacklogDrainsIterativelyInCallOrder() throws Exception {
        try (var fixture = new BlockedWarmDispatch();
                var followers = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-backlog-followers"))) {
            try {
                var blocked = fixture.block();
                int backlog = 4096;
                var queued = followers.submit(() -> {
                    List<CompletableFuture<org.apache.pulsar.client.api.v5.MessageId>> sends = new ArrayList<>();
                    for (int i = 0; i < backlog; i++) {
                        sends.add(fixture.send(2L + i));
                    }
                    return sends;
                }).get(10, TimeUnit.SECONDS);
                assertThat(queued).hasSize(backlog).allSatisfy(send -> assertThat(send).isNotDone());
                assertThat(fixture.requests).hasValue(2);
                fixture.release();
                assertThat(blocked.get(10, TimeUnit.SECONDS)).failsWithin(10, TimeUnit.SECONDS)
                        .withThrowableThat().havingCause().isInstanceOf(PulsarClientException.CryptoException.class);
                for (var send : queued) {
                    assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                            .isInstanceOf(PulsarClientException.CryptoException.class);
                }
                List<Long> expected = new ArrayList<>();
                for (long sequence = 0; sequence < backlog + 2; sequence++) {
                    expected.add(sequence);
                }
                assertThat(fixture.dispatched).containsExactlyElementsOf(expected);
                assertThat(fixture.requests).hasValue(backlog + 2);
                assertThat(fixture.harness.producer.queuedDispatchCount()).isZero();
                assertThat(fixture.harness.memory.currentUsage()).isZero();
            } finally {
                fixture.release();
            }
        }
    }

    /** One completed warm dispatch followed by a real crypto handoff blocked on its remote key. */
    private static final class BlockedWarmDispatch implements AutoCloseable {
        final ExecutorService dispatch = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-dispatch"));
        final ExecutorService caller = Executors.newSingleThreadExecutor(new DefaultThreadFactory("warm-caller"));
        final Harness harness = new Harness(120_000, MoreExecutors.newDirectExecutorService(), dispatch);
        final CompletableFuture<EncryptionKey> publicKey = new CompletableFuture<>();
        final CountDownLatch keyRequested = new CountDownLatch(1);
        final AtomicInteger requests = new AtomicInteger();
        final List<Long> dispatched = new CopyOnWriteArrayList<>();
        final ProducerImpl<String> physical;

        BlockedWarmDispatch() {
            physical = remoteKeyProducer(harness, name -> {
                assertThat(name).isEqualTo("remote-key");
                if (requests.incrementAndGet() == 2) {
                    keyRequested.countDown();
                    return publicKey;
                }
                return CompletableFuture.completedFuture(EncryptionKey.of(new byte[0]));
            }, metadata -> dispatched.add(metadata.getSequenceId()));
            doReturn(CompletableFuture.completedFuture(physical)).when(harness.v4)
                    .createSegmentProducerAsync(any(), any());
        }

        Future<CompletableFuture<org.apache.pulsar.client.api.v5.MessageId>> block() throws Exception {
            assertThat(send(0)).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                    .isInstanceOf(PulsarClientException.CryptoException.class);
            dispatch.submit(() -> { }).get(10, TimeUnit.SECONDS);
            var blocked = caller.submit(() -> send(1));
            assertThat(keyRequested.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(requests).hasValue(2);
            return blocked;
        }

        CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send(long sequence) {
            return harness.producer.sendInternalAsync("key", "value", null, null, sequence,
                    null, null, null, null);
        }

        void release() {
            publicKey.complete(EncryptionKey.of(new byte[0]));
        }

        @Override
        public void close() throws Exception {
            release();
            try {
                caller.submit(() -> { }).get(10, TimeUnit.SECONDS);
                dispatch.submit(() -> { }).get(10, TimeUnit.SECONDS);
                physical.closeAsync().get(10, TimeUnit.SECONDS);
                harness.close();
            } finally {
                caller.shutdownNow();
                dispatch.shutdownNow();
            }
        }
    }

    private static String keyForSegment(ClientSegmentLayout layout, long segmentId) {
        SegmentRouter router = new SegmentRouter();
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            if (router.route(key, layout.activeSegments()) == segmentId) {
                return key;
            }
        }
        throw new AssertionError("No key found for segment " + segmentId);
    }

    @Test
    public void aPendingRemoteKeyProviderCannotBlockTheLogicalDeadlineTimer() throws Exception {
        ExecutorService dispatch = Executors.newSingleThreadExecutor(new DefaultThreadFactory("crypto-dispatch"));
        try (var harness = new Harness(1000, MoreExecutors.newDirectExecutorService(), dispatch);
                var timerThread = Executors.newSingleThreadExecutor(new DefaultThreadFactory("logical-timer"))) {
            CompletableFuture<EncryptionKey> publicKey = new CompletableFuture<>();
            CountDownLatch keyRequested = new CountDownLatch(1);
            PublicKeyProvider provider = name -> {
                assertThat(name).isEqualTo("remote-key");
                keyRequested.countDown();
                return publicKey;
            };
            ProducerImpl<String> physical = remoteKeyProducer(harness, provider);
            doReturn(CompletableFuture.completedFuture(physical)).when(harness.v4)
                    .createSegmentProducerAsync(any(), any());
            try {
                // This is the real TypedMessageBuilderImpl -> ProducerImpl -> public crypto SPI
                // path. The real V5 key adapter joins the provider's still-pending future.
                var send = harness.send(false);
                assertThat(keyRequested.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(publicKey).isNotDone();
                var timerReturned = timerThread.submit(() -> {
                    harness.timeouts.get(0).fire();
                    return null;
                });
                timerReturned.get(10, TimeUnit.SECONDS);
                assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                        .isInstanceOf(TimeoutException.class);
                assertThat(publicKey).isNotDone();
                assertThat(timerThread.submit(() -> true).get(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                publicKey.completeExceptionally(new IllegalStateException("release the remote-key fixture"));
                dispatch.submit(() -> { }).get(10, TimeUnit.SECONDS);
                physical.closeAsync().get(10, TimeUnit.SECONDS);
            }
            // The V4 call was already claimed. It may finish after the logical timeout; only
            // after it releases its own payload reservation can the whole memory account be zero.
            assertThat(harness.memory.currentUsage()).isZero();
        }
    }

    private static ProducerImpl<String> remoteKeyProducer(Harness harness, PublicKeyProvider provider) {
        return remoteKeyProducer(harness, provider, __ -> { });
    }

    private static ProducerImpl<String> remoteKeyProducer(Harness harness, PublicKeyProvider provider,
                                                         Consumer<MessageMetadata> onEncrypt) {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setStatsIntervalSeconds(0);
        ConnectionPool connections = mock(ConnectionPool.class);
        EventLoopGroup eventLoops = harness.v4.eventLoopGroup();
        ScheduledFuture<?> keyRotation = mock(ScheduledFuture.class);
        when(harness.v4.getConfiguration()).thenReturn(clientConf);
        when(harness.v4.getCnxPool()).thenReturn(connections);
        when(harness.v4.getClientClock()).thenReturn(Clock.systemUTC());
        when(harness.v4.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(harness.v4.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());
        doReturn(keyRotation).when(eventLoops)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any());
        ProducerConfigurationData encrypted = new ProducerConfigurationData();
        encrypted.setTopicName(harness.current.activeSegments().get(0).attachTopicName());
        encrypted.setProducerName("remote-key-fixture");
        encrypted.setBatchingEnabled(false);
        encrypted.setSendTimeoutMs(0);
        encrypted.setMemoryLimitAdmittedUpstream(true);
        encrypted.setEncryptionKeys(Set.of("remote-key"));
        encrypted.setCryptoKeyReader(CryptoKeyReaderAdapter.forProducer(provider));
        encrypted.setMessageCrypto(new RemoteKeyCrypto(onEncrypt));
        return new ConnectingCryptoProducer(harness.v4, encrypted);
    }

    private static void assertRetryExhausted(CompletableFuture<?> send, Throwable terminated) {
        assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                .isInstanceOf(PulsarClientException.class).isNotInstanceOf(TimeoutException.class)
                .withCause(terminated);
    }

    private static void assertTimeout(CompletableFuture<?> send, Throwable terminated) {
        assertThat(send).failsWithin(10, TimeUnit.SECONDS).withThrowableThat().havingCause()
                .isInstanceOf(TimeoutException.class).withCause(terminated);
    }

    private record Attempt(long segmentId, Acknowledgement result) {
        void fail(Throwable failure) throws InterruptedException {
            assertThat(result.completeExceptionally(failure)).isTrue();
            assertThat(result.processed.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Signal after the production acknowledgement callback, including an ACK before registration. */
    private static final class Acknowledgement extends CompletableFuture<MessageId> {
        final CountDownLatch processed = new CountDownLatch(1);

        @Override
        public CompletableFuture<MessageId> whenComplete(BiConsumer<? super MessageId, ? super Throwable> action) {
            return super.whenComplete((id, failure) -> {
                try {
                    action.accept(id, failure);
                } finally {
                    processed.countDown();
                }
            });
        }
    }

    private static final class Harness implements AutoCloseable {
        final List<Attempt> attempts = new CopyOnWriteArrayList<>();
        final BlockingQueue<Attempt> pendingAttempts = new LinkedBlockingQueue<>();
        final List<TestTimeout> timeouts = new CopyOnWriteArrayList<>();
        final MemoryLimitController memory = new MemoryLimitController(16 * 1024 * 1024);
        final CountDownLatch flushed = new CountDownLatch(1);
        final ExecutorService dispatch;
        final ExecutorService caller = Executors.newSingleThreadExecutor(new DefaultThreadFactory("sync-contract"));
        final ExecutorService completion;
        final PulsarClientImpl v4;
        final PulsarClientV5 client;
        final AtomicReference<Thread> synchronousCaller = new AtomicReference<>();
        final Map<String, Long> segmentIds = new ConcurrentHashMap<>();
        final AtomicReference<DagWatchClient.LayoutChangeListener> listener = new AtomicReference<>();
        final ScalableTopicProducer<String> producer;
        volatile boolean acknowledgeOnFlush;
        ClientSegmentLayout current = layout(0);

        Harness(long timeoutMs) {
            this(timeoutMs, MoreExecutors.newDirectExecutorService());
        }

        Harness(long timeoutMs, ExecutorService completion) {
            this(timeoutMs, completion, MoreExecutors.newDirectExecutorService());
        }

        Harness(long timeoutMs, ExecutorService completion, ExecutorService dispatch) {
            this.completion = completion;
            this.dispatch = dispatch;
            v4 = mock(PulsarClientImpl.class);
            when(v4.getMemoryLimitController()).thenReturn(memory);
            when(v4.getInternalExecutorService()).thenReturn(dispatch);
            EventLoopGroup eventLoops = mock(EventLoopGroup.class);
            when(eventLoops.iterator()).thenAnswer(__ -> Collections.emptyIterator());
            when(v4.eventLoopGroup()).thenReturn(eventLoops);
            ExecutorProvider external = mock(ExecutorProvider.class);
            when(external.getExecutor()).thenReturn(completion);
            when(v4.externalExecutorProvider()).thenReturn(external);
            Timer timer = mock(Timer.class);
            when(v4.timer()).thenReturn(timer);
            when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
                TestTimeout timeout = new TestTimeout(timer, invocation.getArgument(0),
                        ((TimeUnit) invocation.getArgument(2)).toNanos(invocation.getArgument(1)));
                timeouts.add(timeout);
                return timeout;
            });
            when(v4.createSegmentProducerAsync(any(), any())).thenAnswer(invocation -> {
                ProducerConfigurationData configuration = invocation.getArgument(0);
                return CompletableFuture.completedFuture(segmentProducer(segmentIds.get(configuration.getTopicName())));
            });
            DagWatchClient watch = mock(DagWatchClient.class);
            when(watch.topicName()).thenReturn(TOPIC);
            doAnswer(invocation -> {
                listener.set(invocation.getArgument(0));
                return null;
            }).when(watch).setListener(any());
            recordSegment(current);
            ProducerConfigurationData conf = new ProducerConfigurationData();
            conf.setTopicName(TOPIC.toString());
            conf.setSendTimeoutMs(timeoutMs);
            conf.setBatchingEnabled(true);
            conf.setBatchingMaxPublishDelayMicros(1, TimeUnit.DAYS);
            client = new PulsarClientV5(v4, "test", null);
            producer = new ScalableTopicProducer<>(client, Schema.string(),
                    conf, watch, current);
            client.registerProducer(producer);
        }

        CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send(boolean synchronous) {
            if (!synchronous) {
                return send("key");
            }
            CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> result = new CompletableFuture<>();
            caller.execute(() -> completeSynchronousSend(result));
            return result;
        }

        CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send(String key) {
            return producer.sendInternalAsync(key, "value", null, null, null, null, null, null, null);
        }

        void completeSynchronousSend(CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> result) {
            synchronousCaller.set(Thread.currentThread());
            try {
                result.complete(producer.sendInternal("key", "value", null, null, null, null, null, null, null));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        }

        Attempt nextAttempt() throws InterruptedException {
            Attempt attempt = pendingAttempts.poll(10, TimeUnit.SECONDS);
            assertThat(attempt).as("the next physical dispatch").isNotNull();
            return attempt;
        }

        void applyLayout(int epoch) {
            applyLayout(layout(epoch));
        }

        void applyLayout(ClientSegmentLayout next) {
            recordSegment(next);
            listener.get().onLayoutChange(next, current);
            current = next;
        }

        private void recordSegment(ClientSegmentLayout layout) {
            for (var segment : layout.activeSegments()) {
                segmentIds.put(segment.attachTopicName(), segment.segmentId());
            }
        }

        @SuppressWarnings("unchecked")
        private Producer<String> segmentProducer(long segmentId) {
            Producer<String> segment = mock(Producer.class);
            when(segment.newMessage()).thenAnswer(__ -> {
                TypedMessageBuilderImpl<String> builder = mock(TypedMessageBuilderImpl.class, RETURNS_SELF);
                when(builder.sendAsync()).thenAnswer(ignored -> {
                    Attempt attempt = new Attempt(segmentId, new Acknowledgement());
                    attempts.add(attempt);
                    pendingAttempts.add(attempt);
                    return attempt.result();
                });
                return builder;
            });
            when(segment.flushAsync()).thenAnswer(__ -> {
                if (acknowledgeOnFlush) {
                    attempts.stream().filter(a -> a.segmentId() == segmentId)
                            .forEach(a -> a.result().complete(MessageId.earliest));
                }
                flushed.countDown();
                return CompletableFuture.completedFuture(null);
            });
            when(segment.closeAsync()).thenAnswer(__ -> {
                attempts.stream().filter(a -> a.segmentId() == segmentId && !a.result().isDone())
                        .forEach(a -> a.result().completeExceptionally(new AlreadyClosedException("closed")));
                return CompletableFuture.completedFuture(null);
            });
            return segment;
        }

        @Override
        public void close() throws Exception {
            try {
                producer.closeAsync().get(10, TimeUnit.SECONDS);
            } finally {
                caller.shutdownNow();
                dispatch.shutdownNow();
                completion.shutdownNow();
            }
        }
    }

    /** Queue completions without running them, then model shutdownNow dropping the whole queue. */
    private static class DiscardingExecutor extends AbstractExecutorService {
        private final List<Runnable> pending = new ArrayList<>();
        private boolean shutdown;

        @Override
        public synchronized void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("completion executor shut down");
            }
            pending.add(command);
        }

        synchronized int pendingCount() {
            return pending.size();
        }

        @Override
        public synchronized void shutdown() {
            shutdown = true;
        }

        @Override
        public synchronized List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> dropped = new ArrayList<>(pending);
            pending.clear();
            return dropped;
        }

        @Override
        public synchronized boolean isShutdown() {
            return shutdown;
        }

        @Override
        public synchronized boolean isTerminated() {
            return shutdown;
        }

        @Override
        public synchronized boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }
    }

    /** Accept the task before allowing the submitting timer callback to return. */
    private static final class GatedSubmissionExecutor extends DiscardingExecutor {
        final CountDownLatch accepted = new CountDownLatch(1);
        final CountDownLatch returnFromSubmission = new CountDownLatch(1);

        @Override
        public void execute(Runnable command) {
            super.execute(command);
            accepted.countDown();
            try {
                assertThat(returnFromSubmission.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    /** A supported crypto extension consulting the actual asynchronous public-key adapter. */
    private static final class RemoteKeyCrypto implements MessageCrypto<MessageMetadata, MessageMetadata> {
        private final Consumer<MessageMetadata> onEncrypt;

        RemoteKeyCrypto(Consumer<MessageMetadata> onEncrypt) {
            this.onEncrypt = onEncrypt;
        }

        @Override
        public void addPublicKeyCipher(Set<String> names, CryptoKeyReader reader) {
            // Let the first send load its key, as a newly added/rotated key can do in MessageCryptoBc.
        }

        @Override
        public boolean removeKeyCipher(String name) {
            return true;
        }

        @Override
        public int getMaxOutputSize(int inputLen) {
            return inputLen + 16;
        }

        @Override
        public void encrypt(Set<String> names, CryptoKeyReader reader, Supplier<MessageMetadata> metadata,
                            ByteBuffer payload, ByteBuffer output) throws CryptoException {
            onEncrypt.accept(metadata.get());
            reader.getPublicKey(names.iterator().next(), Map.of());
            throw new CryptoException("The fixture never publishes encrypted data");
        }

        @Override
        public boolean decrypt(Supplier<MessageMetadata> metadata, ByteBuffer payload,
                               ByteBuffer output, CryptoKeyReader reader) {
            return false;
        }
    }

    /** A connecting V4 producer accepts queued sends before it has a broker connection. */
    private static final class ConnectingCryptoProducer extends ProducerImpl<String> {
        ConnectingCryptoProducer(PulsarClientImpl client, ProducerConfigurationData conf) {
            super(client, conf.getTopicName(), conf, new CompletableFuture<>(), 0,
                    org.apache.pulsar.client.api.Schema.STRING, null, Optional.empty());
            // The fixture stops lookup at a pending future. Model the normal Connecting state,
            // which is explicitly accepted by ProducerImpl's real send/encryption path.
            assertThat(changeToConnecting()).isTrue();
        }
    }

    private static ClientSegmentLayout layout(int epoch) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        for (int id = 0; id <= epoch; id++) {
            var segment = dag.addSegment().setSegmentId(id).setHashStart(0).setHashEnd(0xffff)
                    .setState(id == epoch ? SegmentState.ACTIVE : SegmentState.SEALED).setCreatedAtEpoch(id);
            if (id > 0) {
                segment.addParentId(id - 1);
            }
        }
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

    private static final class TestTimeout implements Timeout {
        private final Timer timer;
        private final TimerTask task;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean expired = new AtomicBoolean();
        final long delayNanos;

        TestTimeout(Timer timer, TimerTask task, long delayNanos) {
            this.timer = timer;
            this.task = task;
            this.delayNanos = delayNanos;
        }

        void fire() throws Exception {
            if (!cancelled.get() && expired.compareAndSet(false, true)) {
                task.run(this);
            }
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean isExpired() {
            return expired.get();
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public boolean cancel() {
            return !expired.get() && cancelled.compareAndSet(false, true);
        }
    }
}
