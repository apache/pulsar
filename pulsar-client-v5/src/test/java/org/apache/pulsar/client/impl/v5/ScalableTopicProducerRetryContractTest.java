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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
            encrypted.setMessageCrypto(new RemoteKeyCrypto());
            ProducerImpl<String> physical = new ConnectingCryptoProducer(harness.v4, encrypted);
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
            producer = new ScalableTopicProducer<>(new PulsarClientV5(v4, "test", null), Schema.string(),
                    conf, watch, current);
        }

        CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send(boolean synchronous) {
            if (!synchronous) {
                return producer.sendInternalAsync("key", "value", null, null, null, null, null, null, null);
            }
            CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> result = new CompletableFuture<>();
            caller.execute(() -> completeSynchronousSend(result));
            return result;
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
            ClientSegmentLayout next = layout(epoch);
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
    private static final class DiscardingExecutor extends AbstractExecutorService {
        private final List<Runnable> pending = new ArrayList<>();
        private boolean shutdown;

        @Override
        public synchronized void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("completion executor shut down");
            }
            pending.add(command);
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

    /** A supported crypto extension consulting the actual asynchronous public-key adapter. */
    private static final class RemoteKeyCrypto implements MessageCrypto<MessageMetadata, MessageMetadata> {
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
