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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.EncodeData;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The per-key ordering of {@link ScalableTopicProducer}'s async sends across layout changes, driven
 * step by step: the layouts are applied by the test, the segment producers are fakes whose sends
 * the test acknowledges or fails, and the dispatch executor runs its tasks on the test thread.
 *
 * <p>The layouts: segment 0 covers every key; it splits into segment 1 (the low half of the hash
 * space) and segment 2 (the high half); segment 1 then splits into segments 3 and 4.
 */
public class ScalableTopicProducerOrderingTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/ordering-test");

    private static final ClientSegmentLayout L0 = layout(0);
    private static final ClientSegmentLayout L1 = layout(1);
    private static final ClientSegmentLayout L2 = layout(2);

    /** A key of segment 3, and so of segments 1 and 0. */
    private static final String KEY_A = keyIn(0x0000, 0x3fff);
    /** A key of segment 2, and of segment 0. */
    private static final String KEY_B = keyIn(0x8000, 0xffff);

    /** A message handed to a segment producer, with the future its result is delivered on. */
    private record Sent(long segmentId, String key, String value, CompletableFuture<MessageId> result) {
    }

    private final List<Sent> sent = Collections.synchronizedList(new ArrayList<>());
    private final Set<Long> closedSegments = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> segmentIds = new HashMap<>();
    private TestExecutor dispatchExecutor;
    private ExecutorService completionExecutor;
    private ExecutorProvider external;
    /** What the segment producers' {@code closeAsync} returns. */
    private volatile CompletableFuture<Void> segmentCloseResult;
    private MemoryLimitController memory;
    private PulsarClientImpl v4;
    private PulsarClientV5 client;
    private DagWatchClient.LayoutChangeListener listener;
    private ScalableTopicProducer<String> producer;

    @BeforeMethod
    public void setup() {
        sent.clear();
        closedSegments.clear();
        segmentCloseResult = CompletableFuture.completedFuture(null);
        for (var layout : List.of(L0, L1, L2)) {
            for (var segment : layout.activeSegments()) {
                segmentIds.put(segment.attachTopicName(), segment.segmentId());
            }
        }
        dispatchExecutor = new TestExecutor();
        completionExecutor = MoreExecutors.newDirectExecutorService();
        memory = new MemoryLimitController(64 * 1024 * 1024);
        v4 = mock(PulsarClientImpl.class);
        external = mock(ExecutorProvider.class);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(v4.getMemoryLimitController()).thenReturn(memory);
        when(v4.getInternalExecutorService()).thenReturn(dispatchExecutor);
        when(v4.eventLoopGroup()).thenReturn(eventLoopGroup);
        when(eventLoopGroup.iterator()).thenAnswer(__ -> Collections.emptyIterator());
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(completionExecutor);
        when(v4.createSegmentProducerAsync(any(), any())).thenAnswer(invocation -> {
            ProducerConfigurationData conf = invocation.getArgument(0);
            return CompletableFuture.completedFuture(segmentProducer(segmentIds.get(conf.getTopicName())));
        });
        when(v4.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        client = new PulsarClientV5(v4, "test", null);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (producer != null) {
            producer.closeAsync();
            dispatchExecutor.paused = false;
            dispatchExecutor.runAll();
        }
        completionExecutor.shutdown();
    }

    @Test
    public void resendsTheSendsOfASealedSegmentBeforeTheNewerOnes() {
        create(L0);
        var a1 = send(KEY_A, "a1");
        var b1 = send(KEY_B, "b1");
        assertThat(values(0)).containsExactly("a1", "b1");

        // The split: segment 0 retires, and the new sends of its keys, every key, are held.
        applyLayout(L1, L0);
        var a2 = send(KEY_A, "a2");
        var b2 = send(KEY_B, "b2");
        assertThat(producer.waitingSendCount()).isEqualTo(2);
        assertThat(sent).hasSize(2);

        // Segment 0 is sealed: its sends fail, and are re-sent before the held ones.
        complete(0, new TopicTerminatedException("sealed"));
        assertThat(values(1)).containsExactly("a1", "a2");
        assertThat(values(2)).containsExactly("b1", "b2");
        assertThat(producer.sendsParkedOrHeld.get()).isEqualTo(4);

        complete(1, null);
        complete(2, null);
        assertThat(segmentOf(a1)).isEqualTo(1);
        assertThat(segmentOf(a2)).isEqualTo(1);
        assertThat(segmentOf(b1)).isEqualTo(2);
        assertThat(segmentOf(b2)).isEqualTo(2);
        assertThat(producer.waitingSendCount()).isZero();
        assertThat(memory.currentUsage()).isZero();
    }

    @Test
    public void holdsOnlyTheSendsOfTheKeysOfTheRetiringSegment() {
        create(L1);
        var a1 = send(KEY_A, "a1");
        var b1 = send(KEY_B, "b1");

        // Segment 1 is terminated underneath the layout: a1 waits for the next layout, and so does
        // a2 behind it.
        complete(1, new TopicTerminatedException("terminated"));
        var a2 = send(KEY_A, "a2");
        assertThat(producer.waitingSendCount()).isEqualTo(2);

        // The sends of segment 2's keys, and those without a key, go out meanwhile.
        var b2 = send(KEY_B, "b2");
        assertThat(values(2)).containsExactly("b1", "b2");
        var keyless = send(null, "k");
        assertThat(sent).anyMatch(s -> s.value().equals("k"));
        assertThat(producer.waitingSendCount()).isEqualTo(2);
        complete(2, null);
        assertThat(b1).isCompleted();
        assertThat(b2).isCompleted();
        // The keyless send, if it went to segment 1, waits for the layout too.
        complete(1, new TopicTerminatedException("terminated"));

        // Segment 1 splits: its keys' sends go to its children, in order.
        applyLayout(L2, L1);
        assertThat(valuesOfKey(3, KEY_A)).containsExactly("a1", "a2");
        completeAll();
        assertThat(segmentOf(a1)).isEqualTo(3);
        assertThat(segmentOf(a2)).isEqualTo(3);
        assertThat(keyless).isCompleted();
        assertThat(memory.currentUsage()).isZero();
    }

    @Test
    public void aLayoutAppliedWhileReleasingStopsTheRestOfTheBatch() {
        create(L0);
        var a1 = send(KEY_A, "a1");
        var a2 = send(KEY_A, "a2");
        applyLayout(L1, L0);
        // Sends without a key are not held: they start the dispatch chains of segments 1 and 2, so
        // that the re-sent a1 is handed to segment 1's producer while the batch is released.
        send(null, "k1");
        send(null, "k2");
        complete(1, null);
        complete(2, null);
        var a3 = send(KEY_A, "a3");

        // Segment 1 splits right after a1 is re-sent to it, before a2 is: a2 must wait for a1 to be
        // re-sent again, rather than go to segment 3 ahead of it.
        AtomicBoolean split = new AtomicBoolean();
        producer.afterV4EnqueueHook = __ -> {
            Sent last = sent.get(sent.size() - 1);
            if (last.segmentId() == 1 && last.value().equals("a1") && split.compareAndSet(false, true)) {
                listener.onLayoutChange(L2, L1);
            }
        };
        complete(0, new TopicTerminatedException("sealed"));
        assertThat(split).isTrue();
        assertThat(valuesOfKey(1, KEY_A)).containsExactly("a1");
        assertThat(values(3)).isEmpty();

        complete(1, new TopicTerminatedException("sealed"));
        assertThat(values(3)).containsExactly("a1", "a2", "a3");
        completeAll();
        assertThat(segmentOf(a1)).isEqualTo(3);
        assertThat(segmentOf(a2)).isEqualTo(3);
        assertThat(segmentOf(a3)).isEqualTo(3);
        assertThat(memory.currentUsage()).isZero();
    }

    @Test
    public void closingTheClientFailsTheWaitingSendsWhenTheirReleaseIsDropped() throws Exception {
        create(L0);
        client.registerProducer(producer);
        // Shutting the v4 client down drops the dispatch executor's queued tasks.
        doAnswer(__ -> {
            dispatchExecutor.shutdownNow();
            return null;
        }).when(v4).close();

        var a1 = send(KEY_A, "a1");
        applyLayout(L1, L0);
        var a2 = send(KEY_A, "a2");
        // a1 fails on the sealed segment while the dispatch executor is behind: the release of a1
        // and a2 is queued when the client closes.
        dispatchExecutor.paused = true;
        complete(0, new TopicTerminatedException("sealed"));
        assertThat(dispatchExecutor.tasks).isNotEmpty();

        client.close();
        Class<?> alreadyClosed = org.apache.pulsar.client.api.v5.PulsarClientException.AlreadyClosedException.class;
        assertThat(a1).failsWithin(0, TimeUnit.SECONDS).withThrowableThat()
                .havingCause().isInstanceOf(alreadyClosed);
        assertThat(a2).failsWithin(0, TimeUnit.SECONDS).withThrowableThat()
                .havingCause().isInstanceOf(alreadyClosed);
        assertThat(memory.currentUsage()).isZero();
    }

    @Test
    public void closingTheClientFailsTheSendsBehindAQueuedChainHop() {
        create(L0);
        client.registerProducer(producer);
        // The send's dispatch chain starts with a hop to the dispatch executor, which is behind.
        dispatchExecutor.paused = true;
        var a1 = send(KEY_A, "a1");
        assertThat(sent).isEmpty();

        // Closing the v4 client would drop the hop: closing the producer fails the send without it.
        var closed = client.closeAsync();
        assertThat(closed).isCompleted();
        verify(v4).closeAsync();
        assertClosedFailure(a1);
        assertThat(memory.currentUsage()).isZero();

        dispatchExecutor.paused = false;
        dispatchExecutor.runAll();
        assertThat(sent).isEmpty();
    }

    @Test
    public void closingTheClientCompletesTheWaitingSendsBeforeItsExecutorsShutDown() {
        // The completion executor is behind, and closing the v4 client drops its queued tasks.
        var completions = new TestExecutor();
        completions.paused = true;
        completionExecutor = completions;
        when(external.getExecutor()).thenReturn(completionExecutor);
        when(v4.closeAsync()).thenAnswer(__ -> {
            completions.shutdownNow();
            dispatchExecutor.shutdownNow();
            return CompletableFuture.completedFuture(null);
        });
        create(L0);
        client.registerProducer(producer);

        send(KEY_A, "a1");
        applyLayout(L1, L0);
        // Held behind a1, which is in flight on the retiring segment 0.
        var a2 = send(KEY_A, "a2");
        assertThat(producer.waitingSendCount()).isEqualTo(1);

        assertThat(client.closeAsync()).isCompleted();
        assertClosedFailure(a2);
    }

    @Test
    public void closingTheClientWaitsForAProducerThatIsAlreadyClosing() {
        create(L0);
        client.registerProducer(producer);
        var a1 = send(KEY_A, "a1");
        assertThat(values(0)).containsExactly("a1");

        // The segment producer takes its time to close.
        var segmentClosed = new CompletableFuture<Void>();
        segmentCloseResult = segmentClosed;
        var producerClosed = producer.closeAsync();
        assertThat(producerClosed).isNotDone();

        var clientClosed = client.closeAsync();
        assertThat(clientClosed).isNotDone();
        verify(v4, never()).closeAsync();

        complete(0, new AlreadyClosedException("closed"));
        segmentClosed.complete(null);
        assertThat(producerClosed).isCompleted();
        assertThat(clientClosed).isCompleted();
        verify(v4).closeAsync();
        assertThat(a1).isCompletedExceptionally();
        assertThat(memory.currentUsage()).isZero();
    }

    @Test
    public void closingTheClientDoesNotWaitForASegmentProducerThatIsNotCreatedYet() {
        // The segment producer waits for exclusive access, which another client holds.
        var creation = new CompletableFuture<Producer<String>>();
        doAnswer(__ -> creation).when(v4).createSegmentProducerAsync(any(), any());
        create(L0);
        client.registerProducer(producer);
        var a1 = send(KEY_A, "a1");
        assertThat(a1).isNotDone();

        assertThat(client.closeAsync()).isCompleted();
        verify(v4).closeAsync();
        assertClosedFailure(a1);
        assertThat(memory.currentUsage()).isZero();

        // Created after all: it is closed, and nothing is sent to it.
        creation.complete(segmentProducer(0));
        dispatchExecutor.runAll();
        assertThat(closedSegments).contains(0L);
        assertThat(sent).isEmpty();
    }

    private static void assertClosedFailure(CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send) {
        Class<?> alreadyClosed = org.apache.pulsar.client.api.v5.PulsarClientException.AlreadyClosedException.class;
        assertThat(send).failsWithin(0, TimeUnit.SECONDS).withThrowableThat()
                .havingCause().isInstanceOf(alreadyClosed);
    }

    private void create(ClientSegmentLayout initial) {
        DagWatchClient watch = mock(DagWatchClient.class);
        when(watch.topicName()).thenReturn(TOPIC);
        doAnswer(invocation -> {
            listener = invocation.getArgument(0);
            return null;
        }).when(watch).setListener(any());
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setTopicName(TOPIC.toString());
        dispatchExecutor.testThread = Thread.currentThread();
        producer = new ScalableTopicProducer<>(client, Schema.string(), conf, watch, initial);
    }

    private CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send(String key, String value) {
        var future = producer.sendInternalAsync(key, value, null, null, null, null, null, null, null);
        dispatchExecutor.runAll();
        return future;
    }

    private void applyLayout(ClientSegmentLayout layout, ClientSegmentLayout previous) {
        listener.onLayoutChange(layout, previous);
        dispatchExecutor.runAll();
    }

    /** Deliver the result of the sends pending on a segment: acknowledge them, or fail them. */
    private void complete(long segmentId, Throwable failure) {
        complete(s -> s.segmentId() == segmentId, failure);
    }

    private void completeAll() {
        complete(__ -> true, null);
    }

    private void complete(Predicate<Sent> which, Throwable failure) {
        List<Sent> pending;
        synchronized (sent) {
            pending = sent.stream().filter(s -> !s.result().isDone() && which.test(s)).toList();
        }
        for (Sent s : pending) {
            if (failure == null) {
                s.result().complete(MessageId.earliest);
            } else {
                s.result().completeExceptionally(failure);
            }
        }
        dispatchExecutor.runAll();
    }

    /** The values handed to a segment's producer, in the order it was given them. */
    private List<String> values(long segmentId) {
        return valuesOfKey(segmentId, null);
    }

    private List<String> valuesOfKey(long segmentId, String key) {
        synchronized (sent) {
            return sent.stream()
                    .filter(s -> s.segmentId() == segmentId && (key == null || key.equals(s.key())))
                    .map(Sent::value)
                    .toList();
        }
    }

    private static long segmentOf(CompletableFuture<org.apache.pulsar.client.api.v5.MessageId> send) {
        return ((MessageIdV5) send.join()).segmentId();
    }

    /** A segment producer that records the messages it is given, for the test to complete. */
    @SuppressWarnings("unchecked")
    private Producer<String> segmentProducer(long segmentId) {
        Producer<String> producer = mock(Producer.class);
        when(producer.newMessage()).thenAnswer(__ -> {
            TypedMessageBuilderImpl<String> builder = mock(TypedMessageBuilderImpl.class, RETURNS_SELF);
            String[] key = new String[1];
            String[] value = new String[1];
            when(builder.key(anyString())).thenAnswer(invocation -> {
                key[0] = invocation.getArgument(0);
                return builder;
            });
            when(builder.encodedValue(any())).thenAnswer(invocation -> {
                value[0] = new String(((EncodeData) invocation.getArgument(0)).data(), StandardCharsets.UTF_8);
                return builder;
            });
            when(builder.sendAsync()).thenAnswer(invocation -> {
                if (closedSegments.contains(segmentId)) {
                    return CompletableFuture.failedFuture(new AlreadyClosedException("closed"));
                }
                CompletableFuture<MessageId> result = new CompletableFuture<>();
                sent.add(new Sent(segmentId, key[0], value[0], result));
                return result;
            });
            return builder;
        });
        when(producer.closeAsync()).thenAnswer(__ -> {
            closedSegments.add(segmentId);
            return segmentCloseResult;
        });
        return producer;
    }

    /** A key whose segment-routing hash is in {@code [start, end]}. */
    private static String keyIn(int start, int end) {
        for (int i = 0; ; i++) {
            String key = "key-" + i;
            int hash = SegmentRouter.hash(key);
            if (hash >= start && hash <= end) {
                return key;
            }
        }
    }

    private static ClientSegmentLayout layout(long epoch) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                .setState(epoch >= 1 ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(0);
        if (epoch >= 1) {
            dag.addSegment().setSegmentId(1).setHashStart(0).setHashEnd(0x7fff)
                    .setState(epoch >= 2 ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(1)
                    .addParentId(0);
            dag.addSegment().setSegmentId(2).setHashStart(0x8000).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(1).addParentId(0);
        }
        if (epoch >= 2) {
            dag.addSegment().setSegmentId(3).setHashStart(0).setHashEnd(0x3fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(2).addParentId(1);
            dag.addSegment().setSegmentId(4).setHashStart(0x4000).setHashEnd(0x7fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(2).addParentId(1);
        }
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

    /**
     * The dispatch executor: runs its tasks in order on the test thread, one at a time, as the
     * client's single dispatch thread would. A task submitted on the test thread runs right away,
     * unless another is running, or the executor is paused, as one that is behind; then it waits for
     * the next {@link #runAll}. Tasks from other threads, such as the retirement check's timer, wait
     * for it too.
     */
    private static final class TestExecutor extends AbstractExecutorService {
        /** The thread running the test: the one that created the producer. */
        volatile Thread testThread;
        final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private boolean running;
        volatile boolean paused;
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable task) {
            if (shutdown) {
                throw new RejectedExecutionException("shut down");
            }
            tasks.add(task);
            runAll();
        }

        void runAll() {
            if (running || paused || Thread.currentThread() != testThread) {
                return;
            }
            running = true;
            try {
                Runnable task;
                while ((task = tasks.poll()) != null) {
                    task.run();
                }
            } finally {
                running = false;
            }
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> dropped = new ArrayList<>(tasks);
            tasks.clear();
            return dropped;
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }
}
