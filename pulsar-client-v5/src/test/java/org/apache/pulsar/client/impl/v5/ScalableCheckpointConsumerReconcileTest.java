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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ScalableCheckpointConsumerReconcileTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/reconcile-test");

    /** Reader attach attempts per segment. */
    private final Map<Long, Integer> attempts = new HashMap<>();
    /** Scheduled retries, run by the test. */
    private final ArrayDeque<TimerTask> retries = new ArrayDeque<>();
    private ExecutorService externalExecutor;
    private PulsarClientImpl v4;
    private PulsarClientV5 client;

    @BeforeMethod
    public void setup() {
        attempts.clear();
        retries.clear();
        externalExecutor = MoreExecutors.newDirectExecutorService();
        v4 = mock(PulsarClientImpl.class);
        client = mock(PulsarClientV5.class);
        ExecutorProvider external = mock(ExecutorProvider.class);
        Timer timer = mock(Timer.class);
        when(client.v4Client()).thenReturn(v4);
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(externalExecutor);
        when(v4.timer()).thenReturn(timer);
        when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return null;
        });
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        externalExecutor.shutdown();
    }

    @Test
    public void retriesAReaderThatFailedToAttach() throws Exception {
        // Segment 1's first reader fails to attach with a transient (not segment-gone) error.
        attachReaders((segmentId, attempt) -> segmentId == 1 && attempt == 1
                ? CompletableFuture.failedFuture(new PulsarClientException("transient"))
                : CompletableFuture.completedFuture(reader()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0, false);
        CheckpointConsumer<byte[]> consumer = createUnmanaged(initial, listener);
        try {
            // A split: the reader for child 1 fails to attach, the one for child 2 attaches.
            listener[0].onLayoutChange(layout(1, true), initial);
            assertEquals(attempts.get(1L), 1);
            assertEquals(attempts.get(2L), 1);
            assertEquals(retries.size(), 1, "the failed reader must be retried");

            retries.poll().run(null);
            assertEquals(attempts.get(1L), 2);
            assertEquals(attempts.get(2L), 1, "attached readers are not recreated");
            assertEquals(retries.size(), 0);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void keepsAtMostOneRetryScheduled() throws Exception {
        // Segment 1's reader keeps failing to attach with a transient error.
        attachReaders((segmentId, attempt) -> segmentId == 1
                ? CompletableFuture.failedFuture(new PulsarClientException("transient"))
                : CompletableFuture.completedFuture(reader()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0, false);
        CheckpointConsumer<byte[]> consumer = createUnmanaged(initial, listener);
        try {
            ClientSegmentLayout split = layout(1, true);
            listener[0].onLayoutChange(split, initial);
            assertEquals(retries.size(), 1);

            // The layout is pushed again while the retry waits: each push retries segment 1 right
            // away, which fails again, without scheduling a second retry.
            listener[0].onLayoutChange(split, split);
            listener[0].onLayoutChange(split, split);
            assertEquals(attempts.get(1L), 3);
            assertEquals(retries.size(), 1, "at most one retry must be scheduled");

            retries.poll().run(null);
            assertEquals(attempts.get(1L), 4);
            assertEquals(retries.size(), 1, "the retry must be rescheduled while the reader keeps failing");
        } finally {
            consumer.close();
        }
    }

    @Test
    public void closesARevokedSegmentWhileAnotherReaderAttaches() throws Exception {
        // Segment 1's reader doesn't finish attaching, as when it keeps reconnecting.
        CompletableFuture<Reader<byte[]>> segment1Attach = new CompletableFuture<>();
        AtomicBoolean segment0Closed = new AtomicBoolean();
        attachReaders((segmentId, attempt) -> segmentId == 1 ? segment1Attach
                : CompletableFuture.completedFuture(reader(() -> segment0Closed.set(true))));
        ScalableConsumerClient session = mock(ScalableConsumerClient.class);
        ScalableConsumerClient.AssignmentChangeListener[] listener =
                new ScalableConsumerClient.AssignmentChangeListener[1];
        doAnswer(invocation -> {
            listener[0] = invocation.getArgument(0);
            return null;
        }).when(session).setListener(any());

        CheckpointConsumer<byte[]> consumer = ScalableCheckpointConsumer.createManagedAsync(client,
                Schema.bytes(), TOPIC.toString(), session, List.of(segment(0)), CheckpointV5.EARLIEST,
                "test").join();
        try {
            // The group assigns segment 1 too, then moves segment 0 to another member while
            // segment 1's reader is still attaching.
            listener[0].onAssignmentChange(List.of(segment(0), segment(1)), List.of(segment(0)));
            listener[0].onAssignmentChange(List.of(segment(1)), List.of(segment(0), segment(1)));
            assertTrue(segment0Closed.get(), "the reader of a segment moved away must be closed right away");
        } finally {
            segment1Attach.complete(reader());
            consumer.close();
        }
    }

    /** Answer each reader attach with {@code attach}, given the segment and its attempt number. */
    private void attachReaders(BiFunction<Long, Integer, CompletableFuture<Reader<byte[]>>> attach) {
        when(v4.<byte[]>createSegmentReaderAsync(any(), any())).thenAnswer(invocation -> {
            ReaderConfigurationData<byte[]> conf = invocation.getArgument(0);
            long segmentId = Long.parseLong(conf.getTopicName().substring(
                    conf.getTopicName().lastIndexOf('-') + 1));
            return attach.apply(segmentId, attempts.merge(segmentId, 1, Integer::sum));
        });
    }

    private CheckpointConsumer<byte[]> createUnmanaged(ClientSegmentLayout initial,
                                                       DagWatchClient.LayoutChangeListener[] listener) {
        DagWatchClient watch = mock(DagWatchClient.class);
        when(watch.topicName()).thenReturn(TOPIC);
        doAnswer(invocation -> {
            listener[0] = invocation.getArgument(0);
            return null;
        }).when(watch).setListener(any());
        return ScalableCheckpointConsumer.createUnmanagedAsync(client, Schema.bytes(), watch, initial,
                CheckpointV5.EARLIEST, "test").join();
    }

    /** Segment 0, or after a split, sealed segment 0 with children 1 and 2. */
    private static ClientSegmentLayout layout(long epoch, boolean split) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                .setState(split ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(0);
        if (split) {
            dag.addSegment().setSegmentId(1).setHashStart(0).setHashEnd(0x7fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(epoch).addParentId(0);
            dag.addSegment().setSegmentId(2).setHashStart(0x8000).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(epoch).addParentId(0);
        }
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

    /** A segment assigned to a group member. */
    private static ActiveSegment segment(long segmentId) {
        HashRange range = HashRange.of(0, 0xffff);
        return new ActiveSegment(segmentId, range,
                SegmentTopicName.fromParent(TOPIC, range, segmentId).toString(), null, List.of(), List.of());
    }

    /** A reader with nothing to read: its pending read never completes, and segments stay unsealed. */
    private static Reader<byte[]> reader() {
        return reader(() -> {
        });
    }

    @SuppressWarnings("unchecked")
    private static Reader<byte[]> reader(Runnable onClose) {
        return (Reader<byte[]>) Proxy.newProxyInstance(Reader.class.getClassLoader(),
                new Class<?>[]{Reader.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "readNextAsync" -> new CompletableFuture<>();
                    case "hasMessageAvailableAsync" -> CompletableFuture.completedFuture(true);
                    case "closeAsync" -> {
                        onClose.run();
                        yield CompletableFuture.completedFuture(null);
                    }
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "reader";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
