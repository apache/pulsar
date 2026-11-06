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
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ScalableQueueConsumerReconcileTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/reconcile-test");

    /** Segment subscribe attempts per segment. */
    private final Map<Long, Integer> attempts = new HashMap<>();
    /** Scheduled retries, run by the test. */
    private final ArrayDeque<Runnable> retries = new ArrayDeque<>();
    /** The delay of each retry scheduled, in milliseconds. */
    private final List<Long> retryDelays = new ArrayList<>();
    private ExecutorService externalExecutor;
    private PulsarClientImpl v4;
    private PulsarClientV5 client;

    @BeforeMethod
    public void setup() {
        attempts.clear();
        retries.clear();
        retryDelays.clear();
        externalExecutor = MoreExecutors.newDirectExecutorService();
        v4 = mock(PulsarClientImpl.class);
        client = mock(PulsarClientV5.class);
        ExecutorProvider external = mock(ExecutorProvider.class);
        ScheduledExecutorProvider scheduledProvider = mock(ScheduledExecutorProvider.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(client.v4Client()).thenReturn(v4);
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(externalExecutor);
        when(v4.timer()).thenReturn(mock(Timer.class));
        when(v4.getScheduledExecutorProvider()).thenReturn(scheduledProvider);
        when(scheduledProvider.getExecutor()).thenReturn(scheduler);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            retryDelays.add(invocation.getArgument(1));
            return null;
        });
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        externalExecutor.shutdown();
    }

    @Test
    public void keepsAtMostOneRetryScheduled() throws Exception {
        // Segment 1's subscribe keeps failing with a transient error.
        subscribeSegments((segmentId, attempt) -> segmentId == 1
                ? CompletableFuture.failedFuture(new PulsarClientException("transient"))
                : CompletableFuture.completedFuture(consumer()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0);
        ScalableQueueConsumer<byte[]> consumer = create(initial, listener);
        try {
            ClientSegmentLayout split = layout(1);
            listener[0].onLayoutChange(split, initial);
            assertEquals(attempts.get(1L), 1);
            assertEquals(retries.size(), 1, "the failed subscribe must be retried");

            // The layout is pushed again while the retry waits: each push retries segment 1 right
            // away, which fails again, without scheduling a second retry.
            listener[0].onLayoutChange(split, split);
            listener[0].onLayoutChange(split, split);
            assertEquals(attempts.get(1L), 3);
            assertEquals(retries.size(), 1, "at most one retry must be scheduled");

            retries.poll().run();
            assertEquals(attempts.get(1L), 4);
            assertEquals(retries.size(), 1, "the retry must be rescheduled while the subscribe keeps failing");
            assertEquals(attempts.get(0L), 1, "subscribed segments are not recreated");
            assertEquals(attempts.get(2L), 1, "subscribed segments are not recreated");
        } finally {
            consumer.close();
        }
    }

    @Test
    public void retriesANewFailureWithoutWaitingOutAnEarlierBackoff() throws Exception {
        // Segment 1's subscribe fails three times, then succeeds; segment 3's always fails.
        subscribeSegments((segmentId, attempt) -> segmentId == 3 || (segmentId == 1 && attempt <= 3)
                ? CompletableFuture.failedFuture(new PulsarClientException("transient"))
                : CompletableFuture.completedFuture(consumer()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0);
        ScalableQueueConsumer<byte[]> consumer = create(initial, listener);
        try {
            // Each failed retry doubles the backoff.
            ClientSegmentLayout split = layout(1);
            listener[0].onLayoutChange(split, initial);
            retries.poll().run();
            retries.poll().run();
            assertEquals(attempts.get(1L), 3);
            assertTrue(retryDelays.get(2) >= 300, "the backoff must have grown: " + retryDelays);

            // The layout is pushed again while that retry waits, and segment 1 subscribes. Then segment 2
            // splits and segment 3 fails to subscribe: it's retried after the initial delay, not after the
            // longer one of the retry still pending.
            listener[0].onLayoutChange(split, split);
            assertEquals(attempts.get(1L), 4);
            listener[0].onLayoutChange(layout(2), split);
            assertEquals(retries.size(), 2);
            assertTrue(retryDelays.get(3) < 200, "a new failure must be retried soon: " + retryDelays);

            retries.poll().run();
            assertEquals(attempts.get(3L), 1, "a replaced retry must do nothing");
            retries.poll().run();
            assertEquals(attempts.get(3L), 2);
            assertEquals(retries.size(), 1);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void subscribesNewSegmentsWhileAnotherSubscribeIsPending() throws Exception {
        // Segment 1's subscribe doesn't complete, as while it keeps reconnecting.
        CompletableFuture<Consumer<byte[]>> segment1Subscribe = new CompletableFuture<>();
        subscribeSegments((segmentId, attempt) -> segmentId == 1 ? segment1Subscribe
                : CompletableFuture.completedFuture(consumer()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0);
        ScalableQueueConsumer<byte[]> consumer = create(initial, listener);
        try {
            // Segment 0 splits into 1 and 2, then segment 2 splits into 3 and 4 while segment 1's
            // subscribe is still pending.
            ClientSegmentLayout split = layout(1);
            listener[0].onLayoutChange(split, initial);
            listener[0].onLayoutChange(layout(2), split);
            assertEquals(attempts.get(3L), 1, "a new segment must be subscribed right away");
            assertEquals(attempts.get(4L), 1, "a new segment must be subscribed right away");
            assertEquals(attempts.get(1L), 1, "an in-flight subscribe must be kept, not duplicated");
        } finally {
            segment1Subscribe.complete(consumer());
            consumer.close();
        }
    }

    @Test
    public void closesASegmentThatLeftTheLayoutOnceItsSubscribeCompletes() throws Exception {
        // Segment 2's subscribe doesn't complete until the test completes it.
        CompletableFuture<Consumer<byte[]>> segment2Subscribe = new CompletableFuture<>();
        subscribeSegments((segmentId, attempt) -> segmentId == 2 ? segment2Subscribe
                : CompletableFuture.completedFuture(consumer()));
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        ClientSegmentLayout initial = layout(0);
        ScalableQueueConsumer<byte[]> consumer = create(initial, listener);
        try {
            // Segment 0 splits into 1 and 2, and segment 2 leaves the layout while its subscribe is pending.
            ClientSegmentLayout split = layout(1);
            listener[0].onLayoutChange(split, initial);
            listener[0].onLayoutChange(layout(3), split);
            AtomicBoolean segment2Closed = new AtomicBoolean();
            segment2Subscribe.complete(consumer(() -> segment2Closed.set(true)));
            assertTrue(segment2Closed.get(), "a segment that left the layout must be closed once subscribed");
            assertEquals(attempts.get(2L), 1);
        } finally {
            consumer.close();
        }
    }

    /** Answer each segment subscribe with {@code subscribe}, given the segment and its attempt number. */
    private void subscribeSegments(BiFunction<Long, Integer, CompletableFuture<Consumer<byte[]>>> subscribe) {
        when(v4.<byte[]>subscribeSegmentAsync(any(), any())).thenAnswer(invocation -> {
            ConsumerConfigurationData<byte[]> conf = invocation.getArgument(0);
            String topic = conf.getTopicNames().iterator().next();
            long segmentId = Long.parseLong(topic.substring(topic.lastIndexOf('-') + 1));
            return subscribe.apply(segmentId, attempts.merge(segmentId, 1, Integer::sum));
        });
    }

    private ScalableQueueConsumer<byte[]> create(ClientSegmentLayout initial,
                                                 DagWatchClient.LayoutChangeListener[] listener) {
        DagWatchClient watch = mock(DagWatchClient.class);
        when(watch.topicName()).thenReturn(TOPIC);
        doAnswer(invocation -> {
            listener[0] = invocation.getArgument(0);
            return null;
        }).when(watch).setListener(any());
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("test");
        return ScalableQueueConsumer.createAsyncImpl(client, Schema.bytes(), conf, watch, initial, null, null)
                .join();
    }

    /**
     * Segment 0; after the first split, sealed segment 0 with children 1 and 2; after the second,
     * segment 2 sealed too, with children 3 and 4; once the sealed segments are garbage collected,
     * only 1, 3 and 4.
     */
    private static ClientSegmentLayout layout(long epoch) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        if (epoch < 3) {
            dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                    .setState(epoch >= 1 ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(0);
        }
        if (epoch >= 1) {
            dag.addSegment().setSegmentId(1).setHashStart(0).setHashEnd(0x7fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(1).addParentId(0);
        }
        if (epoch >= 1 && epoch < 3) {
            dag.addSegment().setSegmentId(2).setHashStart(0x8000).setHashEnd(0xffff)
                    .setState(epoch >= 2 ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(1)
                    .addParentId(0);
        }
        if (epoch >= 2) {
            dag.addSegment().setSegmentId(3).setHashStart(0x8000).setHashEnd(0xbfff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(2).addParentId(2);
            dag.addSegment().setSegmentId(4).setHashStart(0xc000).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(2).addParentId(2);
        }
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

    /** A segment consumer with nothing to receive: its pending receive never completes. */
    private static Consumer<byte[]> consumer() {
        return consumer(() -> {
        });
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[]> consumer(Runnable onClose) {
        CompletableFuture<Object> receive = new CompletableFuture<>();
        return (Consumer<byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[]{Consumer.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "receiveAsync" -> receive;
                    case "closeAsync" -> {
                        onClose.run();
                        receive.completeExceptionally(new AlreadyClosedException("closed"));
                        yield CompletableFuture.completedFuture(null);
                    }
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "consumer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
