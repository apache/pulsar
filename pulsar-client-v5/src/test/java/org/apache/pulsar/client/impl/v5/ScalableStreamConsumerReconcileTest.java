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
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timer;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ScalableStreamConsumerReconcileTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/reconcile-test");

    /** Segment subscribe attempts per segment. */
    private final Map<Long, Integer> attempts = new HashMap<>();
    /** Scheduled tasks (reconcile retries and release drain polls), run by the test. */
    private final ArrayDeque<Runnable> scheduled = new ArrayDeque<>();
    /** Messages handed to the application. */
    private final List<MessageV5<byte[]>> delivered = new ArrayList<>();
    private ExecutorService externalExecutor;
    private PulsarClientImpl v4;
    private PulsarClientV5 client;
    private ScalableConsumerClient session;
    private ScalableConsumerClient.AssignmentChangeListener listener;

    @BeforeMethod
    public void setup() {
        attempts.clear();
        scheduled.clear();
        delivered.clear();
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
        when(v4.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(v4.getScheduledExecutorProvider()).thenReturn(scheduledProvider);
        when(scheduledProvider.getExecutor()).thenReturn(scheduler);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(invocation -> {
            scheduled.add(invocation.getArgument(0));
            return null;
        });
        when(scheduler.schedule(any(Callable.class), anyLong(), any())).thenAnswer(invocation -> {
            Callable<?> task = invocation.getArgument(0);
            scheduled.add(() -> {
                try {
                    task.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        });
        session = mock(ScalableConsumerClient.class);
        doAnswer(invocation -> {
            listener = invocation.getArgument(0);
            return null;
        }).when(session).setListener(any());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        externalExecutor.shutdown();
    }

    @Test
    public void keepsAtMostOneRetryScheduled() throws Exception {
        // Segment 1's subscribe keeps being rejected, as while its previous owner still holds it.
        subscribeSegments((segmentId, attempt) -> segmentId == 1
                ? CompletableFuture.failedFuture(new ConsumerBusyException("busy"))
                : CompletableFuture.completedFuture(consumer(new CompletableFuture<>())));
        ScalableStreamConsumer<byte[]> consumer = create(List.of(segment(0)));
        try {
            listener.onAssignmentChange(List.of(segment(0), segment(1)), List.of(segment(0)));
            assertEquals(attempts.get(1L), 1);
            assertEquals(scheduled.size(), 1, "the rejected subscribe must be retried");

            // The assignment is pushed again while the retry waits: each push retries segment 1
            // right away, which fails again, without scheduling a second retry.
            listener.onAssignmentChange(List.of(segment(0), segment(1)), List.of(segment(0), segment(1)));
            listener.onAssignmentChange(List.of(segment(0), segment(1)), List.of(segment(0), segment(1)));
            assertEquals(attempts.get(1L), 3);
            assertEquals(scheduled.size(), 1, "at most one retry must be scheduled");

            scheduled.poll().run();
            assertEquals(attempts.get(1L), 4);
            assertEquals(scheduled.size(), 1, "the retry must be rescheduled while the subscribe keeps failing");
        } finally {
            consumer.close();
        }
    }

    @Test
    public void resubscribesASegmentOnlyAfterItsReleaseDrained() throws Exception {
        // Segment 0's first consumer hands the application a message; its next one, another.
        CompletableFuture<Message<byte[]>> firstReceive = new CompletableFuture<>();
        CompletableFuture<Message<byte[]>> secondReceive = new CompletableFuture<>();
        subscribeSegments((segmentId, attempt) -> CompletableFuture.completedFuture(consumer(
                segmentId != 0 ? new CompletableFuture<>() : attempt == 1 ? firstReceive : secondReceive)));
        ScalableStreamConsumer<byte[]> consumer = create(List.of(segment(0)));
        try {
            firstReceive.complete(message());
            assertEquals(delivered.size(), 1);

            // Segment 0 moves to another member and back while the application holds its message:
            // the assignment that gives it back is applied once the release has drained.
            listener.onAssignmentChange(List.of(segment(1)), List.of(segment(0)));
            listener.onAssignmentChange(List.of(segment(0), segment(1)), List.of(segment(1)));
            assertEquals(attempts.get(0L), 1, "a segment must not be subscribed again while it drains");

            consumer.acknowledgeCumulative(delivered.get(0).id());
            runScheduled();
            assertEquals(attempts.get(0L), 2, "the segment must be subscribed again once its release drained");
            secondReceive.complete(message());
            assertEquals(delivered.size(), 2, "the new consumer of the segment must deliver");
        } finally {
            consumer.close();
        }
    }

    /** Run the scheduled tasks, including those they schedule, until none is left. */
    private void runScheduled() {
        for (int i = 0; i < 100 && !scheduled.isEmpty(); i++) {
            scheduled.poll().run();
        }
        assertEquals(scheduled.size(), 0);
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

    private ScalableStreamConsumer<byte[]> create(List<ActiveSegment> initialAssignment) {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("test");
        return ScalableStreamConsumer.createAsyncImpl(client, Schema.bytes(), conf, session, TOPIC.toString(),
                initialAssignment, msg -> {
                    delivered.add(msg);
                    return CompletableFuture.completedFuture(null);
                }).join();
    }

    /** A segment this consumer owns whole. */
    private static ActiveSegment segment(long segmentId) {
        HashRange range = HashRange.of(0, 0xffff);
        return new ActiveSegment(segmentId, range,
                SegmentTopicName.fromParent(TOPIC, range, segmentId).toString(), null, List.of(), List.of());
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> message() {
        Message<byte[]> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(MessageId.earliest);
        return message;
    }

    /** A segment consumer whose first receive is {@code firstReceive}; later receives never complete. */
    @SuppressWarnings("unchecked")
    private static Consumer<byte[]> consumer(CompletableFuture<Message<byte[]>> firstReceive) {
        List<CompletableFuture<Message<byte[]>>> receives = new ArrayList<>();
        return (Consumer<byte[]>) Proxy.newProxyInstance(Consumer.class.getClassLoader(),
                new Class<?>[]{Consumer.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "receiveAsync" -> {
                        CompletableFuture<Message<byte[]>> receive =
                                receives.isEmpty() ? firstReceive : new CompletableFuture<>();
                        receives.add(receive);
                        yield receive;
                    }
                    case "acknowledgeCumulativeAsync" -> CompletableFuture.completedFuture(null);
                    case "closeAsync" -> {
                        receives.forEach(receive -> receive.completeExceptionally(
                                new AlreadyClosedException("closed")));
                        yield CompletableFuture.completedFuture(null);
                    }
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "consumer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
