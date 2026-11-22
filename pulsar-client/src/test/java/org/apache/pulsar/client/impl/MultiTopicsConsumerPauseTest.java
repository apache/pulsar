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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.impl.ClientTestFixtures.createPulsarClientMock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MultiTopicsConsumerPauseTest {
    private static final String TOPIC = "persistent://public/default/multi-topic-pause";
    private static final int TIMEOUT_SECONDS = 10;

    @DataProvider(name = "childCreationAndParentState")
    public Object[][] childCreationAndParentState() {
        return new Object[][] {
                {false, ChildCreation.NON_PARTITIONED},
                {true, ChildCreation.NON_PARTITIONED},
                {false, ChildCreation.PARTITIONED},
                {true, ChildCreation.PARTITIONED},
                {false, ChildCreation.PARTITION_EXPANSION},
                {true, ChildCreation.PARTITION_EXPANSION}
        };
    }

    @DataProvider(name = "pauseDuringRegistration")
    public Object[][] pauseDuringRegistration() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider(name = "initialPauseState")
    public Object[][] initialPauseState() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "initialPauseState", timeOut = 30000)
    public void testConfiguredTopicSubscribesSynchronously(boolean initiallyPaused) throws Exception {
        Wire wire = new Wire();
        wire.subscribeReply.complete(null);
        try (Fixture fixture = new Fixture(initiallyPaused, wire)) {
            assertThat(fixture.parent.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(fixture.parent.subscribeFuture().isDone()).isTrue();
            assertThat(fixture.parent.subscribeFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isSameAs(fixture.parent);
            assertThat(fixture.parent.getConsumers()).hasSize(1);
            ConsumerImpl<byte[]> child = fixture.parent.consumers.get(TOPIC);
            assertThat(child.paused).isEqualTo(initiallyPaused);

            wire.drain();
            int queueSize = child.getCurrentReceiverQueueSize();
            assertThat(wire.sentPermits.get()).as("configured topic receives its initial permits exactly once")
                    .isEqualTo(initiallyPaused ? 0 : queueSize);
            fixture.parent.resume();
            wire.drain();
            assertThat(child.paused).isFalse();
            assertThat(wire.sentPermits.get()).as("resume preserves the initial permit budget")
                    .isEqualTo(queueSize);
            assertThat(child.getAvailablePermits()).isZero();
        }
    }

    @Test(dataProvider = "pauseDuringRegistration", timeOut = 30000)
    public void testChildStartsPausedBeforePublication(boolean pauseDuringRegistration) throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            Wire wire = new Wire();
            AtomicInteger registrations = new AtomicInteger();
            AtomicBoolean pausedBeforePublication = new AtomicBoolean();
            AtomicBoolean absentBeforePublication = new AtomicBoolean();
            doAnswer(invocation -> {
                ConsumerImpl<?> child = invocation.getArgument(1);
                registrations.incrementAndGet();
                pausedBeforePublication.set(child.paused);
                absentBeforePublication.set(!fixture.parent.consumers.containsKey(child.getTopic()));
                if (pauseDuringRegistration) {
                    // A synchronous transport callback can reenter the parent while the new child is
                    // still inside its constructor and consumers.compute has not published it.
                    fixture.parent.pause();
                }
                return null;
            }).when(wire.cnx).registerConsumer(anyLong(), any());
            doAnswer(invocation -> CompletableFuture.completedFuture(Pair.of(wire.cnx, false)))
                    .when(fixture.client).getConnection(anyString(), anyInt());

            CompletableFuture<Void> subscription = fixture.parent.subscribeAsync(TOPIC, 0);
            ConsumerImpl<byte[]> child = fixture.parent.consumers.get(TOPIC);
            assertThat(registrations.get()).isEqualTo(1);
            assertThat(absentBeforePublication.get()).as("registered child is not yet published").isTrue();
            assertThat(pausedBeforePublication.get()).as("unpublished child starts paused").isTrue();
            assertThat(child).isNotNull();
            assertThat(child.paused).as("published child follows the latest parent state")
                    .isEqualTo(pauseDuringRegistration);

            wire.subscribeReply.complete(null);
            subscription.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            wire.drain();
            assertThat(wire.sentPermits.get()).as("initial FLOW follows the published child state")
                    .isEqualTo(pauseDuringRegistration ? 0 : child.getCurrentReceiverQueueSize());
        }
    }

    @Test(dataProvider = "childCreationAndParentState", timeOut = 30000)
    public void testChildPublicationTracksParentPauseState(boolean initiallyPaused, ChildCreation creation)
            throws Exception {
        try (Fixture fixture = new Fixture(initiallyPaused)) {
            if (creation == ChildCreation.PARTITION_EXPANSION) {
                CompletableFuture<Void> initialSubscription = fixture.parent.subscribeAsync(TOPIC, 1);
                ConsumerImpl<byte[]> existing = fixture.parent.consumers.get(TOPIC + "-partition-0");
                new Wire().subscribe(existing);
                initialSubscription.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            String childTopic = switch (creation) {
                case NON_PARTITIONED -> TOPIC;
                case PARTITIONED -> TOPIC + "-partition-0";
                case PARTITION_EXPANSION -> TOPIC + "-partition-1";
            };
            LookupGate gate = new LookupGate(childTopic);
            fixture.lookupGate = gate;
            Future<CompletableFuture<Void>> publication = fixture.workers.submit(() -> fixture.subscribe(creation));
            try {
                assertThat(gate.lookupReached.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .as("child constructor must copy its pause state before publication")
                        .isTrue();
                assertThat(fixture.parent.consumers).doesNotContainKey(childTopic);
                // The transition must finish while the constructor is held. Holding the parent mutex across
                // construction would block this operation and fail the bounded wait.
                fixture.workers.submit(() -> {
                    if (initiallyPaused) {
                        fixture.parent.resume();
                    } else {
                        fixture.parent.pause();
                    }
                }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                gate.allowPublication.countDown();
            }

            CompletableFuture<Void> subscription = publication.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ConsumerImpl<byte[]> child = fixture.parent.consumers.get(childTopic);
            boolean expectedPaused = !initiallyPaused;
            assertThat(child).isNotNull();
            assertThat(child.paused).as("published child pause state").isEqualTo(expectedPaused);

            Wire initial = new Wire();
            initial.subscribe(child);
            subscription.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(child.paused).as("child pause state after initial subscribe").isEqualTo(expectedPaused);
            int expectedFlow = expectedPaused ? 0 : child.getCurrentReceiverQueueSize();
            assertThat(initial.sentPermits.get()).as("initial subscribe FLOW permits").isEqualTo(expectedFlow);

            assertThat(child.changeToConnecting()).isTrue();
            Wire reconnect = new Wire();
            reconnect.subscribe(child);
            initial.drain();
            assertThat(child.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(child.paused).as("child pause state after reconnect").isEqualTo(expectedPaused);
            assertThat(reconnect.sentPermits.get()).as("reconnect FLOW permits").isEqualTo(expectedFlow);

            fixture.parent.pause();
            assertThat(fixture.parent.getConsumers()).allSatisfy(consumer -> assertThat(consumer.paused).isTrue());
            int sentBeforePause = reconnect.sentPermits.get();
            child.increaseAvailablePermits(reconnect.cnx, child.getCurrentReceiverQueueSize());
            reconnect.drain();
            assertThat(reconnect.sentPermits.get()).as("paused children retain returned permits")
                    .isEqualTo(sentBeforePause);
            int pendingPermits = child.getAvailablePermits();
            assertThat(pendingPermits).isPositive();

            fixture.parent.resume();
            reconnect.drain();
            assertThat(fixture.parent.getConsumers()).allSatisfy(consumer -> assertThat(consumer.paused).isFalse());
            assertThat(reconnect.sentPermits.get()).as("resume returns all retained permits")
                    .isEqualTo(sentBeforePause + pendingPermits);
            assertThat(child.getAvailablePermits()).isZero();
        }
    }

    private enum ChildCreation {
        NON_PARTITIONED,
        PARTITIONED,
        PARTITION_EXPANSION
    }

    private static final class LookupGate {
        private final String topic;
        private final AtomicBoolean reached = new AtomicBoolean();
        private final CountDownLatch lookupReached = new CountDownLatch(1);
        private final CountDownLatch allowPublication = new CountDownLatch(1);

        private LookupGate(String topic) {
            this.topic = topic;
        }

        private void awaitPublication(String topic) throws InterruptedException {
            if (this.topic.equals(topic) && reached.compareAndSet(false, true)) {
                // The lookup is called at the end of the real ConsumerImpl constructor, after startPaused
                // has been copied. The constructor has not returned to consumers.compute yet.
                lookupReached.countDown();
                if (!allowPublication.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Child publication was not released");
                }
            }
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final ExecutorProvider external = new ExecutorProvider(1, "multi-topic-pause-external", true);
        private final ExecutorService internal = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("multi-topic-pause-internal", true));
        private final ExecutorService workers = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("multi-topic-pause-worker", true));
        private final PulsarClientImpl client = createPulsarClientMock(external, internal);
        private final MultiTopicsConsumerImpl<byte[]> parent;
        private volatile LookupGate lookupGate;

        private Fixture(boolean initiallyPaused) {
            this(initiallyPaused, null);
        }

        private Fixture(boolean initiallyPaused, Wire initialWire) {
            client.getConfiguration().setStatsIntervalSeconds(0);
            when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
            when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
            when(client.timer().newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> mock(Timeout.class));
            AtomicLong ids = new AtomicLong();
            when(client.newConsumerId()).thenAnswer(invocation -> ids.incrementAndGet());
            when(client.newRequestId()).thenAnswer(invocation -> ids.incrementAndGet());
            when(client.getConnection(anyString(), anyInt())).thenAnswer(invocation -> {
                LookupGate currentGate = lookupGate;
                if (currentGate != null) {
                    currentGate.awaitPublication(invocation.getArgument(0));
                }
                return new CompletableFuture<>();
            });
            when(client.getPartitionsForTopic(TOPIC, false)).thenReturn(CompletableFuture.completedFuture(
                    Arrays.asList(TOPIC + "-partition-0", TOPIC + "-partition-1")));
            ConsumerConfigurationData<byte[]> config = new ConsumerConfigurationData<>();
            config.setSubscriptionName("multi-topic-pause-subscription");
            config.setConsumerName("multi-topic-pause-consumer");
            config.setReceiverQueueSize(4);
            config.setAutoUpdatePartitions(true);
            config.setStartPaused(initiallyPaused);
            if (initialWire != null) {
                config.setTopicNames(Collections.singleton(TOPIC));
                when(client.getPartitionedTopicMetadata(TOPIC, true, false))
                        .thenReturn(CompletableFuture.completedFuture(new PartitionedTopicMetadata()));
                doAnswer(invocation -> CompletableFuture.completedFuture(Pair.of(initialWire.cnx, false)))
                        .when(client).getConnection(anyString(), anyInt());
            }
            parent = new MultiTopicsConsumerImpl<>(client, config, external,
                    new CompletableFuture<Consumer<byte[]>>(), Schema.BYTES, null, true);
        }

        private CompletableFuture<Void> subscribe(ChildCreation creation) {
            if (creation == ChildCreation.PARTITION_EXPANSION) {
                PartitionsChangedListener listener = parent.topicsPartitionChangedListener;
                return listener.onTopicsExtended(Collections.singleton(TOPIC));
            }
            return parent.subscribeAsync(TOPIC, creation == ChildCreation.PARTITIONED ? 1 : 0);
        }

        @Override
        public void close() throws Exception {
            LookupGate currentGate = lookupGate;
            if (currentGate != null) {
                currentGate.allowPublication.countDown();
            }
            workers.shutdown();
            try {
                assertThat(workers.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
                parent.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                workers.shutdownNow();
                external.shutdownNow();
                internal.shutdownNow();
                assertThat(internal.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    /** Real subscribe callbacks and FLOW tasks run against a transport that records and releases frames. */
    private static final class Wire {
        private final ClientCnx cnx = ClientTestFixtures.mockClientCnx();
        private final CompletableFuture<ProducerResponse> subscribeReply = new CompletableFuture<>();
        private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
        private final AtomicInteger sentPermits = new AtomicInteger();

        private Wire() {
            ChannelHandlerContext ctx = cnx.ctx();
            when(ctx.voidPromise()).thenReturn(mock(ChannelPromise.class));
            EventExecutor eventExecutor = ClientTestFixtures.mockEventExecutor();
            when(ctx.executor()).thenReturn(eventExecutor);
            doAnswer(invocation -> {
                tasks.add(invocation.getArgument(0));
                return null;
            }).when(eventExecutor).execute(any(Runnable.class));
            doAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                assertThat(command.getType()).isEqualTo(BaseCommand.Type.FLOW);
                sentPermits.addAndGet(command.getFlow().getMessagePermits());
                return mock(ChannelFuture.class);
            }).when(ctx).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
            doAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                if (command.getType() == BaseCommand.Type.SUBSCRIBE) {
                    return subscribeReply;
                }
                assertThat(command.getType()).isEqualTo(BaseCommand.Type.CLOSE_CONSUMER);
                return CompletableFuture.<ProducerResponse>completedFuture(null);
            }).when(cnx).sendRequestWithId(any(ByteBuf.class), anyLong());
        }

        private void subscribe(ConsumerImpl<byte[]> child) throws Exception {
            CompletableFuture<Void> opened = child.connectionOpened(cnx);
            subscribeReply.complete(null);
            opened.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            drain();
        }

        private void drain() {
            for (int steps = 0; steps < 100; steps++) {
                Runnable task = tasks.poll();
                if (task == null) {
                    return;
                }
                task.run();
            }
            throw new IllegalStateException("FLOW tasks did not quiesce");
        }

        private static BaseCommand decodeAndRelease(ByteBuf frame) {
            try {
                frame.skipBytes(Integer.BYTES);
                int commandSize = (int) frame.readUnsignedInt();
                BaseCommand command = new BaseCommand();
                command.parseFrom(frame, commandSize);
                return command;
            } finally {
                frame.release();
            }
        }
    }
}
