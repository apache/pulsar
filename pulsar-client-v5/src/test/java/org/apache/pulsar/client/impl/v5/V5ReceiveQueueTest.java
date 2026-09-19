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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.netty.util.HashedWheelTimer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V5ReceiveQueueTest {

    private ExecutorService executor;
    private HashedWheelTimer timer;
    private V5ReceiveQueue<Integer> queue;

    @BeforeMethod
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        timer = new HashedWheelTimer();
        queue = new V5ReceiveQueue<>(executor, timer, 1000);
    }

    /** Wait for the single-threaded executor to drain queued tasks (offer/receive run there). */
    private void flush() throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        executor.execute(() -> f.complete(null));
        f.get(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        executor.shutdownNow();
        timer.stop();
    }

    @Test
    public void bufferedMessageIsDeliveredToReceiveAsync() throws Exception {
        Message<Integer> m = msg(1);
        queue.offer(m);
        assertSame(queue.receiveAsync().get(5, TimeUnit.SECONDS), m);
    }

    @Test
    public void receiveAsyncCompletesWhenMessageArrivesLater() throws Exception {
        CompletableFuture<Message<Integer>> f = queue.receiveAsync();
        assertTrue(!f.isDone());
        Message<Integer> m = msg(1);
        queue.offer(m);
        assertSame(f.get(5, TimeUnit.SECONDS), m);
    }

    @Test
    public void deliversInFifoOrder() throws Exception {
        Message<Integer> m1 = msg(1);
        Message<Integer> m2 = msg(2);
        Message<Integer> m3 = msg(3);
        queue.offer(m1);
        queue.offer(m2);
        queue.offer(m3);
        assertSame(queue.receiveAsync().get(5, TimeUnit.SECONDS), m1);
        assertSame(queue.receiveAsync().get(5, TimeUnit.SECONDS), m2);
        assertSame(queue.receiveAsync().get(5, TimeUnit.SECONDS), m3);
    }

    @Test
    public void timedReceiveReturnsNullOnTimeout() throws Exception {
        assertNull(queue.receiveAsync(Duration.ofMillis(150)).get(5, TimeUnit.SECONDS));
    }

    @Test
    public void messageBeatsTimeout() throws Exception {
        CompletableFuture<Message<Integer>> f = queue.receiveAsync(Duration.ofSeconds(30));
        Message<Integer> m = msg(1);
        queue.offer(m);
        assertSame(f.get(5, TimeUnit.SECONDS), m);
    }

    @Test
    public void blockingTakeReturnsBufferedMessage() throws Exception {
        Message<Integer> m = msg(1);
        queue.offer(m);
        assertSame(queue.take(), m);
    }

    @Test
    public void blockingPollTimesOutToNull() throws Exception {
        assertNull(queue.poll(Duration.ofMillis(150)));
    }

    @Test
    public void closeFailsPendingReceive() {
        CompletableFuture<Message<Integer>> f = queue.receiveAsync();
        queue.close();
        ExecutionException ex = expectThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof PulsarClientException.AlreadyClosedException);
    }

    @Test
    public void receiveAfterCloseFailsFast() {
        queue.close();
        ExecutionException ex = expectThrows(ExecutionException.class,
                () -> queue.receiveAsync().get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof PulsarClientException.AlreadyClosedException);
    }

    @Test
    public void cancelledWaiterDoesNotConsumeMessage() throws Exception {
        CompletableFuture<Message<Integer>> cancelled = queue.receiveAsync();
        cancelled.cancel(true);
        Message<Integer> m = msg(1);
        queue.offer(m);
        // The message must reach a live receiver, not be swallowed by the cancelled one.
        assertSame(queue.receiveAsync().get(5, TimeUnit.SECONDS), m);
    }

    @Test
    public void receiveMultiReturnsUpToMax() throws Exception {
        for (int i = 1; i <= 5; i++) {
            queue.offer(msg(i));
        }
        List<Message<Integer>> batch = queue.receiveMulti(3, Duration.ofSeconds(5));
        assertEquals(batch.size(), 3);
        assertEquals(batch.get(0).value(), Integer.valueOf(1));
        assertEquals(batch.get(1).value(), Integer.valueOf(2));
        assertEquals(batch.get(2).value(), Integer.valueOf(3));
    }

    @Test
    public void receiveMultiReturnsPartialOnTimeout() throws Exception {
        queue.offer(msg(1));
        queue.offer(msg(2));
        List<Message<Integer>> batch = queue.receiveMultiAsync(5, Duration.ofMillis(300))
                .get(5, TimeUnit.SECONDS);
        assertEquals(batch.size(), 2);
    }

    @Test
    public void receiveMultiEmptyOnTimeoutWithNoMessages() throws Exception {
        List<Message<Integer>> batch = queue.receiveMultiAsync(5, Duration.ofMillis(150))
                .get(5, TimeUnit.SECONDS);
        assertTrue(batch.isEmpty());
    }

    @Test
    public void fastPathGrantsCapacityImmediatelyWhenBufferHasRoom() {
        // Empty buffer, nobody paused: offer() must return an already-completed (shared)
        // future decided on the caller thread — no executor hop before the loop re-arms.
        assertTrue(queue.offer(msg(1)).isDone());
    }

    @Test
    public void offerBelowWatermarkCompletesImmediately() throws Exception {
        // receiverQueueSize=4 -> pause at 4, resume at 2.
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 4);
        for (int i = 1; i <= 3; i++) {
            // Completes right away while there is room in the buffer.
            q.offer(msg(i)).get(5, TimeUnit.SECONDS);
            flush();
        }
    }

    @Test
    public void offerAtHighWatermarkPausesUntilDrain() throws Exception {
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 4);
        for (int i = 1; i <= 3; i++) {
            q.offer(msg(i)).get(5, TimeUnit.SECONDS);
            // Let the enqueue task run so the fast-path size snapshot is exact and the
            // 4th offer deterministically takes the slow path.
            flush();
        }
        // The 4th message hits the high watermark and parks the producer.
        CompletableFuture<Void> paused = q.offer(msg(4));
        flush();
        assertTrue(!paused.isDone());

        // Draining below the low watermark (2) resumes it.
        q.receiveAsync().get(5, TimeUnit.SECONDS); // buffer 4 -> 3, still above low
        assertTrue(!paused.isDone());
        q.receiveAsync().get(5, TimeUnit.SECONDS); // buffer 3 -> 2, resume
        paused.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void fairnessPausesSubsequentOffersOncePeerIsPaused() throws Exception {
        // receiverQueueSize=8 -> pause at 8 (or at >4 once a peer is paused), resume at 4.
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 8);
        for (int i = 1; i <= 7; i++) {
            q.offer(msg(i)).get(5, TimeUnit.SECONDS);
            flush();
        }
        CompletableFuture<Void> pausedA = q.offer(msg(8)); // buffer 8 >= high -> parked
        flush();
        assertTrue(!pausedA.isDone());

        for (int i = 0; i < 3; i++) {
            q.receiveAsync().get(5, TimeUnit.SECONDS);     // buffer 8 -> 5, above low
        }
        assertTrue(!pausedA.isDone());

        // v4 fairness clause: with a peer already paused, a new offer above the low
        // watermark must park too instead of overtaking the paused one indefinitely.
        CompletableFuture<Void> pausedB = q.offer(msg(9)); // buffer 6 > low(4) with peer paused
        flush();
        assertTrue(!pausedB.isDone());

        q.receiveAsync().get(5, TimeUnit.SECONDS);         // buffer 6 -> 5
        q.receiveAsync().get(5, TimeUnit.SECONDS);         // buffer 5 -> 4 == low, resume all
        pausedA.get(5, TimeUnit.SECONDS);
        pausedB.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void resumeViaReceiveMultiDrain() throws Exception {
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 4);
        for (int i = 1; i <= 3; i++) {
            q.offer(msg(i)).get(5, TimeUnit.SECONDS);
            flush();
        }
        CompletableFuture<Void> paused = q.offer(msg(4));
        flush();
        assertTrue(!paused.isDone());

        // Batch drain crosses the low watermark (4 -> 1) and must resume the producer.
        List<Message<Integer>> batch = q.receiveMulti(3, Duration.ofSeconds(5));
        assertEquals(batch.size(), 3);
        paused.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void resumeViaTimedReceiveDrain() throws Exception {
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 2);
        q.offer(msg(1)).get(5, TimeUnit.SECONDS);
        flush();
        CompletableFuture<Void> paused = q.offer(msg(2)); // buffer 2 >= high -> parked
        flush();
        assertTrue(!paused.isDone());

        // Timed receive's immediate-poll path (2 -> 1 == low) must resume the producer.
        assertEquals(q.poll(Duration.ofSeconds(5)).value(), Integer.valueOf(1));
        paused.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void closeReleasesPausedProducers() throws Exception {
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 2);
        q.offer(msg(1)).get(5, TimeUnit.SECONDS);          // buffer 1, below watermark
        flush();
        CompletableFuture<Void> paused = q.offer(msg(2));  // buffer 2, parked
        flush();
        assertTrue(!paused.isDone());
        // close() must release parked producers so their receive loops observe the close.
        q.close();
        paused.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void blockingReceiveDoesNotDependOnTheExecutor() throws Exception {
        // Stall the executor: nothing posted to it can run until the gate opens.
        CountDownLatch gate = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            queue.offer(m1);
            queue.offer(m2);
            // The blocking receives pull straight from the buffer on the caller thread — no
            // round trip through the (stalled) executor per message.
            assertSame(queue.take(), m1);
            assertSame(queue.poll(Duration.ofSeconds(5)), m2);
            assertNull(queue.poll(Duration.ofMillis(50)));
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void fastPathHoldsUpToTheHighWatermark() throws Exception {
        // receiverQueueSize=4 -> the fast path (shared completed future, decided on the caller
        // thread) must apply while the buffer is below 4, not just below the low watermark (2).
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 4);
        CountDownLatch gate = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            for (int i = 1; i <= 3; i++) {
                assertTrue(q.offer(msg(i)).isDone(), "offer " + i + " should take the fast path");
            }
            // The 4th fills the buffer: the grant is decided on the executor, so it is still
            // pending while the executor is stalled.
            assertTrue(!q.offer(msg(4)).isDone());
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void directReceiveDuringPausePublicationStillResumesTheProducer() throws Exception {
        // receiverQueueSize=1: the first message fills the buffer, so its offer goes to the
        // executor to decide the pause.
        V5ReceiveQueue<Integer> q = new V5ReceiveQueue<>(executor, timer, 1);
        AtomicReference<Message<Integer>> taken = new AtomicReference<>();
        // Between the executor's size check and the pause publication, a blocking receive
        // drains the buffer. It sees producersPaused == false, so it schedules no resume.
        q.beforePausePublishedHook = () -> {
            q.beforePausePublishedHook = null;
            try {
                taken.set(q.take());
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        };
        Message<Integer> m = msg(1);
        CompletableFuture<Void> capacity = q.offer(m);
        // The producer must not stay parked against an empty buffer.
        capacity.get(5, TimeUnit.SECONDS);
        assertSame(taken.get(), m);
    }

    @Test
    public void olderPendingReceiveIsServedBeforeANewerOne() throws Exception {
        CompletableFuture<Message<Integer>> a = queue.receiveAsync();
        flush(); // A is registered, waiting on an empty buffer
        CountDownLatch gate = stallExecutor();
        try {
            CompletableFuture<Message<Integer>> b = queue.receiveAsync(); // queued behind the gate
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            queue.offer(m1); // appended straight to the buffer; A's drain queues behind B
            queue.offer(m2);
            gate.countDown();
            assertSame(a.get(5, TimeUnit.SECONDS), m1);
            assertSame(b.get(5, TimeUnit.SECONDS), m2);
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void olderPendingReceiveIsServedBeforeANewerTimedOne() throws Exception {
        CompletableFuture<Message<Integer>> a = queue.receiveAsync(Duration.ofSeconds(30));
        flush();
        CountDownLatch gate = stallExecutor();
        try {
            CompletableFuture<Message<Integer>> b = queue.receiveAsync(Duration.ofSeconds(30));
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            queue.offer(m1);
            queue.offer(m2);
            gate.countDown();
            assertSame(a.get(5, TimeUnit.SECONDS), m1);
            assertSame(b.get(5, TimeUnit.SECONDS), m2);
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void receiveMultiDoesNotOvertakeAnOlderPendingReceive() throws Exception {
        CompletableFuture<Message<Integer>> a = queue.receiveAsync();
        flush();
        CountDownLatch gate = stallExecutor();
        try {
            CompletableFuture<List<Message<Integer>>> batch = queue.receiveMultiAsync(2, Duration.ofSeconds(30));
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            Message<Integer> m3 = msg(3);
            queue.offer(m1);
            queue.offer(m2);
            queue.offer(m3);
            gate.countDown();
            assertSame(a.get(5, TimeUnit.SECONDS), m1);
            List<Message<Integer>> got = batch.get(5, TimeUnit.SECONDS);
            assertEquals(got.size(), 2);
            assertSame(got.get(0), m2);
            assertSame(got.get(1), m3);
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void olderReceiveContinuationCancellingTheNewcomerDoesNotLoseAMessage() throws Exception {
        CompletableFuture<Message<Integer>> a = queue.receiveAsync();
        flush();
        CountDownLatch gate = stallExecutor();
        try {
            CompletableFuture<Message<Integer>> b = queue.receiveAsync();
            // Serving A runs this inline on the executor, right before B would poll for itself.
            a.thenRun(() -> b.cancel(false));
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            queue.offer(m1);
            queue.offer(m2);
            gate.countDown();
            assertSame(a.get(5, TimeUnit.SECONDS), m1);
            flush();
            assertTrue(b.isCancelled());
            // The cancelled B must not have taken m2 with it.
            assertSame(queue.poll(Duration.ofSeconds(1)), m2);
        } finally {
            gate.countDown();
        }
    }

    @Test
    public void olderReceiveContinuationCancellingTheNewcomerTimedDoesNotLoseAMessage() throws Exception {
        CompletableFuture<Message<Integer>> a = queue.receiveAsync(Duration.ofSeconds(30));
        flush();
        CountDownLatch gate = stallExecutor();
        try {
            CompletableFuture<Message<Integer>> b = queue.receiveAsync(Duration.ofSeconds(30));
            a.thenRun(() -> b.cancel(false));
            Message<Integer> m1 = msg(1);
            Message<Integer> m2 = msg(2);
            queue.offer(m1);
            queue.offer(m2);
            gate.countDown();
            assertSame(a.get(5, TimeUnit.SECONDS), m1);
            flush();
            assertTrue(b.isCancelled());
            assertSame(queue.poll(Duration.ofSeconds(1)), m2);
        } finally {
            gate.countDown();
        }
    }

    /** Block the single executor thread until the returned latch is counted down. */
    private CountDownLatch stallExecutor() {
        CountDownLatch gate = new CountDownLatch(1);
        executor.execute(() -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return gate;
    }

    private static Message<Integer> msg(int id) {
        return new IntMessage(id);
    }

    /** Minimal {@link Message} whose {@link #value()} carries a test id; other accessors are inert. */
    private static final class IntMessage implements Message<Integer> {
        private final int id;

        IntMessage(int id) {
            this.id = id;
        }

        @Override
        public Integer value() {
            return id;
        }

        @Override
        public byte[] data() {
            return new byte[0];
        }

        @Override
        public MessageId id() {
            return null;
        }

        @Override
        public Optional<String> key() {
            return Optional.empty();
        }

        @Override
        public Map<String, String> properties() {
            return Map.of();
        }

        @Override
        public Instant publishTime() {
            return Instant.EPOCH;
        }

        @Override
        public Optional<Instant> eventTime() {
            return Optional.empty();
        }

        @Override
        public long sequenceId() {
            return id;
        }

        @Override
        public Optional<String> producerName() {
            return Optional.empty();
        }

        @Override
        public String topic() {
            return "test";
        }

        @Override
        public int redeliveryCount() {
            return 0;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Optional<String> replicatedFrom() {
            return Optional.empty();
        }
    }
}
