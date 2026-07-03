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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        queue = new V5ReceiveQueue<>(executor, timer);
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
