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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.testng.annotations.Test;

@CustomLog
public class BrokerTestUtilTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testReceiveMessagesQuietTime() throws Exception {
        // The receive loop runs on a manual clock, so that what it does depends on the times below and not on how
        // fast the JVM runs it.
        ManualReceiveClock clock = new ManualReceiveClock();

        // Mock consumers
        Consumer<Integer> consumer1 = mock(Consumer.class);
        Consumer<Integer> consumer2 = mock(Consumer.class);

        long consumer1DelayMs = 300L;
        long consumer2DelayMs = 400L;
        long quietTimeMs = 500L;

        // The first receive of each consumer gets a message after its delay, the later ones get none
        when(consumer1.receiveAsync())
                .thenAnswer(invocation -> clock.messageAfter(consumer1DelayMs))
                .thenAnswer(invocation -> new CompletableFuture<>());
        when(consumer2.receiveAsync())
                .thenAnswer(invocation -> clock.messageAfter(consumer2DelayMs))
                .thenAnswer(invocation -> new CompletableFuture<>());

        // Atomic variables to track message handling
        AtomicInteger messageCount = new AtomicInteger(0);

        // Message handler
        BiFunction<Consumer<Integer>, Message<Integer>, Boolean> messageHandler = (consumer, msg) -> {
            messageCount.incrementAndGet();
            return true;
        };

        // Call receiveMessages method, and track the time when it finishes
        CompletableFuture<Long> endNanos = BrokerTestUtil.receiveMessagesAsync(messageHandler,
                        Duration.ofMillis(quietTimeMs), clock, Stream.of(consumer1, consumer2))
                .thenApply(__ -> clock.nanoTime());
        clock.run();
        assertThat(endNanos).isCompleted();

        // Verify that messages were attempted to be received: consumer1 receives again after its own quiet time
        // has passed, since consumer2 received a message later
        verify(consumer1, times(3)).receiveAsync();
        verify(consumer2, times(2)).receiveAsync();

        // Verify that the message handler was called
        assertEquals(messageCount.get(), 2);

        // Verify that receiveMessages finished when the quiet time had passed since the last message
        assertEquals(TimeUnit.NANOSECONDS.toMillis(endNanos.join()), consumer2DelayMs + quietTimeMs);
    }

    /**
     * A manual clock for {@link BrokerTestUtil#receiveMessagesAsync}: time moves only when {@link #run()} goes to the
     * next event, and the receive loop runs in the thread that runs the events.
     */
    private static class ManualReceiveClock implements BrokerTestUtil.ReceiveClock {
        private record Event(long nanos, long sequence, Runnable action) {
        }

        private final PriorityQueue<Event> events =
                new PriorityQueue<>(Comparator.comparingLong(Event::nanos).thenComparingLong(Event::sequence));
        private long nanos;
        private long sequence;

        // Runs the events in time order, until there are none left
        void run() {
            Event event;
            while ((event = events.poll()) != null) {
                nanos = event.nanos();
                event.action().run();
            }
        }

        @SuppressWarnings("unchecked")
        CompletableFuture<Message<Integer>> messageAfter(long delayMs) {
            CompletableFuture<Message<Integer>> future = new CompletableFuture<>();
            schedule(TimeUnit.MILLISECONDS.toNanos(delayMs), () -> future.complete(mock(Message.class)));
            return future;
        }

        private void schedule(long delayNanos, Runnable action) {
            events.add(new Event(nanos + delayNanos, sequence++, action));
        }

        @Override
        public long nanoTime() {
            return nanos;
        }

        @Override
        public <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, long timeoutNanos) {
            // Like a timer, time out once the timeout has passed, not at its last nanosecond
            schedule(timeoutNanos + 1, () -> future.completeExceptionally(new TimeoutException()));
            return future;
        }

        @Override
        public Executor executor() {
            // Start the next receive before the event that ended the previous one returns
            return Runnable::run;
        }
    }
}
