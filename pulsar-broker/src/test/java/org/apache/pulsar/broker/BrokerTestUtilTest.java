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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@Slf4j
public class BrokerTestUtilTest {
    @Test
    public void testReceiveMessagesQuietTime() throws Exception {
        // Mock consumers
        Consumer<Integer> consumer1 = mock(Consumer.class);
        Consumer<Integer> consumer2 = mock(Consumer.class);

        long consumer1DelayMs = 300L;
        long consumer2DelayMs = 400L;
        long quietTimeMs = 500L;

        // Define behavior for receiveAsync with delay
        AtomicBoolean consumer1FutureContinueSupplying = new AtomicBoolean(true);
        when(consumer1.receiveAsync()).thenAnswer(invocation -> {
            if (consumer1FutureContinueSupplying.get()) {
                CompletableFuture<Message> messageCompletableFuture =
                        CompletableFuture.supplyAsync(() -> mock(Message.class),
                                CompletableFuture.delayedExecutor(consumer1DelayMs, TimeUnit.MILLISECONDS));
                consumer1FutureContinueSupplying.set(false);
                // continue supplying while the future is cancelled or timed out
                FutureUtil.whenCancelledOrTimedOut(messageCompletableFuture, () -> {
                    consumer1FutureContinueSupplying.set(true);
                });
                return messageCompletableFuture;
            } else {
                return new CompletableFuture<>();
            }
        });
        AtomicBoolean consumer2FutureContinueSupplying = new AtomicBoolean(true);
        when(consumer2.receiveAsync()).thenAnswer(invocation -> {
            if (consumer2FutureContinueSupplying.get()) {
                CompletableFuture<Message> messageCompletableFuture =
                        CompletableFuture.supplyAsync(() -> mock(Message.class),
                                CompletableFuture.delayedExecutor(consumer2DelayMs, TimeUnit.MILLISECONDS));
                consumer2FutureContinueSupplying.set(false);
                // continue supplying while the future is cancelled or timed out
                FutureUtil.whenCancelledOrTimedOut(messageCompletableFuture, () -> {
                    consumer2FutureContinueSupplying.set(true);
                });
                return messageCompletableFuture;
            } else {
                return new CompletableFuture<>();
            }
        });

        // Atomic variables to track message handling
        AtomicInteger messageCount = new AtomicInteger(0);

        // Message handler
        BiFunction<Consumer<Integer>, Message<Integer>, Boolean> messageHandler = (consumer, msg) -> {
            messageCount.incrementAndGet();
            return true;
        };

        // Track start time
        long startTime = System.nanoTime();

        // Call receiveMessages method
        BrokerTestUtil.receiveMessages(messageHandler, Duration.ofMillis(quietTimeMs), consumer1, consumer2);

        // Track end time
        long endTime = System.nanoTime();

        // Verify that messages were attempted to be received
        verify(consumer1, times(3)).receiveAsync();
        verify(consumer2, times(2)).receiveAsync();

        // Verify that the message handler was called
        assertEquals(messageCount.get(), 2);

        // Verify the time spent is as expected (within a reasonable margin)
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        assertThat(durationMillis).isBetween(consumer2DelayMs + quietTimeMs,
                consumer2DelayMs + quietTimeMs + (quietTimeMs / 2));
    }
}