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
package org.apache.pulsar.testclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PerformanceProducerCompletionTest {
    @DataProvider
    public Object[][] completionTiming() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "completionTiming")
    public void successUpdatesCountsBeforeCompleting(boolean completed) {
        RecordingProducer producer = new RecordingProducer();
        AtomicLong totalSent = new AtomicLong();
        CompletableFuture<Object> send = new CompletableFuture<>();
        if (completed) {
            send.complete(new Object());
        }
        CompletableFuture<Void> tracked =
                producer.trackSendCompletion(send, new byte[128], totalSent, System.nanoTime(), 0);
        if (!completed) {
            assertThat(tracked).isNotDone();
            assertThat(totalSent.get()).isZero();
            send.complete(new Object());
        }
        assertThat(tracked.join()).isNull();
        assertThat(totalSent.get()).isEqualTo(1);
        assertThat(producer.getMessagesFailed()).isZero();
        assertThat(producer.observedCause).isNull();
    }

    @DataProvider
    public Object[][] sendFailures() {
        return new Object[][] {
            {false, "ordinary"}, {true, "ordinary"},
            {false, "wrapped"}, {true, "wrapped"},
            {false, "cancelled"}, {true, "cancelled"},
            {false, "closed"}, {true, "closed"},
            {false, "interrupted"}, {true, "interrupted"}
        };
    }

    @Test(dataProvider = "sendFailures")
    public void failuresAreHandledBeforeCompleting(boolean completed, String failureKind) {
        RecordingProducer producer = new RecordingProducer();
        AtomicLong totalSent = new AtomicLong();
        Throwable cause = switch (failureKind) {
            case "closed" -> new PulsarClientException.AlreadyClosedException("closed");
            case "interrupted" -> new InterruptedException("interrupted");
            case "cancelled" -> new CancellationException("cancelled");
            default -> new IllegalStateException("send failed");
        };
        Throwable failure = failureKind.equals("wrapped") ? new CompletionException(cause) : cause;
        CompletableFuture<Object> send = new CompletableFuture<>();
        try {
            if (completed) {
                send.completeExceptionally(failure);
            }
            CompletableFuture<Void> tracked =
                    producer.trackSendCompletion(send, new byte[128], totalSent, System.nanoTime(), 0);
            if (!completed) {
                assertThat(tracked).isNotDone();
                send.completeExceptionally(failure);
            }
            // Transaction send lists depend on failures being consumed by this stage.
            assertThat(CompletableFuture.allOf(tracked).join()).isNull();
            assertThat(producer.observedCause).isSameAs(cause);
            assertThat(totalSent.get()).isZero();
            boolean ignored = failureKind.equals("closed") || failureKind.equals("interrupted");
            assertThat(producer.getMessagesFailed()).isEqualTo(ignored ? 0 : 1);
            assertThat(Thread.currentThread().isInterrupted()).isEqualTo(failureKind.equals("interrupted"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test(dataProvider = "completionTiming")
    public void metricFailuresUseTheSendFailureHandler(boolean completed) {
        RecordingProducer producer = new RecordingProducer();
        AtomicLong totalSent = new AtomicLong();
        CompletableFuture<Object> send = new CompletableFuture<>();
        if (completed) {
            send.complete(null);
        }
        // A future send timestamp produces a negative latency, rejected by HdrHistogram.
        CompletableFuture<Void> tracked =
                producer.trackSendCompletion(send, new byte[128], totalSent, Long.MAX_VALUE, 0);
        if (!completed) {
            send.complete(null);
        }
        assertThat(tracked.join()).isNull();
        assertThat(totalSent.get()).isEqualTo(1);
        assertThat(producer.getMessagesFailed()).isEqualTo(1);
        assertThat(producer.observedCause).isInstanceOf(RuntimeException.class);
    }

    @DataProvider
    public Object[][] failureOrigins() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "failureOrigins")
    public void errorHandlerFailureRemainsExceptional(boolean accountingFailure) {
        IllegalArgumentException handlerFailure = new IllegalArgumentException("classification failed");
        PerformanceProducerV4 producer = new PerformanceProducerV4() {
            @Override
            protected boolean isAlreadyClosedException(Throwable cause) {
                throw handlerFailure;
            }
        };
        CompletableFuture<?> send = accountingFailure ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(new IllegalStateException("send failed"));
        CompletableFuture<Void> tracked = producer.trackSendCompletion(send,
                new byte[128], new AtomicLong(), accountingFailure ? Long.MAX_VALUE : System.nanoTime(), 0);
        assertThatThrownBy(tracked::join).isInstanceOf(CompletionException.class).hasCause(handlerFailure);
    }

    @Test
    public void interruptionIsRestoredOnTheCompletingThread() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        CompletableFuture<Object> send = new CompletableFuture<>();
        CompletableFuture<Void> tracked = producer.trackSendCompletion(
                send, new byte[128], new AtomicLong(), System.nanoTime(), 0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            boolean interrupted = executor.submit(() -> {
                try {
                    send.completeExceptionally(new InterruptedException("send interrupted"));
                    return Thread.currentThread().isInterrupted();
                } finally {
                    Thread.interrupted();
                }
            }).get(10, TimeUnit.SECONDS);
            assertThat(interrupted).isTrue();
            assertThat(tracked.join()).isNull();
            assertThat(Thread.currentThread().isInterrupted()).isFalse();
            assertThat(producer.getMessagesFailed()).isZero();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void exitOnFailureRunsAfterFailureAccounting() {
        PerformanceProducerV4 producer = new PerformanceProducerV4();
        producer.exitOnFailure = true;
        try (MockedStatic<PerfClientUtils> utils = mockStatic(PerfClientUtils.class)) {
            utils.when(() -> PerfClientUtils.exit(1)).thenAnswer(invocation -> {
                assertThat(producer.getMessagesFailed()).isEqualTo(1);
                return null;
            });
            CompletableFuture<Void> tracked = producer.trackSendCompletion(
                    CompletableFuture.failedFuture(new IllegalStateException("send failed")),
                    new byte[128], new AtomicLong(), System.nanoTime(), 0);
            assertThat(tracked.join()).isNull();
            utils.verify(() -> PerfClientUtils.exit(1));
        }
    }

    private static final class RecordingProducer extends PerformanceProducerV4 {
        private Throwable observedCause;

        @Override
        protected boolean isAlreadyClosedException(Throwable cause) {
            observedCause = cause;
            return super.isAlreadyClosedException(cause);
        }
    }
}
