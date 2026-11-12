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
package org.apache.bookkeeper.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class ParallelAuditorShutdownTest {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    private static final int WAIT_SECONDS = 10;

    @Test
    public void capsConcurrencyAndClosesDuplicateElectorsOnce() throws Exception {
        List<AuditorElector> electors = new ArrayList<>();
        CountDownLatch firstWaveStarted = new CountDownLatch(6);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger entered = new AtomicInteger();
        for (int i = 0; i < 13; i++) {
            AuditorElector elector = mock(AuditorElector.class);
            doAnswer(invocation -> {
                maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                entered.incrementAndGet();
                firstWaveStarted.countDown();
                try {
                    await(release);
                } finally {
                    active.decrementAndGet();
                }
                return null;
            }).when(elector).shutdown();
            electors.add(elector);
        }
        List<AuditorElector> duplicates = new ArrayList<>(electors);
        duplicates.add(electors.get(0));
        duplicates.add(electors.get(6));
        duplicates.add(electors.get(12));
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.shutdown(duplicates, SHUTDOWN_TIMEOUT));
        try {
            await(firstWaveStarted);
            assertThat(entered.get()).as("only the first six shutdowns can enter before release").isEqualTo(6);
            assertThat(active.get()).isEqualTo(6);
            assertThat(call.finished.getCount()).as("shutdown must wait for blocked electors").isEqualTo(1);
        } finally {
            release.countDown();
            call.awaitCompletion();
        }
        assertThat(call.failure.get()).isNull();
        assertThat(maximum.get()).isEqualTo(6);
        assertThat(entered.get()).isEqualTo(13);
        assertThat(active.get()).isZero();
        for (AuditorElector elector : electors) {
            verify(elector).shutdown();
        }
    }

    @Test
    public void closeFailuresDoNotAbandonOtherElectors() throws Exception {
        AuditorElector first = mock(AuditorElector.class);
        AuditorElector second = mock(AuditorElector.class);
        AuditorElector blocked = mock(AuditorElector.class);
        IOException firstFailure = new IOException("first close failed");
        AssertionError secondFailure = new AssertionError("second close failed");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        doAnswer(invocation -> {
            throw firstFailure;
        }).when(first).shutdown();
        doAnswer(invocation -> {
            throw secondFailure;
        }).when(second).shutdown();
        doAnswer(invocation -> {
            entered.countDown();
            await(release);
            completed.set(true);
            return null;
        }).when(blocked).shutdown();
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.shutdown(
                List.of(first, second, blocked), SHUTDOWN_TIMEOUT));
        try {
            await(entered);
            assertThat(call.finished.await(50, TimeUnit.MILLISECONDS))
                    .as("a close failure must not allow returning before a peer close finishes").isFalse();
        } finally {
            release.countDown();
            call.awaitCompletion();
        }
        assertThat(completed.get()).isTrue();
        assertThat(call.failure.get()).isSameAs(firstFailure);
        assertThat(firstFailure.getSuppressed()).containsExactly(secondFailure);
        verify(first).shutdown();
        verify(second).shutdown();
        verify(blocked).shutdown();
    }

    @Test
    public void callerInterruptionDrainsShutdownAndRestoresInterruptAfterCleanup() throws Exception {
        AuditorElector elector = mock(AuditorElector.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean laterCleanup = new AtomicBoolean();
        AtomicBoolean workerInterrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            entered.countDown();
            try {
                await(release);
            } catch (InterruptedException error) {
                workerInterrupted.set(true);
                throw error;
            }
            completed.set(true);
            return null;
        }).when(elector).shutdown();
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.cleanup(
                () -> ParallelAuditorShutdown.shutdown(List.of(elector), SHUTDOWN_TIMEOUT),
                () -> {
                    assertThat(completed.get()).isTrue();
                    assertThat(Thread.currentThread().isInterrupted()).isFalse();
                    await(new CountDownLatch(0));
                    laterCleanup.set(true);
                }));
        try {
            await(entered);
            call.thread.interrupt();
            assertThat(call.finished.await(50, TimeUnit.MILLISECONDS))
                    .as("interruption must not abandon a running close").isFalse();
            assertThat(laterCleanup.get()).isFalse();
        } finally {
            release.countDown();
            call.awaitCompletion();
        }
        assertThat(call.failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(call.interruptedAtExit.get()).isTrue();
        assertThat(laterCleanup.get()).isTrue();
        assertThat(workerInterrupted.get()).isFalse();
    }

    @Test
    public void timeoutReportsFailureAfterShutdownAndRemainingCleanupComplete() throws Exception {
        AuditorElector elector = mock(AuditorElector.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean laterCleanup = new AtomicBoolean();
        AtomicBoolean workerInterrupted = new AtomicBoolean();
        doAnswer(invocation -> {
            entered.countDown();
            try {
                await(release);
            } catch (InterruptedException error) {
                workerInterrupted.set(true);
                throw error;
            }
            completed.set(true);
            return null;
        }).when(elector).shutdown();
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.cleanup(
                () -> ParallelAuditorShutdown.shutdown(List.of(elector), Duration.ofMillis(1)),
                () -> {
                    assertThat(completed.get()).isTrue();
                    laterCleanup.set(true);
                }));
        try {
            await(entered);
            assertThat(call.finished.await(50, TimeUnit.MILLISECONDS))
                    .as("an expired deadline must not abandon a running close").isFalse();
            assertThat(laterCleanup.get()).isFalse();
        } finally {
            release.countDown();
            call.awaitCompletion();
        }
        assertThat(call.failure.get()).isInstanceOf(TimeoutException.class);
        assertThat(laterCleanup.get()).isTrue();
        assertThat(workerInterrupted.get()).isFalse();
    }

    @Test
    public void waitsForThreadLocalCleanupBeforeReleasingFixtureResources() throws Exception {
        AuditorElector elector = mock(AuditorElector.class);
        CountDownLatch removalEntered = new CountDownLatch(1);
        CountDownLatch releaseRemoval = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicBoolean removalCompleted = new AtomicBoolean();
        AtomicBoolean laterCleanup = new AtomicBoolean();
        FastThreadLocal<Object> local = new FastThreadLocal<>() {
            @Override
            protected void onRemoval(Object value) throws Exception {
                removalEntered.countDown();
                await(releaseRemoval);
                removalCompleted.set(true);
            }
        };
        doAnswer(invocation -> {
            worker.set(Thread.currentThread());
            local.set(new Object());
            return null;
        }).when(elector).shutdown();
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.cleanup(
                () -> ParallelAuditorShutdown.shutdown(List.of(elector), SHUTDOWN_TIMEOUT),
                () -> {
                    assertThat(removalCompleted.get()).isTrue();
                    assertThat(worker.get().isAlive()).as("fixture cleanup must follow actual worker exit").isFalse();
                    laterCleanup.set(true);
                }));
        try {
            await(removalEntered);
            assertThat(call.finished.await(50, TimeUnit.MILLISECONDS))
                    .as("executor termination precedes Netty thread local cleanup").isFalse();
            assertThat(laterCleanup.get()).isFalse();
        } finally {
            releaseRemoval.countDown();
            call.awaitCompletion();
        }
        assertThat(call.failure.get()).isNull();
        assertThat(laterCleanup.get()).isTrue();
        assertThat(worker.get().isAlive()).isFalse();
    }

    @Test
    public void mergesThreadLocalCleanupErrorsWithCloseFailure() throws Exception {
        AuditorElector elector = mock(AuditorElector.class);
        IOException closeFailure = new IOException("elector close failed");
        AssertionError removalFailure = new AssertionError("thread local cleanup failed");
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicBoolean laterCleanup = new AtomicBoolean();
        FastThreadLocal<Object> local = new FastThreadLocal<>() {
            @Override
            protected void onRemoval(Object value) {
                throw removalFailure;
            }
        };
        doAnswer(invocation -> {
            worker.set(Thread.currentThread());
            local.set(new Object());
            throw closeFailure;
        }).when(elector).shutdown();
        Invocation call = new Invocation(() -> ParallelAuditorShutdown.cleanup(
                () -> ParallelAuditorShutdown.shutdown(List.of(elector), SHUTDOWN_TIMEOUT),
                () -> {
                    assertThat(worker.get().isAlive()).isFalse();
                    laterCleanup.set(true);
                }));
        call.awaitCompletion();
        assertThat(call.failure.get()).isSameAs(closeFailure);
        assertThat(closeFailure.getSuppressed()).containsExactly(removalFailure);
        assertThat(laterCleanup.get()).isTrue();
    }

    @Test
    public void cleanupContinuesAndAggregatesFailuresInOrder() {
        IOException firstFailure = new IOException("elector cleanup failed");
        IOException secondFailure = new IOException("ZooKeeper cleanup failed");
        AssertionError thirdFailure = new AssertionError("driver cleanup failed");
        List<String> steps = new ArrayList<>();
        assertThatThrownBy(() -> ParallelAuditorShutdown.cleanup(
                () -> {
                    steps.add("electors");
                    throw firstFailure;
                },
                () -> {
                    steps.add("ZooKeeper");
                    throw secondFailure;
                },
                () -> {
                    steps.add("driver");
                    throw thirdFailure;
                },
                () -> steps.add("base"))).isSameAs(firstFailure);
        assertThat(steps).containsExactly("electors", "ZooKeeper", "driver", "base");
        assertThat(firstFailure.getSuppressed()).containsExactly(secondFailure, thirdFailure);
    }

    @Test
    public void preexistingInterruptionAllowsBlockingCleanupAndIsRestored() throws Exception {
        AtomicInteger completed = new AtomicInteger();
        Invocation call = new Invocation(() -> {
            Thread.currentThread().interrupt();
            ParallelAuditorShutdown.cleanup(
                    () -> {
                        await(new CountDownLatch(0));
                        completed.incrementAndGet();
                    },
                    () -> {
                        await(new CountDownLatch(0));
                        completed.incrementAndGet();
                    });
        });
        call.awaitCompletion();
        assertThat(call.failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(call.interruptedAtExit.get()).isTrue();
        assertThat(completed.get()).isEqualTo(2);
    }

    @Test
    public void handlesEmptyElectorsAndRejectsInvalidTimeout() throws Exception {
        ParallelAuditorShutdown.shutdown(List.of(), SHUTDOWN_TIMEOUT);
        assertThatThrownBy(() -> ParallelAuditorShutdown.shutdown(List.of(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ParallelAuditorShutdown.shutdown(List.of(), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("controlled cleanup stage must complete").isTrue();
    }

    private static final class Invocation {
        private final CountDownLatch finished = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean interruptedAtExit = new AtomicBoolean();
        private final Thread thread;

        private Invocation(ParallelAuditorShutdown.Cleanup operation) {
            thread = new Thread(() -> {
                try {
                    operation.run();
                } catch (Throwable error) {
                    failure.set(error);
                } finally {
                    interruptedAtExit.set(Thread.currentThread().isInterrupted());
                    finished.countDown();
                }
            }, "parallel-auditor-shutdown-test-caller");
            thread.start();
        }

        private void awaitCompletion() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            assertThat(thread.isAlive()).as("test caller must exit after cleanup is released").isFalse();
        }
    }
}
