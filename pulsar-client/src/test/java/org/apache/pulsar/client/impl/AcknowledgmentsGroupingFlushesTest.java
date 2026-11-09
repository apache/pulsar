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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import java.util.Arrays;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AcknowledgmentsGroupingFlushesTest {
    private static final long GROUP_TIME_MICROS = TimeUnit.MILLISECONDS.toMicros(10);

    private EventExecutor executor;

    @BeforeMethod
    public void setUp() {
        executor = new DefaultEventExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
    }

    @Test
    public void flushesOfAnExecutorAndGroupTimeShareOneTask() {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        AtomicInteger otherGroupTime = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration firstRegistration =
                registerRepeating(executor, GROUP_TIME_MICROS, first::incrementAndGet);
        AcknowledgmentsGroupingFlushes.Registration secondRegistration =
                registerRepeating(executor, GROUP_TIME_MICROS, second::incrementAndGet);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
        AcknowledgmentsGroupingFlushes.Registration otherRegistration =
                registerRepeating(executor, 2 * GROUP_TIME_MICROS, otherGroupTime::incrementAndGet);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(2);

        Awaitility.await().untilAsserted(() -> {
            assertThat(first.get()).isGreaterThan(2);
            assertThat(second.get()).isGreaterThan(2);
            assertThat(otherGroupTime.get()).isGreaterThan(2);
        });

        // a closed registration isn't flushed any more, and the others still are
        firstRegistration.close();
        // closing again does nothing
        firstRegistration.close();
        int firstAtClose = first.get();
        waitForTheNextRuns(second);
        // a run that had started when it was closed may complete
        assertThat(first.get()).isLessThanOrEqualTo(firstAtClose + 1);

        // the last registration of a group time removes its task
        secondRegistration.close();
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
        otherRegistration.close();
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
        int otherAfterClose = otherGroupTime.get();
        sleepGroupTimes(5);
        assertThat(otherGroupTime.get()).isLessThanOrEqualTo(otherAfterClose + 1);
    }

    @Test
    public void aRegistrationClosedDuringARunIsntFlushedByIt() {
        // whichever runs first closes the others, which the run's iterator may already have read
        int count = 3;
        AtomicInteger[] flushes = new AtomicInteger[count];
        AcknowledgmentsGroupingFlushes.Registration[] registrations =
                new AcknowledgmentsGroupingFlushes.Registration[count];
        for (int i = 0; i < count; i++) {
            int index = i;
            flushes[i] = new AtomicInteger();
            registrations[i] = registerRepeating(executor, GROUP_TIME_MICROS, () -> {
                flushes[index].incrementAndGet();
                for (int other = 0; other < count; other++) {
                    if (other != index) {
                        registrations[other].close();
                    }
                }
            }, false);
        }
        // the flushes are requested once every registration is in the array, which the flushes read
        for (AcknowledgmentsGroupingFlushes.Registration registration : registrations) {
            registration.requestFlush();
        }
        try {
            Awaitility.await().untilAsserted(() -> assertThat(
                    Arrays.stream(flushes).mapToInt(AtomicInteger::get).max().getAsInt())
                    .isGreaterThan(2));
            // only the first one ran, in every run
            assertThat(Arrays.stream(flushes).filter(flushCount -> flushCount.get() > 0).count())
                    .isEqualTo(1);
        } finally {
            for (AcknowledgmentsGroupingFlushes.Registration registration : registrations) {
                registration.close();
            }
        }
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
    }

    @Test
    public void tasksAreCreatedAgainAfterTheLastRegistrationCloses() {
        for (int i = 0; i < 100; i++) {
            AtomicInteger flushes = new AtomicInteger();
            AcknowledgmentsGroupingFlushes.Registration registration =
                    registerRepeating(executor, GROUP_TIME_MICROS, flushes::incrementAndGet);
            assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
            registration.close();
            assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
        }
        AtomicInteger flushes = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration registration =
                registerRepeating(executor, GROUP_TIME_MICROS, flushes::incrementAndGet);
        try {
            Awaitility.await().untilAsserted(() -> assertThat(flushes.get()).isGreaterThan(2));
        } finally {
            registration.close();
        }
    }

    @Test
    public void aFailingFlushDoesntStopTheOthers() {
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration failingRegistration =
                registerRepeating(executor, GROUP_TIME_MICROS, () -> {
                    failing.incrementAndGet();
                    throw new IllegalStateException("failing flush");
                });
        AcknowledgmentsGroupingFlushes.Registration otherRegistration =
                registerRepeating(executor, GROUP_TIME_MICROS, other::incrementAndGet);
        try {
            Awaitility.await().untilAsserted(() -> {
                assertThat(failing.get()).isGreaterThan(2);
                assertThat(other.get()).isGreaterThan(2);
            });
        } finally {
            failingRegistration.close();
            otherRegistration.close();
        }
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
    }

    @Test
    public void onlyRequestedFlushesRun() throws Exception {
        AtomicInteger requestedFlushes = new AtomicInteger();
        AtomicInteger idleFlushes = new AtomicInteger();
        // counts the shared task's runs
        AtomicInteger runs = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration probe =
                registerRepeating(executor, GROUP_TIME_MICROS, runs::incrementAndGet);
        AcknowledgmentsGroupingFlushes.Registration requested =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, requestedFlushes::incrementAndGet);
        AcknowledgmentsGroupingFlushes.Registration idle =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, idleFlushes::incrementAndGet);
        try {
            // requests before a run are flushed once
            executor.submit(() -> {
                requested.requestFlush();
                requested.requestFlush();
            }).get();
            waitForTheNextRuns(runs);
            assertThat(requestedFlushes.get()).isEqualTo(1);
            assertThat(idleFlushes.get()).isZero();

            // without a request, a flush doesn't run again
            waitForTheNextRuns(runs);
            assertThat(requestedFlushes.get()).isEqualTo(1);

            // a request from another thread is flushed by a later run
            requested.requestFlush();
            Awaitility.await().untilAsserted(() -> assertThat(requestedFlushes.get()).isEqualTo(2));

            // a request of a registration closed before the next run isn't flushed
            executor.submit(() -> {
                requested.requestFlush();
                requested.close();
            }).get();
            waitForTheNextRuns(runs);
            assertThat(requestedFlushes.get()).isEqualTo(2);
            assertThat(idleFlushes.get()).isZero();
        } finally {
            probe.close();
            requested.close();
            idle.close();
        }
    }

    @Test
    public void aFlushRequestedWhileItRunsRunsInTheNextRun() {
        AtomicInteger runs = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration probe =
                registerRepeating(executor, GROUP_TIME_MICROS, runs::incrementAndGet);
        AtomicInteger flushes = new AtomicInteger();
        AtomicInteger runsAtFlushes = new AtomicInteger(-1);
        AtomicBoolean repeatsInSameRun = new AtomicBoolean();
        AtomicReference<AcknowledgmentsGroupingFlushes.Registration> self = new AtomicReference<>();
        self.set(AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, () -> {
            // the probe runs in the same runs, so a flush in the same run sees the same run count
            if (runsAtFlushes.getAndSet(runs.get()) == runs.get()) {
                repeatsInSameRun.set(true);
            }
            if (flushes.incrementAndGet() < 3) {
                self.get().requestFlush();
            }
        }));
        try {
            self.get().requestFlush();
            Awaitility.await().untilAsserted(() -> assertThat(flushes.get()).isEqualTo(3));
            assertThat(repeatsInSameRun.get()).isFalse();
            waitForTheNextRuns(runs);
            assertThat(flushes.get()).isEqualTo(3);
        } finally {
            probe.close();
            self.get().close();
        }
    }

    @Test
    public void aRejectingExecutorFailsTheRegistration() {
        executor.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
        assertThatThrownBy(() -> AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, () -> { }))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
    }

    @Test
    public void theExecutorsTerminationRemovesItsTasks() {
        AtomicInteger flushes = new AtomicInteger();
        // a consumer that is never closed, as when a client shuts down without closing its consumers
        AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, flushes::incrementAndGet);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
        executor.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
        Awaitility.await().untilAsserted(
                () -> assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero());
    }

    // Registers a flush that requests itself again when it runs, as a tracker that has acks to send in every period
    private static AcknowledgmentsGroupingFlushes.Registration registerRepeating(EventExecutor executor,
                                                                               long groupTimeMicros, Runnable flush) {
        return registerRepeating(executor, groupTimeMicros, flush, true);
    }

    // Registers a flush that requests the next one after each run, and requests the first one when requestFirst
    private static AcknowledgmentsGroupingFlushes.Registration registerRepeating(EventExecutor executor,
                                                                               long groupTimeMicros, Runnable flush,
                                                                               boolean requestFirst) {
        AtomicReference<AcknowledgmentsGroupingFlushes.Registration> self = new AtomicReference<>();
        AcknowledgmentsGroupingFlushes.Registration registration =
                AcknowledgmentsGroupingFlushes.register(executor, groupTimeMicros, () -> {
                    try {
                        flush.run();
                    } finally {
                        self.get().requestFlush();
                    }
                });
        self.set(registration);
        if (requestFirst) {
            registration.requestFlush();
        }
        return registration;
    }

    // Waits until the counter has advanced twice more
    private static void waitForTheNextRuns(AtomicInteger counter) {
        int start = counter.get();
        Awaitility.await().until(() -> counter.get() >= start + 2);
    }

    private static void sleepGroupTimes(int groupTimes) {
        try {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(GROUP_TIME_MICROS) * groupTimes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
