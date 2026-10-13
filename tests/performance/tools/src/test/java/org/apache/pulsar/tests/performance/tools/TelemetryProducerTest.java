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
package org.apache.pulsar.tests.performance.tools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TelemetryProducerTest {
    private ScheduledExecutorService executor;

    @BeforeMethod
    public void startExecutor() {
        executor = Executors.newScheduledThreadPool(4);
    }

    @AfterMethod(alwaysRun = true)
    public void stopExecutor() {
        executor.shutdownNow();
    }

    @Test
    public void precreateOrderIsTheGatewaysOrderOneAtATimeAndARepeatableRandomOrderConcurrently() {
        assertThat(TelemetryProducer.precreateOrder(6, 1)).containsExactly(0, 1, 2, 3, 4, 5);

        int[] concurrent = TelemetryProducer.precreateOrder(500, 32);
        // every producer once, not in the order of the gateways, and the same order for every run
        assertThat(concurrent).containsExactlyInAnyOrder(IntStream.range(0, 500).toArray());
        assertThat(concurrent).isNotEqualTo(IntStream.range(0, 500).toArray());
        assertThat(TelemetryProducer.precreateOrder(500, 32)).isEqualTo(concurrent);
    }

    @Test
    public void precreatesUpToTheConcurrencyAtATime() throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        Integer[] created = new Integer[100];
        TelemetryProducer.precreate(IntStream.range(0, 100).toArray(), 8, created, index -> {
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            return completeLater(() -> {
                inFlight.decrementAndGet();
                return index;
            });
        });
        assertThat(created).containsExactly(IntStream.range(0, 100).boxed().toArray(Integer[]::new));
        assertThat(maxInFlight.get()).isBetween(1, 8);
    }

    @Test
    public void stopsAfterAFailureAndThrowsItOnceTheStartedCreationsHaveCompleted() {
        IllegalStateException failure = new IllegalStateException("lookup failed");
        List<Integer> started = new CopyOnWriteArrayList<>();
        List<Integer> completed = new CopyOnWriteArrayList<>();
        Integer[] created = new Integer[100];
        assertThatThrownBy(() -> TelemetryProducer.precreate(IntStream.range(0, 100).toArray(), 4, created, index -> {
            started.add(index);
            if (index == 10) {
                return CompletableFuture.failedFuture(failure);
            }
            return completeLater(() -> {
                completed.add(index);
                return index;
            });
        })).isSameAs(failure);
        // no more creations started after the failure was seen, and every started one completed before the throw
        assertThat(started).hasSizeLessThan(100);
        assertThat(started.size()).isLessThanOrEqualTo(10 + 1 + 4);
        assertThat(completed).containsExactlyInAnyOrderElementsOf(started.stream().filter(i -> i != 10).toList());
    }

    // completes the value's future after a millisecond on another thread, as a producer's creation completes
    private <T> CompletableFuture<T> completeLater(Supplier<T> value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.schedule(() -> future.complete(value.get()), 1, TimeUnit.MILLISECONDS);
        return future;
    }
}
