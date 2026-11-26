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
package org.apache.pulsar.broker.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class DimensionStatsTest {
    @Test
    void shouldCalculateQuantiles() {
        DimensionStats dimensionStats = new DimensionStats("test", 100, null);
        for (int i = 1; i <= 10000; i++) {
            dimensionStats.recordDimensionTimeValue(i, TimeUnit.MILLISECONDS);
        }
        DimensionStats.DimensionStatsSnapshot snapshot = dimensionStats.getSnapshot();
        assertEquals(snapshot.getMeanDimension(), 5000, 1);
        assertEquals(snapshot.getMedianDimension(), 5000, 100);
        assertEquals(snapshot.getDimension75(), 7500, 100);
        assertEquals(snapshot.getDimension95(), 9500, 100);
        assertEquals(snapshot.getDimension99(), 9900, 100);
        assertEquals(snapshot.getDimension999(), 9990, 10);
        assertEquals(snapshot.getDimension9999(), 9999, 1);
        assertEquals(snapshot.getDimensionCount(), 10000);
        assertEquals(snapshot.getDimensionSum(), 50005000);
    }

    /** An unused or just reset instance reads as the simpleclient version did: no count, and no mean or quantile. */
    @Test
    void shouldReadUnusedStatsAsEmpty() {
        DimensionStats dimensionStats = new DimensionStats("test_empty", 100, null);
        assertEmpty(dimensionStats.getSnapshot());
        dimensionStats.recordDimensionTimeValue(7, TimeUnit.MILLISECONDS);
        assertEquals(dimensionStats.getSnapshot().getMeanDimension(), 7.0);
        dimensionStats.reset();
        assertEmpty(dimensionStats.getSnapshot());
    }

    private static void assertEmpty(DimensionStats.DimensionStatsSnapshot snapshot) {
        assertEquals(snapshot.getDimensionCount(), 0.0);
        assertEquals(snapshot.getDimensionSum(), 0.0);
        assertTrue(Double.isNaN(snapshot.getMeanDimension()), "mean " + snapshot.getMeanDimension());
        for (double quantile : new double[]{snapshot.getMedianDimension(), snapshot.getDimension75(),
                snapshot.getDimension95(), snapshot.getDimension99(), snapshot.getDimension999(),
                snapshot.getDimension9999()}) {
            assertTrue(Double.isNaN(quantile), "quantile " + quantile);
        }
    }

    @Test
    void shouldRegisterAndUnregisterOnItsOwnRegistry() {
        PrometheusRegistry registry = new PrometheusRegistry();
        DimensionStats first = new DimensionStats("test_lifecycle", 100, registry);
        assertTrue(first.isRegistered());
        first.recordDimensionTimeValue(3, TimeUnit.MILLISECONDS);
        assertEquals(registry.scrape("test_lifecycle"::equals).size(), 1);

        DimensionStats duplicate = new DimensionStats("test_lifecycle", 100, registry);
        assertFalse(duplicate.isRegistered(), "a second instance of the same name must not win the registry");
        duplicate.close();
        assertEquals(registry.scrape("test_lifecycle"::equals).size(), 1,
                "closing the unregistered duplicate must leave the owner registered");

        first.close();
        assertFalse(first.isRegistered());
        assertEquals(registry.scrape("test_lifecycle"::equals).size(), 0);

        DimensionStats second = new DimensionStats("test_lifecycle", 100, registry);
        assertTrue(second.isRegistered(), "a new instance must register once the earlier owner closed");
        second.close();
    }

    /** reset() clears the summary's data point; recording and reading must not trip over that. */
    @Test(timeOut = 60_000)
    void shouldRecordAndReadWhileResetRuns() throws Exception {
        DimensionStats dimensionStats = new DimensionStats("test-reset", 100, null);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread resetter = new Thread(() -> {
            try {
                while (!stop.get()) {
                    dimensionStats.reset();
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
        // a daemon, so a run that is cut short by the timeout cannot keep the JVM alive
        resetter.setDaemon(true);
        resetter.start();
        try {
            for (int i = 0; i < 10_000 && failure.get() == null; i++) {
                try {
                    dimensionStats.recordDimensionTimeValue(1, TimeUnit.MILLISECONDS);
                    DimensionStats.DimensionStatsSnapshot snapshot = dimensionStats.getSnapshot();
                    // Whatever survived the resets so far: only 1 ms observations, so sum and count agree.
                    assertEquals(snapshot.getDimensionSum(), snapshot.getDimensionCount());
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }
        } finally {
            stop.set(true);
            resetter.join();
        }
        assertNull(failure.get(), "recording or reading failed while reset() ran concurrently: " + failure.get());
    }
}