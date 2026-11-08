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
package org.apache.pulsar.jetty.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.time.Duration;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class JettyStatisticsCollectorTest {

    /**
     * Jetty counts requests and responses in an {@code int} and bytes and nanoseconds in a {@code long}. Once a
     * total wraps, the scrape must still succeed: a 1.x counter rejects a negative value. The int totals read as
     * unsigned, a wrapped long total reads as zero.
     */
    @Test
    public void testWrappedJettyTotalsDoNotFailTheScrape() {
        StatisticsHandler stats = mock(StatisticsHandler.class);
        when(stats.getRequests()).thenReturn(Integer.MIN_VALUE);
        when(stats.getHandleTotal()).thenReturn(-1);
        when(stats.getResponses2xx()).thenReturn(-7);
        when(stats.getStatisticsDuration()).thenReturn(Duration.ofSeconds(1));
        when(stats.getBytesRead()).thenReturn(Long.MAX_VALUE);
        when(stats.getBytesWritten()).thenReturn(1L);
        when(stats.getRequestTimeTotal()).thenReturn(Long.MIN_VALUE);

        PrometheusRegistry registry = new PrometheusRegistry();
        registry.register(new JettyStatisticsCollector(stats));
        MetricSnapshots snapshots = registry.scrape();

        assertThat(counterValue(snapshots, "jetty_requests")).isEqualTo(2147483648.0);
        assertThat(counterValue(snapshots, "jetty_dispatched")).isEqualTo(4294967295.0);
        assertThat(((CounterSnapshot) family(snapshots, "jetty_responses")).getDataPoints().stream()
                .filter(p -> "2xx".equals(p.getLabels().get("code")))
                .findFirst().orElseThrow().getValue()).isEqualTo(4294967289.0);
        assertThat(counterValue(snapshots, "jetty_responses_bytes")).isZero();
        assertThat(counterValue(snapshots, "jetty_request_time_seconds")).isZero();
    }

    @Test
    public void testSecondCollectorIsRejectedAndUnregisterFreesTheNames() {
        PrometheusRegistry registry = new PrometheusRegistry();
        JettyStatisticsCollector first = new JettyStatisticsCollector(mock(StatisticsHandler.class));
        registry.register(first);
        assertThrows(IllegalArgumentException.class,
                () -> registry.register(new JettyStatisticsCollector(mock(StatisticsHandler.class))));
        registry.unregister(first);
        registry.register(new JettyStatisticsCollector(mock(StatisticsHandler.class)));
    }

    private static double counterValue(MetricSnapshots snapshots, String name) {
        return ((CounterSnapshot) family(snapshots, name)).getDataPoints().get(0).getValue();
    }

    private static MetricSnapshot family(MetricSnapshots snapshots, String name) {
        for (MetricSnapshot snapshot : snapshots) {
            if (name.equals(snapshot.getMetadata().getName())) {
                return snapshot;
            }
        }
        throw new AssertionError("No family " + name);
    }
}
