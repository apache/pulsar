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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.util.Map;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.PulsarStats;
import org.testng.annotations.Test;

/**
 * The topic load summary is registered per {@link BrokerOperabilityMetrics} instance on a registry that outlives
 * the broker, so closing must hand the family back for the next broker in the same JVM.
 */
public class BrokerOperabilityMetricsTest {
    private static final String TOPIC_LOAD_TIMES = "pulsar_topic_load_times";

    private static PulsarService mockPulsar() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getConfig()).thenReturn(config);
        when(pulsar.getAdvertisedAddress()).thenReturn("localhost");
        PulsarBrokerOpenTelemetry openTelemetry = mock(PulsarBrokerOpenTelemetry.class);
        when(openTelemetry.getMeter()).thenReturn(OpenTelemetry.noop().getMeter("test"));
        when(pulsar.getOpenTelemetry()).thenReturn(openTelemetry);
        return pulsar;
    }

    private static long scrapedTopicLoadCount(PrometheusRegistry registry) {
        MetricSnapshots snapshots = registry.scrape(TOPIC_LOAD_TIMES::equals);
        assertEquals(snapshots.size(), 1, "expected exactly one " + TOPIC_LOAD_TIMES + " family");
        SummarySnapshot summary = (SummarySnapshot) snapshots.get(0);
        assertEquals(summary.getDataPoints().size(), 1);
        return summary.getDataPoints().get(0).getCount();
    }

    @Test
    public void testCloseUnregistersTopicLoadSummaryForTheNextInstance() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PulsarService pulsar = mockPulsar();

        BrokerOperabilityMetrics first = new BrokerOperabilityMetrics(pulsar, registry);
        assertTrue(first.getTopicLoadStats().isRegistered());
        first.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1);

        // A second broker in the same JVM loses the registration while the first one is open.
        BrokerOperabilityMetrics overlapping = new BrokerOperabilityMetrics(pulsar, registry);
        assertFalse(overlapping.getTopicLoadStats().isRegistered());
        overlapping.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1, "the scrape must still read the registered instance");
        overlapping.close();
        assertEquals(scrapedTopicLoadCount(registry), 1, "closing a non-owner must not unregister the owner");

        first.close();
        assertFalse(first.getTopicLoadStats().isRegistered());
        assertEquals(registry.scrape(TOPIC_LOAD_TIMES::equals).size(), 0,
                "close() must unregister the topic load summary");

        BrokerOperabilityMetrics second = new BrokerOperabilityMetrics(pulsar, registry);
        assertTrue(second.getTopicLoadStats().isRegistered(), "a new instance must register after close()");
        second.recordTopicLoadTimeValue(7);
        second.recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 2, "the scrape must read the new instance's observations");
        second.close();
    }

    @Test
    public void testPulsarStatsCloseReleasesTopicLoadSummary() {
        PrometheusRegistry registry = new PrometheusRegistry();
        PulsarService pulsar = mockPulsar();

        PulsarStats first = new PulsarStats(pulsar, registry);
        assertTrue(first.getBrokerOperabilityMetrics().getTopicLoadStats().isRegistered());
        first.close();
        assertEquals(registry.scrape(TOPIC_LOAD_TIMES::equals).size(), 0,
                "PulsarStats.close() must close BrokerOperabilityMetrics");

        PulsarStats second = new PulsarStats(pulsar, registry);
        assertTrue(second.getBrokerOperabilityMetrics().getTopicLoadStats().isRegistered(),
                "a broker restarted in the same JVM must own the topic load summary again");
        second.getBrokerOperabilityMetrics().recordTopicLoadTimeValue(7);
        assertEquals(scrapedTopicLoadCount(registry), 1);
        second.close();
    }

    /** getMetrics() resets before it reads, so the exported topic load mean comes from an empty summary. */
    @Test
    public void testTopicLoadMeanIsNaNWhenNothingWasRecordedSinceTheLastReset() {
        BrokerOperabilityMetrics metrics = new BrokerOperabilityMetrics(mockPulsar(), new PrometheusRegistry());
        try {
            // two values, so the mean is not also one of the quantiles
            metrics.recordTopicLoadTimeValue(1);
            metrics.recordTopicLoadTimeValue(5);
            Map<String, Object> recorded = metrics.getTopicLoadMetrics().getMetrics();
            assertEquals(recorded.get("brk_topic_load_time_mean_ms"), 3.0);
            assertEquals(recorded.get("brk_topic_load_rate_s"), 2.0);

            Map<String, Object> afterReset = metrics.getMetrics().get(0).getMetrics();
            assertEquals(afterReset.get("brk_topic_load_rate_s"), 0.0);
            assertEquals(afterReset.get("brk_topic_load_time_mean_ms"), Double.NaN);
            assertEquals(afterReset.get("brk_topic_load_time_median_ms"), Double.NaN);
        } finally {
            metrics.close();
        }
    }
}
