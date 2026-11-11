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
package org.apache.pulsar.broker.web;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Multimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.Test;

/**
 * {@link WebExecutorStats} is a JVM-wide singleton: the first {@link WebService} registers the gauges, the first
 * close unregisters them and nothing re-registers them afterwards. Broker-level tests therefore cannot rely on the
 * families being present, so this test resets the singleton and checks the gauges and {@code close()} directly.
 */
@Test(groups = "broker")
public class WebExecutorStatsTest {
    private static final String PREFIX = "pulsar_web_executor_";
    private static final String MAX = PREFIX + "max_threads";
    private static final String MIN = PREFIX + "min_threads";
    private static final String IDLE = PREFIX + "idle_threads";
    private static final String ACTIVE = PREFIX + "active_threads";
    private static final String CURRENT = PREFIX + "current_threads";
    private static final Set<String> FAMILIES = Set.of(MAX, MIN, IDLE, ACTIVE, CURRENT);
    private static final String CLUSTER = "web-executor-stats-cluster";

    @Test
    public void testGaugesFollowTheExecutorAndCloseUnregistersThem() throws Exception {
        resetSingleton();
        // Jetty prestarts min(8, maxThreads) core threads, so with 4 max threads the pool is fully idle at rest.
        WebExecutorThreadPool pool = new WebExecutorThreadPool(4, "web-executor-stats-test");
        pool.start();
        WebExecutorThreadPool otherPool = new WebExecutorThreadPool(16, "web-executor-stats-other");
        try {
            WebExecutorStats stats = WebExecutorStats.getStats(pool);
            // The first executor wins; later web services share its gauges.
            assertSame(WebExecutorStats.getStats(otherPool), stats);

            Map<String, Double> registry = registryValues();
            assertEquals(registry.keySet(), FAMILIES);
            assertEquals(registry.get(MAX), 4.0);
            assertEquals(registry.get(MIN), 4.0);
            assertEquals(registry.get(CURRENT), 4.0);
            assertEquals(registry.get(IDLE), 4.0);
            assertEquals(registry.get(ACTIVE), 0.0);

            // The broker scrape renders the 1.x registry under the legacy names with the cluster label.
            Multimap<String, Metric> scraped = parseMetrics(render());
            for (String family : FAMILIES) {
                List<Metric> samples = new ArrayList<>(scraped.get(family));
                assertEquals(samples.size(), 1, family);
                assertEquals(samples.get(0).tags.get("cluster"), CLUSTER, family);
                assertEquals(samples.get(0).value, registry.get(family), family);
            }

            stats.close();
            assertTrue(registryValues().isEmpty());
            assertTrue(parseMetrics(render()).keySet().stream().noneMatch(name -> name.startsWith(PREFIX)));
            // Closing again is a no-op.
            stats.close();
            // Pre-existing behaviour: a closed singleton is handed out again but never re-registers its gauges.
            assertSame(WebExecutorStats.getStats(pool), stats);
            assertTrue(registryValues().isEmpty());
        } finally {
            pool.stop();
        }
    }

    private static Map<String, Double> registryValues() {
        Map<String, Double> values = new HashMap<>();
        for (MetricSnapshot snapshot : PrometheusRegistry.defaultRegistry.scrape(name -> name.startsWith(PREFIX))) {
            GaugeSnapshot gauge = (GaugeSnapshot) snapshot;
            assertEquals(gauge.getDataPoints().size(), 1, snapshot.getMetadata().getPrometheusName());
            values.put(snapshot.getMetadata().getPrometheusName(), gauge.getDataPoints().get(0).getValue());
        }
        return values;
    }

    private static String render() {
        ByteBuf buf = Unpooled.buffer();
        try {
            PrometheusMetricsGeneratorUtils.generateSystemMetrics(new SimpleTextOutputStream(buf), CLUSTER);
            return buf.toString(StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }

    /**
     * Puts the singleton back into its never-created state. A previous owner in this JVM has already unregistered
     * its gauges on close; if one leaked, closing it here unregisters them so that the new instance can register.
     */
    private static void resetSingleton() throws Exception {
        Field instanceField = WebExecutorStats.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        WebExecutorStats previous = (WebExecutorStats) instanceField.get(null);
        if (previous != null) {
            previous.close();
        }
        Field closedField = WebExecutorStats.class.getDeclaredField("CLOSED");
        closedField.setAccessible(true);
        ((AtomicBoolean) closedField.get(null)).set(false);
        instanceField.set(null, null);
    }
}
