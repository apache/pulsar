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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PrometheusMetricsGeneratorUtilsTest {

    private static final String LABEL_NAME_CLUSTER = "cluster";

    @Test
    public void testGenerateSystemMetricsWithSpecifyCluster() throws Exception {
        String defaultClusterValue = "cluster_test";
        String specifyClusterValue = "lb_x";
        String metricsName = "label_contains_cluster" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .labelNames(LABEL_NAME_CLUSTER)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.labels(specifyClusterValue).inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        assertTrue(out.toString().contains(
                String.format("%s_total{cluster=\"%s\"} 1.0", metricsName, specifyClusterValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    @Test
    public void testGenerateSystemMetricsWithDefaultCluster() throws Exception {
        String defaultClusterValue = "cluster_test";
        String labelName = "lb_name";
        String labelValue = "lb_value";
        // default cluster.
        String metricsName = "label_use_default_cluster" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .labelNames(labelName)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.labels(labelValue).inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        assertTrue(out.toString().contains(
                String.format("%s_total{cluster=\"%s\",%s=\"%s\"} 1.0",
                        metricsName, defaultClusterValue, labelName, labelValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    @Test
    public void testGenerateSystemMetricsWithoutCustomizedLabel() throws Exception {
        String defaultClusterValue = "cluster_test";
        // default cluster.
        String metricsName = "label_without_customized_label" + randomString();
        Counter counter = new Counter.Builder()
                .name(metricsName)
                .help("x")
                .register(CollectorRegistry.defaultRegistry);
        counter.inc();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsGeneratorUtils.generate(defaultClusterValue, out,  Collections.emptyList());
        assertThat(out.toString()).contains("# TYPE " + metricsName + " counter\n")
                .doesNotContain("# TYPE " + metricsName + "_total counter\n");
        assertTrue(out.toString().contains(
                String.format("%s_total{cluster=\"%s\"} 1.0", metricsName, defaultClusterValue)
        ));
        // cleanup
        out.close();
        CollectorRegistry.defaultRegistry.unregister(counter);
    }

    @DataProvider
    public Object[][] optInCounterNameSuffixes() {
        return new Object[][]{{""}, {"_total"}};
    }

    @Test(dataProvider = "optInCounterNameSuffixes")
    public void testLegacyMetricsUnchangedByOptInCounter(String familyNameSuffix) throws Exception {
        String prefix = "metric_compatibility_" + randomString() + "_";
        String legacyPrefix = prefix + "legacy_";
        String newMetricName = prefix + "opt_in";
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        Counter counter = Counter.build(legacyPrefix + "counter", "Legacy counter").create();
        Gauge gauge = Gauge.build(legacyPrefix + "gauge", "Legacy gauge").create();
        Histogram histogram = Histogram.build(legacyPrefix + "histogram", "Legacy histogram")
                .buckets(1, 2).create();
        // No quantile window: all exported summary state stays deterministic between scrapes.
        Summary summary = Summary.build(legacyPrefix + "summary", "Legacy summary").create();
        OptInCounterCollector optIn = new OptInCounterCollector(newMetricName + familyNameSuffix);
        List<Collector> legacyCollectors = List.of(counter, gauge, histogram, summary);
        boolean optInRegistered = false;
        try {
            legacyCollectors.forEach(registry::register);
            counter.inc(7);
            gauge.set(3.5);
            histogram.observe(0.5);
            histogram.observe(1.5);
            histogram.observe(3);
            summary.observe(2);
            summary.observe(6);
            var histogramBefore = histogram.labels().get();
            var summaryBefore = summary.get();
            Runnable assertLegacyState = () -> {
                assertThat(counter.get()).as("legacy counter state").isEqualTo(7);
                assertThat(gauge.get()).as("legacy gauge state").isEqualTo(3.5);
                var histogramAfter = histogram.labels().get();
                assertThat(histogramAfter.sum).as("legacy histogram sum").isEqualTo(histogramBefore.sum);
                assertThat(histogramAfter.buckets).as("legacy histogram buckets")
                        .containsExactly(histogramBefore.buckets);
                assertThat(histogramAfter.created).as("legacy histogram creation time")
                        .isEqualTo(histogramBefore.created);
                var summaryAfter = summary.get();
                assertThat(summaryAfter.count).as("legacy summary count").isEqualTo(summaryBefore.count);
                assertThat(summaryAfter.sum).as("legacy summary sum").isEqualTo(summaryBefore.sum);
                assertThat(summaryAfter.quantiles).as("legacy summary quantiles").isEqualTo(summaryBefore.quantiles);
                assertThat(summaryAfter.created).as("legacy summary creation time").isEqualTo(summaryBefore.created);
            };
            String baseline = fixtureLines(exportMetrics(), legacyPrefix);
            assertThat(baseline).contains("# TYPE " + legacyPrefix + "counter counter\n")
                    .contains(legacyPrefix + "counter_total{cluster=\"compatibility\"} 7.0")
                    .contains(legacyPrefix + "counter_created{cluster=\"compatibility\"} ")
                    .contains("# TYPE " + legacyPrefix + "gauge gauge\n")
                    .contains("# TYPE " + legacyPrefix + "histogram histogram\n")
                    .contains("# TYPE " + legacyPrefix + "summary summary\n")
                    .doesNotContain("# TYPE " + legacyPrefix + "counter_total counter");
            byte[] legacyBefore = baseline.getBytes(StandardCharsets.UTF_8);
            assertLegacyState.run();

            registry.register(optIn);
            optInRegistered = true;
            String afterRegistration = exportMetrics();
            assertOptInCounterExport(afterRegistration, newMetricName, 0);
            assertLegacyExportUnchanged(afterRegistration, legacyPrefix, legacyBefore);
            assertLegacyState.run();

            optIn.value.addAndGet(9);
            for (int scrape = 0; scrape < 3; scrape++) {
                String afterIncrement = exportMetrics();
                assertOptInCounterExport(afterIncrement, newMetricName, 9);
                assertLegacyExportUnchanged(afterIncrement, legacyPrefix, legacyBefore);
                assertLegacyState.run();
            }

            registry.unregister(optIn);
            optInRegistered = false;
            String afterUnregistration = exportMetrics();
            assertThat(fixtureLines(afterUnregistration, newMetricName)).as("opt-in collector was removed").isEmpty();
            assertLegacyExportUnchanged(afterUnregistration, legacyPrefix, legacyBefore);
            assertLegacyState.run();
        } finally {
            if (optInRegistered) {
                registry.unregister(optIn);
            }
            legacyCollectors.forEach(registry::unregister);
        }
    }

    private static String exportMetrics() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PrometheusMetricsGeneratorUtils.generate("compatibility", out, Collections.emptyList());
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    private static String fixtureLines(String metrics, String prefix) {
        return metrics.lines()
                .filter(line -> line.startsWith(prefix) || line.startsWith("# TYPE " + prefix))
                .sorted()
                .collect(Collectors.joining("\n"));
    }

    private static void assertLegacyExportUnchanged(String metrics, String prefix, byte[] baseline) {
        assertThat(fixtureLines(metrics, prefix).getBytes(StandardCharsets.UTF_8))
                .as("all legacy TYPE and sample bytes remain unchanged")
                .containsExactly(baseline);
    }

    private static void assertOptInCounterExport(String metrics, String name, long value) {
        String fixture = fixtureLines(metrics, name);
        assertThat(fixture.lines().filter(line -> line.startsWith("# TYPE ")).toList())
                .as("one TYPE declaration for the opt-in counter's total samples")
                .containsExactly("# TYPE " + name + "_total counter");
        assertThat(fixture.lines().filter(line -> !line.startsWith("#")).toList())
                .as("only total samples, without an automatically generated created sample")
                .containsExactlyInAnyOrder(
                        name + "_total{cluster=\"compatibility\",result=\"written\"} " + (double) value,
                        name + "_total{cluster=\"compatibility\",result=\"unchanged\"} 0.0");
    }

    private static final class OptInCounterCollector extends Collector implements Collector.Describable {
        private final String name;
        private final AtomicLong value = new AtomicLong();

        private OptInCounterCollector(String name) {
            this.name = name;
        }

        @Override
        public List<MetricFamilySamples> describe() {
            return List.of(new PrometheusCounterMetricFamily(name, "Opt-in counter", List.of("result")));
        }

        @Override
        public List<MetricFamilySamples> collect() {
            var family = new PrometheusCounterMetricFamily(name, "Opt-in counter", List.of("result"));
            family.addMetric(List.of("written"), value.get());
            family.addMetric(List.of("unchanged"), 0);
            return List.of(family);
        }
    }

    private static String randomString(){
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
