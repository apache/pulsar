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
package org.apache.pulsar.functions.worker.rest.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.metrics.model.registry.Collector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerStatsManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The standalone worker /metrics must expose both Prometheus Java client registries when standard metrics are
 * enabled, because shared collectors such as the Caffeine cache metrics register on the 1.x registry.
 */
public class FunctionsMetricsResourceTest {

    private static final String LEGACY_NAME = "functions_metrics_resource_test_legacy";
    private static final String MODERN_NAME = "functions_metrics_resource_test_modern";
    private static final String FUNCTIONS_MARKER = "functions_metrics_resource_test_marker 1.0\n";

    private Gauge legacyGauge;
    private Collector modernCollector;
    private WorkerStatsManager standaloneWorkerStats;

    @BeforeClass
    public void startStandaloneWorkerStats() {
        // A standalone worker registers the Netty direct memory gauges for the life of the JVM, and registering
        // them a second time fails. A later run of this class in the same JVM therefore reuses the first run's.
        if (!isInTheDefaultRegistry("jvm_memory_direct_bytes_max")
                && !isInTheDefaultRegistry("jvm_memory_direct_bytes_used")) {
            standaloneWorkerStats = new WorkerStatsManager(
                    new WorkerConfig().setPulsarFunctionsCluster("test-cluster"), true);
        }
    }

    @AfterClass(alwaysRun = true)
    public void closeStandaloneWorkerStats() {
        if (standaloneWorkerStats != null) {
            standaloneWorkerStats.close();
        }
    }

    @BeforeMethod
    public void setup() {
        legacyGauge = Gauge.build(LEGACY_NAME, "legacy registry gauge").register(CollectorRegistry.defaultRegistry);
        legacyGauge.set(1);
        modernCollector = new Collector() {
            @Override
            public MetricSnapshot collect() {
                return GaugeSnapshot.builder()
                        .name(MODERN_NAME)
                        .help("1.x registry gauge")
                        .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder().value(1).build())
                        .build();
            }
        };
        PrometheusRegistry.defaultRegistry.register(modernCollector);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        CollectorRegistry.defaultRegistry.unregister(legacyGauge);
        PrometheusRegistry.defaultRegistry.unregister(modernCollector);
    }

    @Test
    public void testStandardMetricsEnabledExportsBothRegistries() throws Exception {
        String body = scrape(true);
        assertTrue(body.contains("# TYPE " + LEGACY_NAME + " gauge\n"), body);
        assertTrue(body.contains(LEGACY_NAME + " 1.0\n"), body);
        assertTrue(body.contains("# TYPE " + MODERN_NAME + " gauge\n"), body);
        assertTrue(body.contains(MODERN_NAME + " 1.0\n"), body);
        assertTrue(body.contains(FUNCTIONS_MARKER), body);
    }

    @Test
    public void testStandardMetricsDisabledExportsOnlyFunctionsStats() throws Exception {
        String body = scrape(false);
        assertFalse(body.contains(LEGACY_NAME), body);
        assertFalse(body.contains(MODERN_NAME), body);
        assertTrue(body.contains(FUNCTIONS_MARKER), body);
    }

    @Test
    public void testStandaloneWorkerExportsTheNettyDirectMemoryGauges() throws Exception {
        String body = scrape(true);
        assertThat(sampleValues(body, "jvm_memory_direct_bytes_max"))
                .as("jvm_memory_direct_bytes_max samples in the worker /metrics")
                .containsExactly((double) DirectMemoryUtils.jvmMaxDirectMemory());
        assertThat(sampleValues(body, "jvm_memory_direct_bytes_used"))
                .as("jvm_memory_direct_bytes_used samples in the worker /metrics")
                .singleElement()
                .satisfies(used -> assertThat(used).as("-1 means the JVM cannot estimate it")
                        .isGreaterThanOrEqualTo(-1.0));
        // The worker /metrics writes the simpleclient registry and the 1.x registry, so the scrape alone does not
        // tell in which of them the gauges are registered
        for (String gauge : List.of("jvm_memory_direct_bytes_max", "jvm_memory_direct_bytes_used")) {
            assertThat(PrometheusRegistry.defaultRegistry.scrape(gauge::equals))
                    .as("%s in the 1.x registry", gauge)
                    .hasSize(1);
            assertThat(Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples()))
                    .as("families in the simpleclient registry")
                    .extracting(family -> family.name)
                    .doesNotContain(gauge);
        }
    }

    private static String scrape(boolean includeStandardPrometheusMetrics) throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setIncludeStandardPrometheusMetrics(includeStandardPrometheusMetrics);
        WorkerService workerService = mock(WorkerService.class);
        when(workerService.getWorkerConfig()).thenReturn(workerConfig);
        doAnswer(invocation -> {
            SimpleTextOutputStream out = invocation.getArgument(0);
            out.write(FUNCTIONS_MARKER);
            return null;
        }).when(workerService).generateFunctionsStats(any(SimpleTextOutputStream.class));

        FunctionsMetricsResource resource = new FunctionsMetricsResource() {
            @Override
            public WorkerService get() {
                return workerService;
            }
        };
        Response response = resource.getMetrics();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ((StreamingOutput) response.getEntity()).write(out);
        return out.toString(StandardCharsets.UTF_8);
    }

    private static List<Double> sampleValues(String body, String name) {
        Matcher sample = Pattern.compile("^" + Pattern.quote(name) + " (\\S+)$", Pattern.MULTILINE).matcher(body);
        List<Double> values = new ArrayList<>();
        while (sample.find()) {
            values.add(Double.parseDouble(sample.group(1)));
        }
        return values;
    }

    private static boolean isInTheDefaultRegistry(String name) {
        return PrometheusRegistry.defaultRegistry.scrape(name::equals).size() > 0;
    }
}
