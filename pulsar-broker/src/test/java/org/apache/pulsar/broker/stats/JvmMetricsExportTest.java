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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Multimap;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class JvmMetricsExportTest extends SharedPulsarBaseTest {

    /** The JVM families whose name differs between the simpleclient hotspot exports and the 1.x JvmMetrics. */
    private static final Map<String, String> RENAMED = Map.of(
            "jvm_memory_bytes_used", "jvm_memory_used_bytes",
            "jvm_memory_bytes_committed", "jvm_memory_committed_bytes",
            "jvm_memory_bytes_max", "jvm_memory_max_bytes",
            "jvm_memory_bytes_init", "jvm_memory_init_bytes",
            "jvm_memory_pool_bytes_used", "jvm_memory_pool_used_bytes",
            "jvm_memory_pool_bytes_committed", "jvm_memory_pool_committed_bytes",
            "jvm_memory_pool_bytes_max", "jvm_memory_pool_max_bytes",
            "jvm_memory_pool_bytes_init", "jvm_memory_pool_init_bytes",
            "jvm_info", "jvm_runtime_info");

    @Test
    public void testBrokerExportsTheJvmMetricsUnderThe1xNames() throws Exception {
        String metrics = scrape();

        RENAMED.forEach((oldName, newName) -> assertThat(metrics)
                .as("broker /metrics declares %s and not %s", newName, oldName)
                .contains("# TYPE " + newName + " ")
                .doesNotContain("# TYPE " + oldName + " "));
        assertThat(metrics)
                .as("the Netty direct memory gauges keep their names")
                .contains("# TYPE jvm_memory_direct_bytes_used gauge\n")
                .contains("# TYPE jvm_memory_direct_bytes_max gauge\n");
    }

    @Test
    public void testBrokerExportsTheNettyDirectMemoryGauges() throws Exception {
        Multimap<String, Metric> metrics = parseMetrics(scrape());
        assertThat(metrics.get("jvm_memory_direct_bytes_max")).singleElement()
                .satisfies(max -> assertThat(max.value).isEqualTo(DirectMemoryUtils.jvmMaxDirectMemory()));
        assertThat(metrics.get("jvm_memory_direct_bytes_used")).singleElement()
                .satisfies(used -> assertThat(used.value).as("-1 means the JVM cannot estimate it")
                        .isGreaterThanOrEqualTo(-1));
        assertRegisteredInThe1xRegistryOnly("jvm_memory_direct_bytes_max");
        assertRegisteredInThe1xRegistryOnly("jvm_memory_direct_bytes_used");
    }

    @Test
    public void testBrokerExportsItsVersionAndCommit() throws Exception {
        assertThat(parseMetrics(scrape()).get("pulsar_version_info")).singleElement().satisfies(versionInfo -> {
            assertThat(versionInfo.tags)
                    .containsEntry("version", PulsarVersion.getVersion())
                    .containsEntry("commit", PulsarVersion.getGitSha());
            assertThat(versionInfo.value).isEqualTo(1.0);
        });
        assertRegisteredInThe1xRegistryOnly("pulsar_version_info");
    }

    /**
     * The broker /metrics writes the simpleclient registry and the 1.x registry, so the scrape alone does not tell
     * in which of them a family is registered.
     */
    private static void assertRegisteredInThe1xRegistryOnly(String family) {
        assertThat(PrometheusRegistry.defaultRegistry.scrape(family::equals))
                .as("%s in the 1.x registry", family)
                .hasSize(1);
        assertThat(Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples()))
                .as("families in the simpleclient registry")
                .extracting(legacyFamily -> legacyFamily.name)
                .doesNotContain(family);
    }

    private String scrape() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(getPulsar(), false, false, false, out);
        return out.toString(StandardCharsets.UTF_8);
    }
}
