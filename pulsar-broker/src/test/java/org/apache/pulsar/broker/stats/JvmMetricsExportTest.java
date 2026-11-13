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

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(getPulsar(), false, false, false, out);
        String metrics = out.toString(StandardCharsets.UTF_8);

        RENAMED.forEach((oldName, newName) -> assertThat(metrics)
                .as("broker /metrics declares %s and not %s", newName, oldName)
                .contains("# TYPE " + newName + " ")
                .doesNotContain("# TYPE " + oldName + " "));
        assertThat(metrics)
                .as("the Netty direct memory gauges keep their names")
                .contains("# TYPE jvm_memory_direct_bytes_used gauge\n")
                .contains("# TYPE jvm_memory_direct_bytes_max gauge\n");
    }
}
