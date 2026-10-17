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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.Test;

public class MetricsSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsTheIntervalOrDefaultsToFiveSeconds() throws Exception {
        assertThat(MetricsSettings.read(mapper.readTree("intervalSeconds: 10")).intervalSeconds()).isEqualTo(10);
        assertThat(MetricsSettings.read(mapper.readTree("workloads: {}").path("metrics")).intervalSeconds())
                .isEqualTo(5);
        assertThatThrownBy(() -> MetricsSettings.read(mapper.readTree("intervalSeconds: 0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metrics.intervalSeconds must be a whole number of seconds, 1 or more");
        assertThatThrownBy(() -> MetricsSettings.read(mapper.readTree("interval: 5")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metrics.interval isn't a setting");
    }

    @Test
    public void setsTheBrokersStatsPeriodsToTheIntervalUnlessTheScenarioSetsThem() {
        Map<String, String> env = new MetricsSettings(5).withBrokerStatsSettings(
                Map.of("managedLedgerStatsPeriodSeconds", "30", "PULSAR_MEM", "-Xmx1g"));

        assertThat(env)
                .containsEntry("statsUpdateFrequencyInSecs", "5")
                .containsEntry("statsUpdateInitialDelayInSecs", "5")
                .containsEntry("managedLedgerPrometheusStatsLatencyRolloverSeconds", "5")
                // The scenario's own value stays
                .containsEntry("managedLedgerStatsPeriodSeconds", "30")
                .containsEntry("PULSAR_MEM", "-Xmx1g");
        assertThat(new MetricsSettings(5).withBrokerStatsSettings(null)).hasSize(4);
        // bookkeeper.conf doesn't have the bookies' setting, which the prefix adds
        assertThat(new MetricsSettings(5).withBookieStatsSettings(Map.of("PULSAR_MEM", "-Xmx1g")))
                .containsEntry("PULSAR_PREFIX_prometheusStatsLatencyRolloverSeconds", "5")
                .containsEntry("PULSAR_MEM", "-Xmx1g");
    }

    @Test
    public void labelsTheRunByItsPathInTheReportsRoot() {
        Path root = Path.of("/home/user/pulsar/build/performance");

        assertThat(MetricsCollection.clusterLabel(root,
                root.resolve("2026-09-27/master/iot-telemetry/09-27-12-00-00")))
                .isEqualTo("2026-09-27/master/iot-telemetry/09-27-12-00-00");
        // A run written with --output outside the root
        assertThat(MetricsCollection.clusterLabel(root, Path.of("/tmp/my-run"))).isEqualTo("my-run");
    }
}
