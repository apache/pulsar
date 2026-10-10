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
package org.apache.pulsar.tests.performance.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class YamlScenarioLoaderTest {
    @Test
    public void resolvesRelativeParentsRemovalSelectionAndEnvironment() throws Exception {
        Path directory = Files.createTempDirectory("scenario-loader");
        Files.createDirectories(directory.resolve("parents"));
        Files.writeString(directory.resolve("base.yaml"), "workload:\n  rate: 100\n  removed: value\n");
        Files.writeString(directory.resolve("parents/child.yaml"), """
                extends: ../base.yaml
                workload:
                  removed: ~
                  clients: 10
                """);
        var loader = new YamlScenarioLoader();
        var resolved = loader.resolve(directory.resolve("parents/child.yaml"), null,
                Map.of("PERF_WORKLOAD_RATE", "200"), "PERF_", "PERF_CONFIG");

        assertThat(loader.select(resolved, "workload").get("rate").intValue()).isEqualTo(200);
        assertThat(loader.select(resolved, "workload").get("clients").intValue()).isEqualTo(10);
        assertThat(loader.select(resolved, "workload").has("removed")).isFalse();
    }

    @Test
    public void mergesAppendedScenariosLastAndBeforeTheEnvironment() throws Exception {
        Path directory = Files.createTempDirectory("scenario-append");
        Files.writeString(directory.resolve("scenario.yaml"), """
                workloads:
                  iotTelemetry:
                    rate: 1000
                profiling:
                  brokerOptions: ""
                """);
        Files.writeString(directory.resolve("profile-base.yaml"), "profiling:\n  retainOriginalRecording: true\n");
        Files.writeString(directory.resolve("profile-broker.yaml"), """
                extends: profile-base.yaml
                profiling:
                  brokerOptions: event=cpu
                workloads:
                  iotTelemetry:
                    rate: 2000
                """);
        var loader = new YamlScenarioLoader();
        var resolved = loader.resolve(directory.resolve("scenario.yaml"),
                List.of(directory.resolve("profile-broker.yaml")), null,
                Map.of("PERF_WORKLOADS_IOTTELEMETRY_RATE", "3000"), "PERF_", "PERF_CONFIG");

        assertThat(loader.select(resolved, "profiling").get("brokerOptions").textValue()).isEqualTo("event=cpu");
        assertThat(loader.select(resolved, "profiling").get("retainOriginalRecording").booleanValue()).isTrue();
        assertThat(loader.select(resolved, "workloads.iotTelemetry").get("rate").intValue()).isEqualTo(3000);
    }

    @Test
    public void setsValuesOfTheResolvedScenario() throws Exception {
        Path scenario = Files.createTempFile("scenario-set", ".yaml");
        Files.writeString(scenario, """
                workloads:
                  iotTelemetry:
                    rate: 1000
                    batchingEnabled: true
                    topicPrefix: persistent://public/default/iot-
                cluster:
                  brokers:
                    env:
                      PULSAR_MEM: -Xmx2g
                profiling:
                  offCpu:
                    reasons: [blocked]
                """);
        var loader = new YamlScenarioLoader();
        var resolved = loader.resolve(scenario, null, Map.of(), "PERF_", "PERF_CONFIG");

        loader.set(resolved, "workloads.iotTelemetry.rate=5000");
        loader.set(resolved, "WORKLOADS.iottelemetry.batchingEnabled=false");
        loader.set(resolved, "workloads.iotTelemetry.topicPrefix=persistent://tenant/ns/iot-");
        loader.set(resolved, "cluster.brokers.env.dispatcherMaxReadBatchSize=500");
        loader.set(resolved, "profiling.offCpu.reasons=[blocked, runnable]");

        var workload = loader.select(resolved, "workloads.iotTelemetry");
        assertThat(workload.get("rate").isIntegralNumber()).isTrue();
        assertThat(workload.get("rate").intValue()).isEqualTo(5000);
        assertThat(workload.get("batchingEnabled").booleanValue()).isFalse();
        assertThat(workload.get("topicPrefix").textValue()).isEqualTo("persistent://tenant/ns/iot-");
        assertThat(loader.select(resolved, "cluster.brokers.env").get("dispatcherMaxReadBatchSize").intValue())
                .isEqualTo(500);
        assertThat(loader.select(resolved, "profiling.offCpu").get("reasons").toString())
                .isEqualTo("[\"blocked\",\"runnable\"]");
        assertThatThrownBy(() -> loader.set(resolved, "workloads.iotTelemtry.rate=1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no section 'workloads.iotTelemtry'");
        assertThatThrownBy(() -> loader.set(resolved, "workloads.iotTelemetry.rate"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("<path>=<value>");
    }

    @Test
    public void matchesEnvironmentOverridesToKeysInAnyCase() throws Exception {
        Path scenario = Files.createTempFile("scenario-case", ".yaml");
        Files.writeString(scenario, """
                workloads:
                  iotTelemetry:
                    rate: 1000
                    clientsPerApplication: 10
                    payloadBytes: 64
                cluster:
                  brokers:
                    env:
                      dbStorage_writeCacheMaxSizeMb: "64"
                """);
        var loader = new YamlScenarioLoader();
        var resolved = loader.resolve(scenario, null, Map.of(
                "PERF_WORKLOADS_IOTTELEMETRY_RATE", "5000",
                "PERF_workloads_iotTelemetry_clientsPerApplication", "20",
                "perf_workloads_iottelemetry_payloadbytes", "128",
                "PERF_CLUSTER_BROKERS_ENV_DBSTORAGE_WRITECACHEMAXSIZEMB", "128",
                // A prefix in neither upper nor lower case isn't an override
                "Perf_workloads_iotTelemetry_rate", "1",
                "perf_config", "ignored"), "PERF_", "PERF_CONFIG");

        var workload = loader.select(resolved, "workloads.iotTelemetry");
        assertThat(workload.get("rate").intValue()).isEqualTo(5000);
        assertThat(workload.get("clientsPerApplication").intValue()).isEqualTo(20);
        assertThat(workload.get("payloadBytes").intValue()).isEqualTo(128);
        assertThat(loader.select(resolved, "cluster.brokers.env").get("dbStorage_writeCacheMaxSizeMb").textValue())
                .isEqualTo("128");
    }
}
