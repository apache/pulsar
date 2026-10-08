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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Resolves the scenario files that the performance tests ship, as the launcher does, so that a broken inheritance, such
 * as a memory configuration that no longer reaches the cluster's settings, a ledger ensemble that the cluster's
 * bookies can't hold, or a workload that the gateways and the applications would reject, such as a timeout shorter than
 * the warmup and the measurement, fails here rather than in a run.
 */
public class ScenarioFilesTest {
    // The tests run in the launcher's project directory
    private static final Path SCENARIOS = Path.of("../scenarios").toAbsolutePath().normalize();
    private static final Path CONFIGS = SCENARIOS.resolve("configs");

    private final YamlScenarioLoader loader = new YamlScenarioLoader();

    @DataProvider
    public static Object[][] scenarios() throws IOException {
        assertThat(SCENARIOS).isDirectory();
        try (Stream<Path> files = Files.list(SCENARIOS)) {
            return files.filter(file -> file.getFileName().toString().endsWith(".yaml")).sorted()
                    .map(file -> new Object[] {file.getFileName().toString()}).toArray(Object[][]::new);
        }
    }

    @Test(dataProvider = "scenarios")
    public void resolvesToTheClusterAndTheMemoryOfEveryContainer(String scenario) {
        ObjectNode resolved = resolve(SCENARIOS.resolve(scenario), List.of());

        ClusterSettings cluster = ClusterSettings.read(loader.mapper(), resolved.path("cluster"));
        assertThat(cluster.brokers().env()).containsKey(PerformanceLauncher.PULSAR_MEM)
                .containsEntry("brokerDeduplicationEnabled", "true");
        assertThat(cluster.bookies().env()).containsKey(PerformanceLauncher.PULSAR_MEM)
                .containsEntry("gcWaitTime", "86400000");
        // The broker can create ledgers only when the bookies can hold their ensemble
        int ensembleSize = ledgerSetting(cluster, "managedLedgerDefaultEnsembleSize");
        int writeQuorum = ledgerSetting(cluster, "managedLedgerDefaultWriteQuorum");
        int ackQuorum = ledgerSetting(cluster, "managedLedgerDefaultAckQuorum");
        assertThat(ensembleSize).as("the ensemble size of %d bookie(s)", cluster.bookies().replicas())
                .isBetween(1, cluster.bookies().replicas());
        assertThat(writeQuorum).as("the write quorum").isBetween(1, ensembleSize);
        assertThat(ackQuorum).as("the ack quorum").isBetween(1, writeQuorum);
        JsonNode workload = loader.select(resolved, "workloads.iotTelemetry");
        for (String component : List.of("gateways", "applications")) {
            assertThat(workload.path(component).path("env").path(PerformanceLauncher.PULSAR_MEM).isTextual())
                    .as(component + ".env." + PerformanceLauncher.PULSAR_MEM).isTrue();
        }
        checkWorkload(resolved);
    }

    @Test(dataProvider = "scenarios")
    public void acceptsEveryConfigurationOnTopOfTheScenario(String scenario) throws IOException {
        // What --extends adds: a memory configuration or a profile, which must leave a valid workload
        try (Stream<Path> files = Files.list(CONFIGS)) {
            for (Path configuration : files.filter(file -> file.getFileName().toString().matches(
                    "(iot-telemetry-.*-mem|profile-.*)\\.yaml")).sorted().toList()) {
                checkWorkload(resolve(SCENARIOS.resolve(scenario), List.of(configuration)));
            }
        }
    }

    // The workload's settings as the launcher checks them before it starts a cluster, with the service URL it sets
    private void checkWorkload(ObjectNode resolved) {
        ObjectNode workload = (ObjectNode) loader.select(resolved, "workloads.iotTelemetry").deepCopy();
        workload.put("serviceUrl", "pulsar://broker:6650");
        PerformanceLauncher.checkWorkload(loader.mapper(), workload);
    }

    // A ledger replication setting of the brokers, or the default in Pulsar's conf/broker.conf, 2, when unset
    private static int ledgerSetting(ClusterSettings cluster, String name) {
        String value = cluster.brokers().env().get(name);
        return value != null ? Integer.parseInt(value) : 2;
    }

    @Test
    public void theCatchUpScenarioJoinsApplicationsAfterTheMeasurementStarts() {
        ObjectNode applications = (ObjectNode) resolve(SCENARIOS.resolve("iot-telemetry-catch-up.yaml"), List.of())
                .path("workloads").path("iotTelemetry").path("applications");
        assertThat(applications.path("joinSeconds").toString()).isEqualTo("[0,20,22,24,60]");
        assertThat(applications.path("count").asInt()).isEqualTo(5);
    }

    @Test
    public void theMemoryConfigurationsSetTheirOwnMemory() {
        // The configuration that a scenario extends last sets the memory, over the default of iot-telemetry-base.yaml
        String defaultMemory = brokerMemory(resolve(SCENARIOS.resolve("iot-telemetry.yaml"), List.of()));
        for (String configuration : List.of("iot-telemetry-low-mem.yaml", "iot-telemetry-high-mem.yaml")) {
            ObjectNode resolved = resolve(SCENARIOS.resolve("iot-telemetry.yaml"),
                    List.of(CONFIGS.resolve(configuration)));
            assertThat(brokerMemory(resolved)).as(configuration).isNotEqualTo(defaultMemory);
            assertThat(brokerMemory(resolved)).as(configuration)
                    .isEqualTo(brokerMemory(resolve(CONFIGS.resolve(configuration), List.of())));
        }
    }

    @Test
    public void theProfileConfigurationsProfileTheirComponent() {
        for (String component : List.of("broker", "gateways", "applications")) {
            ObjectNode resolved = resolve(SCENARIOS.resolve("iot-telemetry.yaml"),
                    List.of(CONFIGS.resolve("profile-" + component + ".yaml")));
            ProfilingSettings profiling = ProfilingSettings.read(loader.mapper(), resolved.path("profiling"));
            ProfilingSettings.Component profiled = switch (component) {
                case "broker" -> profiling.broker();
                case "gateways" -> profiling.gateways();
                default -> profiling.applications();
            };
            assertThat(profiled.profiled()).as(component).isTrue();
        }
    }

    private ObjectNode resolve(Path scenario, List<Path> appended) {
        return loader.resolve(scenario, appended, null, Map.of(), "PULSAR_PERFORMANCE_", "PULSAR_PERFORMANCE_CONFIG");
    }

    private String brokerMemory(ObjectNode resolved) {
        return resolved.path("cluster").path("brokers").path("env").path(PerformanceLauncher.PULSAR_MEM).asText();
    }
}
