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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class RemoteApplicationsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void givesTheRemoteApplicationsTheRemoteClusterAndTheirSettings() throws Exception {
        ObjectNode workload = (ObjectNode) mapper.readTree("""
                serviceUrl: pulsar://iot-1-pulsar-broker-0:6650
                rate: 1000
                topics:
                  prefix: persistent://public/default/iot-telemetry-
                applications:
                  count: 20
                  podsPerApplication: 10
                  joinSeconds: [0, 30]
                  skipMessages: [0, 100]
                  unloadBeforeJoin: true
                  env:
                    PULSAR_MEM: "-Xmx1g"
                """);
        ClusterSettings.Remote remote = remote("""
                  applications:
                    count: 4
                    env:
                      PULSAR_MEM: "-Xmx256m"
                """);

        ObjectNode remoteWorkload = PerformanceLauncher.remoteApplicationsWorkload(workload, remote,
                remote.serviceUrl("iot-1"));

        assertThat(remoteWorkload.path("serviceUrl").asText()).isEqualTo("pulsar://iot-1-remote-pulsar-broker-0:6650");
        assertThat(remoteWorkload.path("rate").asInt()).isEqualTo(1000);
        assertThat(remoteWorkload.path("applications").path("count").asInt()).isEqualTo(4);
        assertThat(remoteWorkload.path("applications").path("podsPerApplication").asInt()).isEqualTo(10);
        assertThat(remoteWorkload.path("applications").has("joinSeconds")).isFalse();
        // which the join preparation needs
        assertThat(remoteWorkload.path("applications").has("skipMessages")).isFalse();
        assertThat(remoteWorkload.path("applications").has("unloadBeforeJoin")).isFalse();
        // The workload itself is unchanged
        assertThat(workload.path("applications").path("count").asInt()).isEqualTo(20);
        assertThat(PerformanceLauncher.remoteApplicationsEnv(Map.of("PULSAR_MEM", "-Xmx1g"), remote))
                .containsExactly(Map.entry("PULSAR_MEM", "-Xmx256m"));
    }

    @Test
    public void runsTheWorkloadsApplicationsWithoutSettingsOfTheirOwn() throws Exception {
        ObjectNode workload = (ObjectNode) mapper.readTree("""
                topics:
                  prefix: persistent://public/default/iot-telemetry-
                applications:
                  count: 20
                  podsPerApplication: 10
                """);
        ClusterSettings.Remote remote = remote("  {}\n");

        ObjectNode remoteWorkload = PerformanceLauncher.remoteApplicationsWorkload(workload, remote, "pulsar://x");

        assertThat(remoteWorkload.path("applications").path("count").asInt()).isEqualTo(20);
        assertThat(PerformanceLauncher.remoteApplicationsEnv(Map.of("PULSAR_MEM", "-Xmx1g"), remote))
                .containsExactly(Map.entry("PULSAR_MEM", "-Xmx1g"));
    }

    @Test
    public void rejectsTopicsOutsideTheReplicatedNamespace() throws Exception {
        ObjectNode workload = (ObjectNode) mapper.readTree("""
                topics:
                  prefix: persistent://public/other/iot-telemetry-
                applications:
                  count: 20
                """);

        assertThatThrownBy(() -> PerformanceLauncher.remoteApplicationsWorkload(workload, remote("  {}\n"),
                "pulsar://x")).hasMessageContaining("must be persistent topics in it");
    }

    @Test
    public void namesTheContainersOfBothClustersWithoutTheClusters() {
        assertThat(PerformanceLauncher.componentName("/iot-1-pulsar-broker-0", "iot-1")).isEqualTo("broker-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-iot-1-pulsar-broker-0", "iot-1")).isEqualTo("broker-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-remote-iot-1-remote-pulsar-broker-1", "iot-1-remote"))
                .isEqualTo("broker-1");
        assertThat(PerformanceLauncher.componentName("/iot-1-remote-bookie-0", "iot-1-remote")).isEqualTo("bookie-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-zookeeper", "iot-1")).isEqualTo("zookeeper");
        assertThat(PerformanceLauncher.componentName("/iot-1-iot-1-pulsar-bookie-0", "iot-1")).isEqualTo("bookie-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-remote-iot-1-remote-zookeeper", "iot-1-remote"))
                .isEqualTo("zookeeper");
    }

    @Test
    public void namesEveryContainerThatTheEndpointsExpandTo() {
        // The containers as the clusters name them: with a shared configuration store, the name of a broker, a bookie
        // and ZooKeeper repeats its cluster's
        Set<String> launcherNames = new TreeSet<>();
        for (String container : List.of("/iot-1-iot-1-pulsar-broker-0", "/iot-1-iot-1-pulsar-broker-1",
                "/iot-1-iot-1-pulsar-bookie-0", "/iot-1-iot-1-pulsar-bookie-1", "/iot-1-iot-1-pulsar-bookie-2",
                "/iot-1-iot-1-zookeeper")) {
            launcherNames.add(PerformanceLauncher.componentName(container, "iot-1"));
        }
        for (String container : List.of("/iot-1-remote-iot-1-remote-pulsar-broker-0",
                "/iot-1-remote-iot-1-remote-pulsar-bookie-0", "/iot-1-remote-iot-1-remote-pulsar-bookie-1",
                "/iot-1-remote-iot-1-remote-zookeeper")) {
            launcherNames.add(NetworkSettings.REMOTE_PREFIX + PerformanceLauncher.componentName(container,
                    "iot-1-remote"));
        }
        // The workloads and the configuration store, as networkEndpoints names them
        launcherNames.addAll(List.of(NetworkSettings.GATEWAYS, NetworkSettings.APPLICATIONS,
                NetworkSettings.CONFIGURATION_STORE, PerformanceLauncher.REMOTE_APPLICATIONS_DIRECTORY));

        Set<String> endpointNames = new TreeSet<>(NetworkSettings.containers(List.of("local-region",
                "remote-region"), new NetworkSettings.Topology(2, 3, 1, 2)));

        assertThat(launcherNames).isEqualTo(endpointNames);
    }

    private ClusterSettings.Remote remote(String section) throws Exception {
        return ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 1
                bookies:
                  replicas: 1
                remote:
                """ + section)).remote();
    }
}
