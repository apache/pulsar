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
package org.apache.pulsar.tests.integration.profiling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarProfilingConfigTest {
    @Test
    public void loadsYamlAndEnvironmentOverrides() throws Exception {
        Path file = Files.createTempFile("pulsar-profiling", ".yaml");
        try {
            Files.writeString(file, "load:\n  messageSize: 256\noutput:\n  directory: custom-output\n");
            PulsarProfilingConfig.Config config = PulsarProfilingConfig.Config.read(file, Map.of(
                    "PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES", "1234",
                    "PULSAR_PROFILING_CLUSTER_NUM_BOOKIES", "5"));

            assertThat(config.load().messageSize()).isEqualTo(256);
            assertThat(config.load().numberOfMessages()).isEqualTo(1234);
            assertThat(config.cluster().numBookies()).isEqualTo(5);
            assertThat(config.output().directory()).isEqualTo("custom-output");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void configuresIsolatedProducersAndExclusiveConsumer() {
        var config = PulsarProfilingConfig.Config.read(null, Map.of(
                "PULSAR_PROFILING_LOAD_PRODUCER_COUNT", "500",
                "PULSAR_PROFILING_LOAD_ISOLATED_PRODUCERS", "500",
                "PULSAR_PROFILING_LOAD_PRODUCER_IO_THREADS", "8",
                "PULSAR_PROFILING_LOAD_SUBSCRIPTION_TYPE", "Exclusive",
                "PULSAR_PROFILING_LOAD_TIMEOUT_SECONDS", "480"));
        assertThat(config.load().producerCount()).isEqualTo(500);
        assertThat(config.load().isolatedProducers()).isEqualTo(500);
        assertThat(config.load().producerIoThreads()).isEqualTo(8);
        assertThat(config.load().subscriptionType()).isEqualTo(SubscriptionType.Exclusive);
        assertThat(config.load().consumerCount()).isEqualTo(1);
        assertThat(config.load().timeoutSeconds()).isEqualTo(480);
    }

    @Test
    public void inheritsIndependentClientProfilingAndBatchingOptions() throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            files.write("clients.yaml", """
                    profiling:
                      producerOptions: event=cpu,interval=10ms
                      consumerOptions: event=cpu,lock=0
                    load:
                      batchingEnabled: true
                    """);
            Path child = files.write("scenario.yaml", """
                    extends: clients.yaml
                    profiling:
                      consumerOptions: ~
                    """);
            var config = PulsarProfilingConfig.Config.read(child, Map.of(
                    "PULSAR_PROFILING_PROFILING_PRODUCER_OPTIONS", "event=cpu,interval=20ms",
                    "PULSAR_PROFILING_LOAD_BATCHING_ENABLED", "false"));
            assertThat(config.profiling().producerOptions()).isEqualTo("event=cpu,interval=20ms");
            assertThat(config.profiling().consumerOptions()).isNull();
            assertThat(config.load().batchingEnabled()).isFalse();
            assertThat(PulsarProfilingConfig.Config.defaults().profiling().producerOptions()).isEmpty();
            assertThat(PulsarProfilingConfig.Config.defaults().profiling().consumerOptions()).isEmpty();
        }
    }

    @Test
    public void rejectsWorkloadsThatCannotFinish() {
        assertThatThrownBy(() -> PulsarProfilingConfig.Config.read(null,
                Map.of("PULSAR_PROFILING_LOAD_ISOLATED_PRODUCERS", "500")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarProfilingConfig.Config.read(null, Map.of(
                "PULSAR_PROFILING_LOAD_PRODUCER_COUNT", "500",
                "PULSAR_PROFILING_LOAD_ISOLATED_PRODUCERS", "500",
                "PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES", "501")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PulsarProfilingConfig.Config.read(null, Map.of(
                "PULSAR_PROFILING_LOAD_SUBSCRIPTION_TYPE", "Exclusive",
                "PULSAR_PROFILING_LOAD_CONSUMER_COUNT", "2")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void mergesRecursiveParentsInOrderBeforeChildAndEnvironment() throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            files.write("common.yaml", """
                    load:
                      messageSize: 256
                      produceRate: 1000
                    cluster:
                      numBookies: 5
                    """);
            files.write("parents/first.yaml", """
                    extends: ../common.yaml
                    load:
                      messageSize: 512
                      producerCount: 3
                    """);
            files.write("parents/second.yaml", """
                    load:
                      messageSize: 1024
                      produceRate: 2000
                    """);
            Path child = files.write("scenario.yaml", """
                    extends: [parents/first.yaml, parents/second.yaml]
                    load:
                      produceRate: 3000
                    """);
            var config = PulsarProfilingConfig.Config.read(child, Map.of(
                    "PULSAR_PROFILING_CLUSTER_NUM_BOOKIES", "7"));
            assertThat(config.load().messageSize()).isEqualTo(1024);
            assertThat(config.load().produceRate()).isEqualTo(3000);
            assertThat(config.load().producerCount()).isEqualTo(3);
            assertThat(config.cluster().numBookies()).isEqualTo(7);
            assertThat(config.load().consumerCount()).isEqualTo(1);
        }
    }

    @Test
    public void removesInheritedAndDefaultEntriesIncludingThroughRecursiveParents() throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            files.write("base.yaml", """
                    cluster:
                      brokerEnvs:
                        customSetting: enabled
                        keptSetting: kept
                        restoredSetting: old
                    """);
            files.write("removals.yaml", """
                    cluster:
                      brokerEnvs:
                        customSetting: ~
                        preciseDispatcherFlowControl: null
                        restoredSetting: ~
                        absentSetting: ~
                      bookkeeperEnvs: ~
                    """);
            files.write("parent.yaml", "extends: removals.yaml\n");
            Path child = files.write("scenario.yaml", """
                    extends: [base.yaml, parent.yaml]
                    cluster:
                      brokerEnvs:
                        restoredSetting: new
                      bookkeeperEnvs:
                        journalSyncData: "true"
                        absentSetting: ~
                    """);
            var config = PulsarProfilingConfig.Config.read(child, Map.of());
            assertThat(config.cluster().brokerEnvs())
                    .doesNotContainKeys("customSetting", "preciseDispatcherFlowControl", "absentSetting")
                    .containsEntry("keptSetting", "kept")
                    .containsEntry("restoredSetting", "new")
                    .containsEntry("managedLedgerDefaultEnsembleSize", "1");
            assertThat(config.cluster().bookkeeperEnvs()).containsExactlyEntriesOf(Map.of("journalSyncData", "true"));
        }
    }

    @Test
    public void allowsSharedAncestorsWithoutTreatingThemAsCycles() throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            files.write("common.yaml", "load:\n  messageSize: 256\n");
            files.write("first.yaml", "extends: common.yaml\nload:\n  messageSize: 512\n");
            files.write("second.yaml", "extends: common.yaml\n");
            Path child = files.write("scenario.yaml", "extends: [first.yaml, second.yaml]\n");
            var config = PulsarProfilingConfig.Config.read(child, Map.of());
            // The second parent's ancestry is applied too, in declaration order.
            assertThat(config.load().messageSize()).isEqualTo(256);
        }
    }

    @Test
    public void rejectsInheritanceCycles() throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            Path first = files.write("first.yaml", "extends: second.yaml\n");
            files.write("second.yaml", "extends: ./first.yaml\n");
            assertThatThrownBy(() -> PulsarProfilingConfig.Config.read(first, Map.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("inheritance cycle")
                    .hasMessageContaining("first.yaml")
                    .hasMessageContaining("second.yaml");
        }
    }

    @DataProvider
    public Object[][] invalidInheritance() {
        return new Object[][] {
            {"extends: 42\n"},
            {"extends: ~\n"},
            {"extends: {}\n"},
            {"extends: ''\n"},
            {"extends: [42]\n"},
            {"extends: [~]\n"},
            {"extends: missing.yaml\n"},
            {"- not-a-mapping\n"},
            {""}
        };
    }

    @Test(dataProvider = "invalidInheritance")
    public void rejectsInvalidInheritance(String yaml) throws Exception {
        try (ScenarioFiles files = new ScenarioFiles()) {
            Path child = files.write("scenario.yaml", yaml);
            assertThatThrownBy(() -> PulsarProfilingConfig.Config.read(child, Map.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("config");
        }
    }

    @Test
    public void sharedScenarioOnlyChangesSubscriptionTypeAndOutputDirectory() {
        Path scenarios = Path.of("../performance/scenarios");
        var exclusive = PulsarProfilingConfig.Config.read(
                scenarios.resolve("read-completion-isolation.yaml"), Map.of());
        var shared = PulsarProfilingConfig.Config.read(scenarios.resolve("read-completion-isolation-shared.yaml"),
                Map.of());
        assertThat(shared.cluster()).isEqualTo(exclusive.cluster());
        assertThat(shared.load()).usingRecursiveComparison().ignoringFields("subscriptionType")
                .isEqualTo(exclusive.load());
        assertThat(shared.load().subscriptionType()).isEqualTo(SubscriptionType.Shared);
        assertThat(shared.output().directory()).isEqualTo("build/pulsar-profiling/read-completion-isolation-shared");
    }

    private static final class ScenarioFiles implements AutoCloseable {
        private final Path directory;

        private ScenarioFiles() throws IOException {
            directory = Files.createTempDirectory("pulsar-profiling-config");
        }

        private Path write(String name, String yaml) throws IOException {
            Path path = directory.resolve(name);
            Files.createDirectories(path.getParent());
            return Files.writeString(path, yaml);
        }

        @Override
        public void close() throws IOException {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }
}
