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
package org.apache.pulsar.tests.performance.tools;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class IotScenarioTest {
    @Test
    public void directCommandsDefaultCoordinationDirectoryAndRequireRunIdOnlyForWarmup() throws Exception {
        Path config = Files.createTempFile("iot-scenario", ".yaml");
        try {
            YamlScenarioLoader loader = new YamlScenarioLoader();
            for (String command : new String[] {"iot-produce", "iot-consume"}) {
                List<String> arguments = new ArrayList<>(List.of(command, "--config", config.toString(),
                        "--output", "results"));
                var parsed = new CommandLine(new PerformanceTool()).parseArgs(arguments.toArray(String[]::new));
                var tool = (PerformanceTool.ScenarioCommand) parsed.subcommand().commandSpec().userObject();
                assertThat(tool.coordinationDirectory()).isEqualTo(Path.of("results/coordination"));
                loader.mapper().writeValue(config.toFile(),
                        Map.of("workloads", Map.of("iotTelemetry", scenario(0, 0, 0, 100))));
                assertThat(tool.scenario().warmupMessageCount()).isZero();

                loader.mapper().writeValue(config.toFile(),
                        Map.of("workloads", Map.of("iotTelemetry", scenario(0, 10, 0, 100))));
                assertThatThrownBy(tool::scenario).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("--run-id");
                arguments.addAll(List.of("--run-id", "shared-run", "--coordination-directory", "shared"));
                parsed = new CommandLine(new PerformanceTool()).parseArgs(arguments.toArray(String[]::new));
                tool = (PerformanceTool.ScenarioCommand) parsed.subcommand().commandSpec().userObject();
                assertThat(tool.scenario().warmupMessageCount()).isEqualTo(10);
                assertThat(tool.runId).isEqualTo("shared-run");
                assertThat(tool.coordinationDirectory()).isEqualTo(Path.of("shared"));
            }
        } finally {
            Files.deleteIfExists(config);
        }
    }

    @Test
    public void calculatesRateLimitedWarmupAndMeasurementCounts() {
        IotScenario scenario = scenario(20, 0, 1000, 0);

        assertThat(scenario.warmupMessageCount()).isEqualTo(20_000);
        assertThat(scenario.measurementMessageCount()).isEqualTo(120_000);
        assertThat(scenario.messageCount()).isEqualTo(140_000);
    }

    @Test
    public void calculatesUnrestrictedWarmupAndMeasurementCounts() {
        IotScenario scenario = scenario(0, 1_000_000, 0, 5_000_000);

        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000_000);
        assertThat(scenario.measurementMessageCount()).isEqualTo(5_000_000);
        assertThat(scenario.messageCount()).isEqualTo(6_000_000);
    }

    @Test
    public void calculatesMultipleWarmupRounds() {
        IotScenario scenario = scenario(0, 500_000, 2, 5, 0, 5_000_000);

        assertThat(scenario.warmupMessageCountPerRound()).isEqualTo(500_000);
        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000_000);
        assertThat(scenario.messageCount()).isEqualTo(6_000_000);
        assertThat(scenario.warmupRoundDelaySeconds()).isEqualTo(5);
    }

    @Test
    public void defaultsMissingWarmupRoundsToOne() {
        IotScenario scenario = scenario(0, 1_000, 0, 0, 0, 5_000);

        assertThat(scenario.warmupRounds()).isEqualTo(1);
        assertThat(scenario.warmupMessageCount()).isEqualTo(1_000);
    }

    @Test
    public void rejectsTimeBasedWarmupWithoutRateLimit() {
        assertThatThrownBy(() -> scenario(20, 0, 0, 5_000_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void defaultsAnOmittedPrecreateConcurrency() {
        assertThat(new IotScenario.Producer(2, 2, 100, true, true, null).precreateConcurrency())
                .isEqualTo(IotScenario.Producer.DEFAULT_PRECREATE_CONCURRENCY);
    }

    @Test
    public void rejectsPrecreateConcurrencyBelowOne() {
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, 0), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null), null, 300))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gateways.producer.precreateConcurrency must be at least 1");
    }

    @Test
    public void rejectsTwoWarmupLimits() {
        assertThatThrownBy(() -> scenario(20, 1_000, 1000, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void includesRateLimitedMessageWarmupInMinimumRuntime() {
        // 11 s for each of the 3 warmup rounds of 1,001 messages at 100 msg/s, 2 s between them and 120 s measured
        assertThatThrownBy(() -> scenario(0, 1_001, 3, 2, 100, 1_000, 158))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeoutSeconds is 158, but the workload needs 159 s: 39 s of warmup"
                        + " (3 round(s) of 11 s and a 2 s delay after each) and 120 s of measurement at 100 msg/s")
                .hasMessageContaining("set timeoutSeconds to at least 159, and better about 259");
    }

    private static IotScenario scenario(int warmupSeconds, long warmupMessages, int rate, long numberOfMessages) {
        return scenario(warmupSeconds, warmupMessages, 1, 0, rate, numberOfMessages);
    }

    private static IotScenario scenario(int warmupSeconds, long warmupMessages, int warmupRounds,
                                        int warmupRoundDelaySeconds, int rate, long numberOfMessages) {
        return scenario(warmupSeconds, warmupMessages, warmupRounds, warmupRoundDelaySeconds, rate, numberOfMessages,
                300);
    }

    private static IotScenario scenario(int warmupSeconds, long warmupMessages, int warmupRounds,
                                        int warmupRoundDelaySeconds, int rate, long numberOfMessages,
                                        int timeoutSeconds) {
        return new IotScenario("pulsar://localhost:6650",
                new IotScenario.Warmup(warmupSeconds, warmupMessages, warmupRounds, warmupRoundDelaySeconds),
                new IotScenario.Measurement(120, numberOfMessages), rate, new IotScenario.Payload(64),
                new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, 32), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null), null, timeoutSeconds);
    }
}
