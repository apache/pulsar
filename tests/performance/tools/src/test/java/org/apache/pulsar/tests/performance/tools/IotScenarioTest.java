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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionType;
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
    public void applicationsThatJoinLaterRequireARunId() throws Exception {
        Path config = Files.createTempFile("iot-scenario", ".yaml");
        try {
            YamlScenarioLoader loader = new YamlScenarioLoader();
            loader.mapper().writeValue(config.toFile(), Map.of("workloads", Map.of("iotTelemetry",
                    scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                            List.of(0, 20), null)))));
            List<String> arguments = new ArrayList<>(List.of("iot-consume", "--config", config.toString(),
                    "--output", "results"));
            var parsed = new CommandLine(new PerformanceTool()).parseArgs(arguments.toArray(String[]::new));
            var tool = (PerformanceTool.ScenarioCommand) parsed.subcommand().commandSpec().userObject();
            assertThatThrownBy(tool::scenario).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("join later require the same --run-id");
            arguments.addAll(List.of("--run-id", "shared-run"));
            parsed = new CommandLine(new PerformanceTool()).parseArgs(arguments.toArray(String[]::new));
            tool = (PerformanceTool.ScenarioCommand) parsed.subcommand().commandSpec().userObject();
            assertThat(tool.scenario().joinSeconds(1)).isEqualTo(20);
        } finally {
            Files.deleteIfExists(config);
        }
    }

    @Test
    public void joinsApplicationsLater() {
        IotScenario scenario = scenario(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 20, 20), null));
        assertThat(scenario.joinSeconds(0)).isZero();
        assertThat(scenario.joinSeconds(1)).isEqualTo(20);
        assertThat(scenario.hasLateApplications()).isTrue();
        assertThat(scenario.applications().caughtUpLatencyMillis())
                .isEqualTo(IotScenario.Applications.DEFAULT_CAUGHT_UP_LATENCY_MILLIS);
        // without the setting, every application joins at the start
        IotScenario atStart = scenario(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2), null));
        assertThat(atStart.joinSeconds(2)).isZero();
        assertThat(atStart.hasLateApplications()).isFalse();
    }

    @Test
    public void preparesTheJoinOfApplicationsThatSkipMessages() {
        IotScenario scenario = unbatched(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 30, 20), null, List.of(0L, 100L, 0L), true));
        assertThat(scenario.skipMessages(1)).isEqualTo(100);
        assertThat(scenario.skipMessages(2)).isZero();
        // 100 of each of the 2 topics
        assertThat(scenario.expectedMessageCount(1)).isEqualTo(800);
        assertThat(scenario.expectedMessageCount(2)).isEqualTo(1_000);
        assertThat(scenario.preparesJoin()).isTrue();
        assertThat(scenario.earliestLateJoinSeconds()).isEqualTo(20);
        IotScenario withoutPreparation = scenario(new IotScenario.Applications(3, 2, "app-",
                new IotScenario.Client(2, 2), null, List.of(0, 30, 20), null));
        assertThat(withoutPreparation.skipMessages(1)).isZero();
        assertThat(withoutPreparation.expectedMessageCount(1)).isEqualTo(1_000);
        assertThat(withoutPreparation.preparesJoin()).isFalse();
    }

    @Test
    public void rejectsInvalidJoinPreparations() {
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null, List.of(10L), false)))
                .hasMessageContaining("skipMessages must have a value for each of the 2 applications");
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null, List.of(0L, -1L), false))).hasMessageContaining("must be at least 0");
        // an application that receives from the start can't skip
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null, List.of(10L, 10L), false)))
                .hasMessageContaining("only an application that joins later");
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, List.of(0L, 10L), false))).hasMessageContaining("only an application that joins");
        // 500 of each of the 2 topics are all of the 1,000 messages
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null, List.of(0L, 500L), false))).hasMessageContaining("must leave messages");
        // the launcher skips entries, which are messages only without batching
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null, List.of(0L, 10L), false)))
                .hasMessageContaining("needs gateways.producer.batchingEnabled: false");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 0), null, null, true))).hasMessageContaining("unloadBeforeJoin needs an application");
    }

    @Test
    public void resetsTheSubscriptionsOfLateApplications() {
        IotScenario scenario = unbatched(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 30, 30), null, null, false, List.of(-1L, 5_000L, -1L)));
        assertThat(scenario.resetToMillis(0)).isEqualTo(-1);
        assertThat(scenario.resetToMillis(1)).isEqualTo(5_000);
        assertThat(scenario.resetsSubscriptions()).isTrue();
        assertThat(scenario.preparesJoin()).isTrue();
        assertThat(scenario.startsAtFirstSequence(1)).isTrue();
        assertThat(scenario.startsAtFirstSequence(2)).isFalse();
        // only a late application resets, unbatched, and doesn't also skip
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, null, false, List.of(5_000L, -1L))))
                .hasMessageContaining("only an application that joins later");
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, List.of(0L, 10L), false, List.of(-1L, 5_000L))))
                .hasMessageContaining("can't also skip messages");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, null, false, List.of(-1L, 5_000L))))
                .hasMessageContaining("resetToMillis needs gateways.producer.batchingEnabled: false");
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, null, false, List.of(-2L, 5_000L))))
                .hasMessageContaining("-1 for none, or at least 0");
    }

    @Test
    public void choosesTheSubscriptionType() {
        IotScenario keyShared = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null));
        assertThat(keyShared.subscriptionType()).isEqualTo(SubscriptionType.Key_Shared);
        assertThat(keyShared.keepsDeviceOrder()).isTrue();
        IotScenario shared = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                null, null, null, false, null, "Shared"));
        assertThat(shared.subscriptionType()).isEqualTo(SubscriptionType.Shared);
        assertThat(shared.keepsDeviceOrder()).isFalse();
        assertThat(scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, null,
                null, null, false, null, "Failover")).keepsDeviceOrder()).isTrue();
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, null, false, null, "Exclusive")))
                .hasMessageContaining("applications.subscriptionType must be one of [Key_Shared, Shared, Failover]");
        // skipping and resetting expect each device's messages from the first one received, in order
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, List.of(0L, 10L), false, null, "Shared")))
                .hasMessageContaining("need a subscription type that keeps each device's order");
        assertThatThrownBy(() -> unbatched(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, null, false, List.of(-1L, 5_000L), "Shared")))
                .hasMessageContaining("need a subscription type that keeps each device's order");
        // a scenario file without the setting keeps Key_Shared
        IotScenario.Applications fromYaml = new YamlScenarioLoader().mapper().convertValue(Map.of("count", 2,
                "podsPerApplication", 2, "subscriptionPrefix", "app-", "client",
                Map.of("ioThreads", 2, "listenerThreads", 2)), IotScenario.Applications.class);
        assertThat(scenario(fromYaml).subscriptionType()).isEqualTo(SubscriptionType.Key_Shared);
    }

    @Test
    public void partitionsTheTopics() {
        IotScenario plain = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null));
        assertThat(plain.topics().partitioned()).isFalse();
        assertThat(plain.statsTopicNames()).containsExactly("persistent://public/default/iot-0",
                "persistent://public/default/iot-1");
        IotScenario partitioned = withTopics(new IotScenario.Topics(2, "persistent://public/default/iot-", 2),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null));
        assertThat(partitioned.topicNames()).containsExactly("persistent://public/default/iot-0",
                "persistent://public/default/iot-1");
        assertThat(partitioned.statsTopicNames()).containsExactly("persistent://public/default/iot-0-partition-0",
                "persistent://public/default/iot-0-partition-1", "persistent://public/default/iot-1-partition-0",
                "persistent://public/default/iot-1-partition-1");
        assertThatThrownBy(() -> withTopics(new IotScenario.Topics(2, "persistent://public/default/iot-", -1),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null)))
                .hasMessageContaining("topics.partitions must not be negative");
        // the launcher prepares a join with each topic's stats
        assertThatThrownBy(() -> withTopics(new IotScenario.Topics(2, "persistent://public/default/iot-", 2),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null,
                        null, true)))
                .hasMessageContaining("topics.partitions doesn't support");
        assertThatThrownBy(() -> withTopics(new IotScenario.Topics(2, "persistent://public/default/iot-", 2),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null)))
                .hasMessageContaining("topics.partitions doesn't support applications.joinSeconds");
        // a scenario file without the setting keeps non-partitioned topics
        IotScenario.Topics fromYaml = new YamlScenarioLoader().mapper().convertValue(Map.of("count", 2,
                "prefix", "persistent://public/default/iot-"), IotScenario.Topics.class);
        assertThat(fromYaml.partitions()).isZero();
    }

    @Test
    public void keepsTheLatestStateOfASubscription() {
        IotScenario latest = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                null, null, null, false, null, null, "latestState"));
        assertThat(latest.applications().keepsLatestState()).isTrue();
        assertThat(latest.applications().readsCompacted()).isFalse();
        assertThat(latest.handsOverFinalSequences()).isTrue();
        // a redelivered older message would look like a state going back
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, null, false, null, "Shared", "latestState")))
                .hasMessageContaining("applications.mode latestState needs a subscription type that keeps each"
                        + " device's order");
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(new IotScenario.PodRestarts(30, 0.5)),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, null, null, null,
                        false, null, null, "latestState")))
                .hasMessageContaining("doesn't support behaviors.podRestarts");
    }

    @Test
    public void subscribesByPattern() {
        IotScenario.Topics byPattern = new IotScenario.Topics(2, "persistent://public/default/iot-site-", null, 30);
        assertThat(byPattern.subscribedByPattern()).isTrue();
        assertThat(byPattern.pattern().matcher("persistent://public/default/iot-site-17").matches()).isTrue();
        assertThat(byPattern.pattern().matcher("persistent://public/default/iot-site-app-0-DLQ").matches()).isFalse();
        assertThat(withTopics(byPattern, new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null)).topics().patternDiscoverySeconds()).isEqualTo(30);
        assertThatThrownBy(() -> withTopics(new IotScenario.Topics(2, "persistent://public/default/iot.site-", null,
                30), new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null)))
                .hasMessageContaining("needs a topics.prefix without regular expression characters");
        assertThatThrownBy(() -> withTopics(new IotScenario.Topics(2, "persistent://public/default/iot-site-", 2, 30),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null)))
                .hasMessageContaining("doesn't support topics.partitions");
        assertThat(new IotScenario.Topics(2, "persistent://public/default/iot-").subscribedByPattern()).isFalse();
    }

    @Test
    public void choosesTheMode() {
        assertThat(scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null))
                .applications().readsCompacted()).isFalse();
        IotScenario readers = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                null, null, null, false, null, null, "compactedReaders"));
        assertThat(readers.applications().readsCompacted()).isTrue();
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, null, false, null, null, "tableView")))
                .hasMessageContaining("applications.mode must be one of [subscription, compactedReaders, latestState]");
        // a reading pod rebuilds the state from the start at any time, so it doesn't join later
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 30), null, null, false, null, null, "compactedReaders")))
                .hasMessageContaining("applications.mode compactedReaders doesn't support");
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 1_000, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64),
                new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, null, null, null,
                        false, null, null, "compactedReaders"), null, 300))
                .hasMessageContaining("applications.mode compactedReaders doesn't support");
        // a scenario file without the setting keeps the subscription
        IotScenario.Applications fromYaml = new YamlScenarioLoader().mapper().convertValue(Map.of("count", 2,
                "podsPerApplication", 2, "subscriptionPrefix", "app-", "client",
                Map.of("ioThreads", 2, "listenerThreads", 2)), IotScenario.Applications.class);
        assertThat(fromYaml.mode()).isEqualTo("subscription");
    }

    @Test
    public void chunksLargeMessages() {
        IotScenario.Producer unbatchedChunks = new IotScenario.Producer(2, 2, 100, false, true, null, null, 131_072);
        IotScenario chunked = new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(1_048_576),
                new IotScenario.Devices(1_000), new IotScenario.Gateways(10, unbatchedChunks, null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2, 16), null), null, 300);
        assertThat(chunked.gateways().producer().chunkMaxMessageSizeBytes()).isEqualTo(131_072);
        assertThat(chunked.applications().client().receiverQueueSize()).isEqualTo(16);
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(1_048_576),
                new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null, null, 131_072),
                        null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null), null, 300))
                .hasMessageContaining("needs batchingEnabled: false");
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(1_048_576),
                new IotScenario.Devices(1_000), new IotScenario.Gateways(10, unbatchedChunks, null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null, null, null, null,
                        false, null, "Shared"), null, 300))
                .hasMessageContaining("needs a subscription type other than Shared");
        // compaction would keep only a message's last chunk
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(1_048_576),
                new IotScenario.Devices(1_000), new IotScenario.Gateways(10, unbatchedChunks, null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null, null, null, null,
                        false, null, null, "compactedReaders"), null, 300))
                .hasMessageContaining("chunkMaxMessageSizeBytes doesn't support applications.mode compactedReaders");
    }

    @Test
    public void schedulesTheDelivery() {
        IotScenario.Applications shared = new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, null, false, null, "Shared");
        IotScenario.ScheduledDelivery scheduled = new IotScenario.ScheduledDelivery(1_000, 60_000, null);
        assertThat(scheduled.earlyToleranceMillis()).isEqualTo(1_000);
        assertThat(withBehaviors(new IotScenario.Behaviors(null, null, null, scheduled), shared).behaviors()
                .scheduledDelivery().enabled()).isTrue();
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null, null, scheduled),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null)))
                .hasMessageContaining("behaviors.scheduledDelivery needs applications.subscriptionType Shared");
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null, null,
                new IotScenario.ScheduledDelivery(2_000, 1_000, null)), shared))
                .hasMessageContaining("0 <= minDelayMillis <= maxDelayMillis");
        // the last message is delivered 200 s after the measurement's 120 s
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null, null,
                new IotScenario.ScheduledDelivery(0, 200_000, null)), shared))
                .hasMessageContaining("timeoutSeconds is 300, but the last scheduled message is delivered 200 s"
                        + " after the warmup and the measurement, which take 120 s, so set timeoutSeconds to at"
                        + " least 320");
    }

    @Test
    public void failsAndRetriesMessages() {
        assertThat(new IotScenario.ProcessingFailures(0.05, 0.01, null, null, null).retry()).isEqualTo("negativeAck");
        IotScenario.ProcessingFailures failures =
                new IotScenario.ProcessingFailures(0.05, 0.01, "retryTopic", null, null);
        assertThat(failures.maxRedeliveries()).isEqualTo(3);
        long poison = 0;
        long transientFailures = 0;
        for (long sequence = 0; sequence < 100_000; sequence++) {
            if (failures.poison(7, sequence)) {
                poison++;
                // a poison message fails every attempt
                assertThat(failures.fails(7, sequence, 10)).isTrue();
            } else if (failures.fails(7, sequence, 0)) {
                transientFailures++;
                // the same messages fail their second attempt, and succeed by their third
                assertThat(failures.fails(7, sequence, 1)).isTrue();
                assertThat(failures.fails(7, sequence, 2)).isFalse();
            }
        }
        assertThat(poison).isBetween(800L, 1_200L);
        assertThat(transientFailures).isBetween(4_500L, 5_500L);
        IotScenario.Applications shared = new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, null, null, null, false, null, "Shared");
        assertThat(withBehaviors(new IotScenario.Behaviors(null, null, failures), shared)
                .handsOverFinalSequences()).isTrue();
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null, failures),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null)))
                .hasMessageContaining("behaviors.processingFailures needs applications.subscriptionType Shared");
        // a late application's catch-up would count the retry topic as one of the topics
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null, failures),
                new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null,
                        null, false, null, "Shared")))
                .hasMessageContaining("doesn't support applications.joinSeconds");
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null,
                new IotScenario.ProcessingFailures(0.05, 0, "retryTopic", null, 2)), shared))
                .hasMessageContaining("maxRedeliveries must be at least 3");
        // the test scenario's gateways batch, so negative acknowledgments can't count the attempts
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null,
                new IotScenario.ProcessingFailures(0.05, 0, null, null, null)), shared))
                .hasMessageContaining("behaviors.processingFailures.retry negativeAck needs no podRestarts");
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null, null,
                new IotScenario.ProcessingFailures(0.05, 0, "deadLetter", null, null)), shared))
                .hasMessageContaining("behaviors.processingFailures.retry must be one of [negativeAck, retryTopic]");
    }

    @Test
    public void validatesThePodPauses() {
        assertThat(withBehaviors(null).behaviors().podPauses().enabled()).isFalse();
        assertThat(withBehaviors(new IotScenario.Behaviors(null, new IotScenario.PodPauses(15, 0.1, 5000)))
                .behaviors().podPauses().enabled()).isTrue();
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null,
                new IotScenario.PodPauses(5, 0.1, 5000))))
                .hasMessageContaining("behaviors.podPauses.pauseMillis must be shorter than intervalSeconds");
        assertThatThrownBy(() -> withBehaviors(new IotScenario.Behaviors(null,
                new IotScenario.PodPauses(15, 1.5, 5000))))
                .hasMessageContaining("fraction must be from 0 to 1");
        // a scenario file with restarts only has no pauses
        IotScenario.Behaviors fromYaml = new YamlScenarioLoader().mapper().convertValue(
                Map.of("podRestarts", Map.of("intervalSeconds", 30, "fraction", 0.1)), IotScenario.Behaviors.class);
        assertThat(withBehaviors(fromYaml).behaviors().podPauses().enabled()).isFalse();
    }

    private static IotScenario withBehaviors(IotScenario.Behaviors behaviors) {
        return withBehaviors(behaviors,
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null));
    }

    private static IotScenario withBehaviors(IotScenario.Behaviors behaviors,
                                             IotScenario.Applications applications) {
        return new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64),
                new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"), applications, behaviors, 300);
    }

    @Test
    public void choosesTheKeyType() {
        assertThat(withDevices(new IotScenario.Devices(1_000)).stringKeys()).isFalse();
        assertThat(withDevices(new IotScenario.Devices(1_000, "bytes")).stringKeys()).isFalse();
        assertThat(withDevices(new IotScenario.Devices(1_000, "string")).stringKeys()).isTrue();
        assertThatThrownBy(() -> withDevices(new IotScenario.Devices(1_000, "long")))
                .hasMessageContaining("devices.keyType must be one of [bytes, string], not long");
        // a scenario file without the setting keeps binary keys
        IotScenario.Devices fromYaml = new YamlScenarioLoader().mapper().convertValue(Map.of("count", 1_000),
                IotScenario.Devices.class);
        assertThat(withDevices(fromYaml).stringKeys()).isFalse();
    }

    private static IotScenario withDevices(IotScenario.Devices devices) {
        return new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), devices,
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null), null, 300);
    }

    @Test
    public void rejectsInvalidJoinSettings() {
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), null))).hasMessageContaining("a value for each of the 3 applications");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, -1), null))).hasMessageContaining("must be at least 0");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 300), null))).hasMessageContaining("the latest join must be within timeoutSeconds");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, Arrays.asList(0, null), null))).hasMessageContaining("applications.joinSeconds must be");
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 20), 0))).hasMessageContaining("caughtUpLatencyMillis must be at least 1");
        // the joins count from the measurement's start, after 2 warmup rounds of 20 s with 2 s after each
        IotScenario.Warmup warmup = new IotScenario.Warmup(20, 0, 2, 2);
        assertThat(scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 255), null), warmup).joinSeconds(1)).isEqualTo(255);
        assertThatThrownBy(() -> scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2),
                null, List.of(0, 256), null), warmup))
                .hasMessageContaining("the warmup (44 s) and the latest join must be within timeoutSeconds");
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
    public void rejectsABatchingDelayBelowOneMillisecond() {
        assertThatThrownBy(() -> new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null, 0), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"),
                new IotScenario.Applications(1, 2, "app-", new IotScenario.Client(2, 2), null), null, 300))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gateways.producer.batchingMaxPublishDelayMillis must be at least 1");
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

    private static IotScenario scenario(IotScenario.Applications applications) {
        return scenario(applications, new IotScenario.Warmup(0, 0, 1, 0));
    }

    private static IotScenario withTopics(IotScenario.Topics topics, IotScenario.Applications applications) {
        return new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null), null),
                topics, applications, null, 300);
    }

    // The gateways without batching, as skipping messages needs
    private static IotScenario unbatched(IotScenario.Applications applications) {
        return new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, false, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"), applications, null, 300);
    }

    private static IotScenario scenario(IotScenario.Applications applications, IotScenario.Warmup warmup) {
        // a time-based warmup needs a rate
        return new IotScenario("pulsar://localhost:6650", warmup, new IotScenario.Measurement(120, 1_000),
                warmup.seconds() > 0 ? 100 : 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, true, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"), applications, null, 300);
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
