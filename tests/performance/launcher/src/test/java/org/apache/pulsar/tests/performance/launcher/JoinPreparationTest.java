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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JoinPreparationTest {
    private static final String RUN_ID = "run";
    private static final List<String> TOPICS = List.of("persistent://public/default/iot-0",
            "persistent://public/default/iot-1");

    private Path runOutput;
    private Path coordination;
    private final List<String> statuses = new CopyOnWriteArrayList<>();

    @BeforeMethod
    public void createDirectories() throws IOException {
        runOutput = Files.createTempDirectory("join-preparation");
        coordination = runOutput.resolve("coordination");
        Files.createDirectories(coordination);
        // The measurement started a minute ago, so the late applications' join is due
        Files.writeString(coordination.resolve("measurement-start-" + RUN_ID),
                Long.toString(System.currentTimeMillis() - 60_000));
        statuses.clear();
    }

    @AfterMethod(alwaysRun = true)
    public void deleteDirectories() throws IOException {
        if (!Files.exists(runOutput)) {
            return;
        }
        try (var paths = Files.walk(runOutput)) {
            paths.sorted(Collections.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    @Test
    public void skipsResetsAndUnloadsAndRecordsTheOffloadStatusBeforeMarkingTheJoinPrepared() throws Exception {
        // app-1 skips 100 messages of each topic, app-2 resets its subscriptions, and both join after 30 s
        IotScenario scenario = scenario(new IotScenario.Applications(3, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 30, 30), null, List.of(0L, 100L, 0L), true, List.of(-1L, -1L, 5_000L)));
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.backlogs.put("app-1", 400L);
        topicOperations.backlogs.put("app-2", 250L);
        // the gateways have finished, which resetting waits for
        Files.createDirectories(runOutput.resolve("gateways"));
        Files.writeString(runOutput.resolve("gateways").resolve("gateways-summary.json"), "{}");
        Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE), RunReport.OFFLOAD_STATUS_HEADER + "\n");

        JoinPreparation preparation = new JoinPreparation(topicOperations, scenario, runOutput, statuses::add);
        preparation.prepare(coordination, RUN_ID, true, deadline());

        assertThat(preparation.failure()).isNotDone();
        assertThat(topicOperations.calls).containsExactly(
                "backlog " + TOPICS.get(0) + " app-1", "skip " + TOPICS.get(0) + " app-1 100",
                "backlog " + TOPICS.get(1) + " app-1", "skip " + TOPICS.get(1) + " app-1 100",
                "reset " + TOPICS.get(0) + " app-2", "backlog " + TOPICS.get(0) + " app-2",
                "reset " + TOPICS.get(1) + " app-2", "backlog " + TOPICS.get(1) + " app-2",
                "unload " + TOPICS.get(0), "unload " + TOPICS.get(1),
                // just before the join is marked prepared
                "offloadStatus " + TOPICS.get(0) + " unmarked", "offloadStatus " + TOPICS.get(1) + " unmarked");
        // app-2 expects its backlogs after the resets
        assertThat(Files.readString(coordination.resolve("join-prepared-" + RUN_ID))).isEqualTo("2=250\n");
        List<String> actions = Files.readAllLines(runOutput.resolve(RunReport.JOIN_PREPARATION_FILE));
        assertThat(actions.get(0)).isEqualTo(RunReport.JOIN_PREPARATION_HEADER);
        assertThat(actions.subList(1, actions.size())).map(line -> String.join(",",
                        List.of(line.split(",")).subList(2, 6)))
                .containsExactly("skip,app-1," + TOPICS.get(0) + ",100", "skip,app-1," + TOPICS.get(1) + ",100",
                        "reset,app-2," + TOPICS.get(0) + ",5000", "reset,app-2," + TOPICS.get(1) + ",5000",
                        "unload,," + TOPICS.get(0) + ",0", "unload,," + TOPICS.get(1) + ",0");
        List<String> offloadStatus = Files.readAllLines(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE));
        assertThat(offloadStatus.subList(1, offloadStatus.size())).map(line -> line.substring(line.indexOf(',') + 1))
                .containsExactly("join," + TOPICS.get(0) + ",3,2,2097152,3000",
                        "join," + TOPICS.get(1) + ",3,2,2097152,3000");
        assertThat(statuses).contains("Offload status (join): 4 of 6 ledger(s) offloaded, 4 MB")
                .anyMatch(status -> status.startsWith("Prepared the late applications' join"));
    }

    @Test
    public void failsWhenTheBacklogHasFewerMessagesThanToSkip() throws Exception {
        IotScenario scenario = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 30), null, List.of(0L, 100L), false));
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        // 99 of each topic
        topicOperations.backlogs.put("app-1", 198L);

        JoinPreparation preparation = new JoinPreparation(topicOperations, scenario, runOutput, statuses::add);
        preparation.prepare(coordination, RUN_ID, false, deadline());

        assertThat(preparation.failure()).isCompletedExceptionally();
        assertThat(preparation.failure().exceptionNow())
                .hasMessageContaining("app-1 of " + TOPICS.get(0) + " has a backlog of 99 message(s), fewer than"
                        + " the 100 to skip");
        assertThat(topicOperations.calls).containsExactly("backlog " + TOPICS.get(0) + " app-1");
        assertThat(coordination.resolve("join-prepared-" + RUN_ID)).doesNotExist();
        assertThat(statuses).anyMatch(status -> status.startsWith("Preparing the late applications' join failed"));
    }

    @Test
    public void failingToReadTheOffloadStatusIsAWarning() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.offloadStatusFails = true;
        Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE), RunReport.OFFLOAD_STATUS_HEADER + "\n");

        // A late application that the launcher doesn't prepare: only the offload status is recorded
        JoinPreparation onlyStatus = new JoinPreparation(topicOperations, scenario(new IotScenario.Applications(2, 2,
                "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null)), runOutput, statuses::add);
        onlyStatus.prepare(coordination, RUN_ID, true, deadline());
        assertThat(onlyStatus.failure()).isNotDone();
        assertThat(statuses).containsExactly("Warning: couldn't record the offload status (join): no stats");

        // Preparing the join goes on to mark it prepared
        JoinPreparation preparation = new JoinPreparation(topicOperations, scenario(new IotScenario.Applications(2, 2,
                "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null, null, true)), runOutput,
                statuses::add);
        preparation.prepare(coordination, RUN_ID, true, deadline());
        assertThat(preparation.failure()).isNotDone();
        assertThat(coordination.resolve("join-prepared-" + RUN_ID)).exists();

        preparation.recordOffloadStatusOrWarn("end");
        assertThat(statuses).contains("Warning: couldn't record the offload status (end): no stats");
        assertThat(Files.readAllLines(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE)))
                .containsExactly(RunReport.OFFLOAD_STATUS_HEADER);
    }

    @Test
    public void closingEndsThePreparationWithoutFailingIt() throws Exception {
        IotScenario scenario = scenario(new IotScenario.Applications(2, 2, "app-", new IotScenario.Client(2, 2), null,
                List.of(0, 30), null, List.of(0L, 100L), false));
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.backlogs.put("app-1", 400L);
        topicOperations.blockSkips = true;
        JoinPreparation preparation = new JoinPreparation(topicOperations, scenario, runOutput, statuses::add);
        Thread thread = new Thread(() -> preparation.prepare(coordination, RUN_ID, false, deadline()));
        thread.start();
        assertThat(topicOperations.skipStarted.await(10, TimeUnit.SECONDS)).isTrue();

        // Closing the admin client fails the skip in progress
        preparation.close();
        thread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(thread.isAlive()).isFalse();
        assertThat(preparation.failure()).isNotDone();
        assertThat(statuses).isEmpty();
    }

    @Test
    public void closingWhileRecordingTheOffloadStatusIsNoWarning() throws Exception {
        FakeTopicOperations topicOperations = new FakeTopicOperations();
        topicOperations.blockOffloadStatus = true;
        Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE), RunReport.OFFLOAD_STATUS_HEADER + "\n");
        JoinPreparation preparation = new JoinPreparation(topicOperations, scenario(new IotScenario.Applications(2, 2,
                "app-", new IotScenario.Client(2, 2), null, List.of(0, 30), null)), runOutput, statuses::add);
        Thread thread = new Thread(() -> preparation.prepare(coordination, RUN_ID, true, deadline()));
        thread.start();
        assertThat(topicOperations.offloadStatusStarted.await(10, TimeUnit.SECONDS)).isTrue();

        preparation.close();
        thread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(thread.isAlive()).isFalse();
        assertThat(preparation.failure()).isNotDone();
        assertThat(statuses).isEmpty();
    }

    private static long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    }

    // The gateways without batching, as skipping and resetting need
    private static IotScenario scenario(IotScenario.Applications applications) {
        return new IotScenario("pulsar://localhost:6650", new IotScenario.Warmup(0, 0, 1, 0),
                new IotScenario.Measurement(120, 1_000), 0, new IotScenario.Payload(64), new IotScenario.Devices(1_000),
                new IotScenario.Gateways(10, new IotScenario.Producer(2, 2, 100, false, true, null), null),
                new IotScenario.Topics(2, "persistent://public/default/iot-"), applications, null, 300);
    }

    /** Records the admin operations; a subscription's backlog is split evenly over the topics. */
    private final class FakeTopicOperations implements JoinPreparation.TopicOperations {
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        final Map<String, Long> backlogs = new HashMap<>();
        final CountDownLatch skipStarted = new CountDownLatch(1);
        final CountDownLatch offloadStatusStarted = new CountDownLatch(1);
        final CountDownLatch closed = new CountDownLatch(1);
        volatile boolean offloadStatusFails;
        volatile boolean blockSkips;
        volatile boolean blockOffloadStatus;

        @Override
        public long backlog(String topic, String subscription) {
            calls.add("backlog " + topic + " " + subscription);
            return backlogs.getOrDefault(subscription, 0L) / TOPICS.size();
        }

        @Override
        public void skip(String topic, String subscription, long messages) throws Exception {
            calls.add("skip " + topic + " " + subscription + " " + messages);
            if (blockSkips) {
                skipStarted.countDown();
                closed.await();
                throw new IllegalStateException("the admin client was closed");
            }
        }

        @Override
        public void reset(String topic, String subscription, long publishTime) {
            calls.add("reset " + topic + " " + subscription);
        }

        @Override
        public void unload(String topic) {
            calls.add("unload " + topic);
        }

        @Override
        public JoinPreparation.OffloadStatus offloadStatus(String topic) {
            calls.add("offloadStatus " + topic
                    + (Files.exists(coordination.resolve("join-prepared-" + RUN_ID)) ? " marked" : " unmarked"));
            if (offloadStatusFails) {
                throw new IllegalStateException("no stats");
            }
            if (blockOffloadStatus) {
                offloadStatusStarted.countDown();
                try {
                    closed.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("the admin client was closed");
            }
            return new JoinPreparation.OffloadStatus(3, 2, 2 * 1024 * 1024, 3_000);
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }
}
