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

import io.github.merlimat.slog.Logger;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;
import org.apache.pulsar.tests.performance.tools.JoinPreparedMarker;
import org.apache.pulsar.tests.performance.tools.MeasurementStartMarker;

/**
 * Prepares the join of the applications that join later, which wait for it: at the earliest one's join time, it
 * moves each late application's subscription forward by its {@code applications.skipMessages} on every topic, resets it
 * to the publish time of its {@code applications.resetToMillis} once the gateways have finished, unloads every topic
 * with {@code applications.unloadBeforeJoin}, and then marks the join prepared, with the messages that each application
 * that reset its subscription expects: its backlog then. It records how long each of these took in
 * {@link RunReport#JOIN_PREPARATION_FILE}. With offloading, it
 * records the topics' ledgers, and how many of them the brokers have offloaded, before it and at the run's end, in
 * {@link RunReport#OFFLOAD_STATUS_FILE}.
 */
final class JoinPreparation implements AutoCloseable {
    /** The admin operations of the join preparation, which a test fakes. */
    interface TopicOperations extends AutoCloseable {
        long backlog(String topic, String subscription) throws Exception;

        void skip(String topic, String subscription, long messages) throws Exception;

        void reset(String topic, String subscription, long publishTime) throws Exception;

        void unload(String topic) throws Exception;

        OffloadStatus offloadStatus(String topic) throws Exception;

        @Override
        void close();
    }

    /** A topic's ledgers, how many of them have been offloaded and their bytes, and its entries. */
    record OffloadStatus(int ledgers, int offloadedLedgers, long offloadedBytes, long entries) {
    }

    private final TopicOperations topicOperations;
    private final IotScenario scenario;
    private final Path runOutput;
    // Written when the gateways have finished
    private final Path gatewaysSummary;
    private final Consumer<String> status;
    // Completed exceptionally when preparing the join fails, which fails the run at once
    private final CompletableFuture<Void> failure = new CompletableFuture<>();
    // Set by close(), whose interrupt ends a preparation in progress without it having failed
    private volatile boolean closing;
    private Thread thread;

    JoinPreparation(TopicOperations topicOperations, IotScenario scenario, Path runOutput, Consumer<String> status) {
        this.gatewaysSummary = runOutput.resolve("gateways").resolve("gateways-summary.json");
        this.topicOperations = topicOperations;
        this.scenario = scenario;
        this.runOutput = runOutput;
        this.status = status;
    }

    /**
     * Starts preparing the join, when the scenario prepares it, in the background, and with {@code offloads} records
     * the offload status when the late applications join; the launcher records it at the run's end too.
     */
    static JoinPreparation start(String adminUrl, IotScenario scenario, boolean offloads, Path runOutput,
                                 Path coordinationDirectory, String runId, Consumer<String> status) throws IOException {
        if (offloads) {
            Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE),
                    RunReport.OFFLOAD_STATUS_HEADER + "\n");
        }
        JoinPreparation preparation = new JoinPreparation(adminTopicOperations(PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl)
                .connectionTimeout(10, TimeUnit.SECONDS)
                .readTimeout(2, TimeUnit.MINUTES)
                .build()), scenario, runOutput, status);
        // At the earliest late join: preparing it, and with offloading the offload status then
        if (scenario.preparesJoin() || offloads && scenario.hasLateApplications()) {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(scenario.timeoutSeconds());
            preparation.thread = new Thread(() -> preparation.prepare(coordinationDirectory, runId, offloads,
                    deadlineNanos), "join-preparation");
            preparation.thread.setDaemon(true);
            preparation.thread.start();
        }
        return preparation;
    }

    /**
     * Waits for the earliest late join, prepares it, and marks it prepared; with {@code offloads}, records the offload
     * status just before, as the applications will read the topics.
     */
    void prepare(Path coordinationDirectory, String runId, boolean offloads, long deadlineNanos) {
        try {
            long measurementStart = MeasurementStartMarker.await(coordinationDirectory, runId, deadlineNanos);
            long waitMillis = measurementStart + TimeUnit.SECONDS.toMillis(scenario.earliestLateJoinSeconds())
                    - System.currentTimeMillis();
            if (waitMillis > 0) {
                Thread.sleep(waitMillis);
            }
            long started = System.nanoTime();
            if (!scenario.preparesJoin()) {
                if (offloads) {
                    recordOffloadStatusOrWarn("join");
                }
                return;
            }
            Files.writeString(runOutput.resolve(RunReport.JOIN_PREPARATION_FILE),
                    RunReport.JOIN_PREPARATION_HEADER + "\n");
            for (int application = 0; application < scenario.applicationCount(); application++) {
                long skipped = scenario.skipMessages(application);
                if (skipped > 0) {
                    for (String topic : scenario.topicNames()) {
                        String subscription = scenario.subscriptionName(application);
                        // The broker skips no more than the backlog, and the application expects every message but
                        // the skipped ones, so the backlog has to have them; it only grows, without consumers
                        long backlog = topicOperations.backlog(topic, subscription);
                        if (backlog < skipped) {
                            throw new IllegalStateException(String.format(Locale.ROOT, "%s of %s has a backlog of"
                                    + " %,d message(s), fewer than the %,d to skip: join later, or skip fewer",
                                    subscription, topic, backlog, skipped));
                        }
                        long start = System.currentTimeMillis();
                        topicOperations.skip(topic, subscription, skipped);
                        record(start, "skip", subscription, topic, skipped);
                    }
                }
            }
            Map<Integer, Long> expectedMessages = new LinkedHashMap<>();
            if (scenario.resetsSubscriptions()) {
                // The backlog after a reset is what the application expects, so the gateways have to have finished
                awaitGateways(deadlineNanos);
                for (int application = 0; application < scenario.applicationCount(); application++) {
                    long resetTo = scenario.resetToMillis(application);
                    if (resetTo < 0) {
                        continue;
                    }
                    String subscription = scenario.subscriptionName(application);
                    long backlog = 0;
                    for (String topic : scenario.topicNames()) {
                        long start = System.currentTimeMillis();
                        topicOperations.reset(topic, subscription, measurementStart + resetTo);
                        record(start, "reset", subscription, topic, resetTo);
                        backlog += topicOperations.backlog(topic, subscription);
                    }
                    expectedMessages.put(application, backlog);
                }
            }
            if (scenario.applications().unloadBeforeJoin()) {
                for (String topic : scenario.topicNames()) {
                    long start = System.currentTimeMillis();
                    topicOperations.unload(topic);
                    record(start, "unload", "", topic, 0);
                }
            }
            if (offloads) {
                recordOffloadStatusOrWarn("join");
            }
            JoinPreparedMarker.mark(coordinationDirectory, runId, expectedMessages);
            status.accept(String.format(Locale.ROOT, "Prepared the late applications' join in %.1f s:%s%s",
                    (System.nanoTime() - started) / 1e9,
                    scenario.applications().skipMessages().stream().anyMatch(skipped -> skipped > 0)
                            ? " skipped " + scenario.applications().skipMessages() + " message(s) of each topic;"
                            : "",
                    (expectedMessages.isEmpty() ? "" : " reset subscriptions to "
                            + scenario.applications().resetToMillis() + " ms after the measurement's start, which"
                            + " left backlogs of " + expectedMessages.values() + ";")
                            + (scenario.applications().unloadBeforeJoin() ? " unloaded the topics" : "")));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            // the applications' timeout fails the run
            status.accept("The late applications' join wasn't prepared: " + e.getMessage());
        } catch (Throwable e) {
            if (closing || Thread.currentThread().isInterrupted()) {
                // The run is shutting down, such as after another failure, which interrupted an admin request
                log().debug().exception(e).log("Preparing the late applications' join ended by the shutdown");
                return;
            }
            failure.completeExceptionally(e);
            status.accept("Preparing the late applications' join failed: " + e);
            log().warn().exception(e).log("Preparing the late applications' join failed");
        }
    }

    // Appends an action of the join preparation and how long it took to the join preparation file
    private void record(long startEpochMillis, String action, String subscription, String topic, long argument)
            throws IOException {
        long end = System.currentTimeMillis();
        Files.writeString(runOutput.resolve(RunReport.JOIN_PREPARATION_FILE), startEpochMillis + "," + end + ","
                + action + "," + subscription + "," + topic + "," + argument + "," + (end - startEpochMillis) + "\n",
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    // Waits for the gateways to finish, which they have when they have written their summary
    private void awaitGateways(long deadlineNanos) throws InterruptedException, TimeoutException {
        if (!Files.exists(gatewaysSummary)) {
            status.accept("Waiting for the gateways to finish before resetting the late applications' subscriptions");
        }
        while (!Files.exists(gatewaysSummary)) {
            if (deadlineNanos - System.nanoTime() <= 0) {
                throw new TimeoutException("The gateways didn't finish");
            }
            Thread.sleep(100);
        }
    }

    /**
     * Completes exceptionally when preparing the join fails: the late applications would wait for it until the
     * workload's timeout.
     */
    CompletableFuture<Void> failure() {
        return failure;
    }

    /**
     * Records the offload status, which is diagnostic: a failure to read it is a warning, not the run's failure.
     */
    void recordOffloadStatusOrWarn(String phase) {
        try {
            recordOffloadStatus(phase);
        } catch (Exception e) {
            if (closing || Thread.currentThread().isInterrupted()) {
                // The run is shutting down, which interrupted or closed the admin request
                log().debug().attr("phase", phase).exception(e).log("Recording the offload status ended by the"
                        + " shutdown");
                return;
            }
            status.accept("Warning: couldn't record the offload status (" + phase + "): " + e.getMessage());
            log().warn().attr("phase", phase).exception(e).log("Recording the offload status failed");
        }
    }

    /**
     * Appends each topic's ledgers and how many of them the brokers have offloaded to
     * {@link RunReport#OFFLOAD_STATUS_FILE}, and shows their sums.
     */
    void recordOffloadStatus(String phase) throws Exception {
        StringBuilder lines = new StringBuilder();
        int ledgers = 0;
        int offloaded = 0;
        long offloadedBytes = 0;
        long now = System.currentTimeMillis();
        for (String topic : scenario.topicNames()) {
            OffloadStatus topicStatus = topicOperations.offloadStatus(topic);
            lines.append(now).append(',').append(phase).append(',').append(topic).append(',')
                    .append(topicStatus.ledgers()).append(',').append(topicStatus.offloadedLedgers()).append(',')
                    .append(topicStatus.offloadedBytes()).append(',').append(topicStatus.entries()).append('\n');
            ledgers += topicStatus.ledgers();
            offloaded += topicStatus.offloadedLedgers();
            offloadedBytes += topicStatus.offloadedBytes();
        }
        Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE), lines, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        String summary = String.format(Locale.ROOT, "%d of %d ledger(s) offloaded, %,d MB", offloaded, ledgers,
                offloadedBytes / (1024 * 1024));
        status.accept("Offload status (" + phase + "): " + summary);
    }

    @Override
    public void close() throws InterruptedException {
        closing = true;
        try {
            if (thread != null) {
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(10));
            }
        } finally {
            topicOperations.close();
        }
    }

    private static TopicOperations adminTopicOperations(PulsarAdmin admin) {
        return new TopicOperations() {
            @Override
            public long backlog(String topic, String subscription) throws PulsarAdminException {
                SubscriptionStats stats = admin.topics().getStats(topic).getSubscriptions().get(subscription);
                return stats != null ? stats.getMsgBacklog() : 0;
            }

            @Override
            public void skip(String topic, String subscription, long messages) throws PulsarAdminException {
                admin.topics().skipMessages(topic, subscription, messages);
            }

            @Override
            public void reset(String topic, String subscription, long publishTime) throws PulsarAdminException {
                admin.topics().resetCursor(topic, subscription, publishTime);
            }

            @Override
            public void unload(String topic) throws PulsarAdminException {
                admin.topics().unload(topic);
            }

            @Override
            public OffloadStatus offloadStatus(String topic) throws PulsarAdminException {
                PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
                int offloaded = 0;
                long offloadedBytes = 0;
                for (LedgerInfo ledger : stats.ledgers) {
                    if (ledger.offloaded) {
                        offloaded++;
                        offloadedBytes += ledger.size;
                    }
                }
                return new OffloadStatus(stats.ledgers.size(), offloaded, offloadedBytes, stats.numberOfEntries);
            }

            @Override
            public void close() {
                admin.close();
            }
        };
    }

    private static Logger log() {
        return Logger.get(JoinPreparation.class);
    }
}
