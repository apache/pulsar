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
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;
import org.apache.pulsar.tests.performance.tools.JoinPreparedMarker;
import org.apache.pulsar.tests.performance.tools.MeasurementStartMarker;

/**
 * Prepares the join of the applications that join later, which wait for it: at the earliest one's join time, it
 * moves each late application's subscription forward by its {@code applications.skipMessages} on every topic, unloads
 * every topic with {@code applications.unloadBeforeJoin}, and then marks the join prepared. With offloading, it
 * records the topics' ledgers, and how many of them the brokers have offloaded, before it and at the run's end, in
 * {@link RunReport#OFFLOAD_STATUS_FILE}.
 */
final class JoinPreparation implements AutoCloseable {
    private final PulsarAdmin admin;
    private final IotScenario scenario;
    private final Path runOutput;
    private final Consumer<String> status;
    // Completed exceptionally when preparing the join fails, which fails the run at once
    private final CompletableFuture<Void> failure = new CompletableFuture<>();
    private Thread thread;

    private JoinPreparation(PulsarAdmin admin, IotScenario scenario, Path runOutput, Consumer<String> status) {
        this.admin = admin;
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
        JoinPreparation preparation = new JoinPreparation(PulsarAdmin.builder().serviceHttpUrl(adminUrl)
                .connectionTimeout(10, TimeUnit.SECONDS)
                .readTimeout(2, TimeUnit.MINUTES)
                .build(), scenario, runOutput, status);
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

    private void prepare(Path coordinationDirectory, String runId, boolean offloads, long deadlineNanos) {
        try {
            long measurementStart = MeasurementStartMarker.await(coordinationDirectory, runId, deadlineNanos);
            long waitMillis = measurementStart + TimeUnit.SECONDS.toMillis(scenario.earliestLateJoinSeconds())
                    - System.currentTimeMillis();
            if (waitMillis > 0) {
                Thread.sleep(waitMillis);
            }
            long started = System.nanoTime();
            if (offloads) {
                recordOffloadStatus("join");
            }
            if (!scenario.preparesJoin()) {
                return;
            }
            for (int application = 0; application < scenario.applicationCount(); application++) {
                long skipped = scenario.skipMessages(application);
                if (skipped > 0) {
                    for (String topic : scenario.topicNames()) {
                        admin.topics().skipMessages(topic, scenario.subscriptionName(application), skipped);
                    }
                }
            }
            if (scenario.applications().unloadBeforeJoin()) {
                for (String topic : scenario.topicNames()) {
                    admin.topics().unload(topic);
                }
            }
            JoinPreparedMarker.mark(coordinationDirectory, runId);
            status.accept(String.format(Locale.ROOT, "Prepared the late applications' join in %.1f s:%s%s",
                    (System.nanoTime() - started) / 1e9,
                    scenario.applications().skipMessages().stream().anyMatch(skipped -> skipped > 0)
                            ? " skipped " + scenario.applications().skipMessages() + " message(s) of each topic;"
                            : "",
                    scenario.applications().unloadBeforeJoin() ? " unloaded the topics" : ""));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            // the applications' timeout fails the run
            status.accept("The late applications' join wasn't prepared: the measurement didn't start");
        } catch (Throwable e) {
            failure.completeExceptionally(e);
            status.accept("Preparing the late applications' join failed: " + e);
            log().warn().exception(e).log("Preparing the late applications' join failed");
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
     * Appends each topic's ledgers and how many of them the brokers have offloaded to
     * {@link RunReport#OFFLOAD_STATUS_FILE}, and shows their sums.
     */
    void recordOffloadStatus(String phase) throws PulsarAdminException, IOException {
        StringBuilder lines = new StringBuilder();
        int ledgers = 0;
        int offloaded = 0;
        long offloadedBytes = 0;
        long now = System.currentTimeMillis();
        for (String topic : scenario.topicNames()) {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
            int topicOffloaded = 0;
            long topicOffloadedBytes = 0;
            for (LedgerInfo ledger : stats.ledgers) {
                if (ledger.offloaded) {
                    topicOffloaded++;
                    topicOffloadedBytes += ledger.size;
                }
            }
            lines.append(now).append(',').append(phase).append(',').append(topic).append(',')
                    .append(stats.ledgers.size()).append(',').append(topicOffloaded).append(',')
                    .append(topicOffloadedBytes).append(',').append(stats.numberOfEntries).append('\n');
            ledgers += stats.ledgers.size();
            offloaded += topicOffloaded;
            offloadedBytes += topicOffloadedBytes;
        }
        Files.writeString(runOutput.resolve(RunReport.OFFLOAD_STATUS_FILE), lines, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        String summary = String.format(Locale.ROOT, "%d of %d ledger(s) offloaded, %,d MB", offloaded, ledgers,
                offloadedBytes / (1024 * 1024));
        status.accept("Offload status (" + phase + "): " + summary);
    }

    @Override
    public void close() throws InterruptedException {
        try {
            if (thread != null) {
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(10));
            }
        } finally {
            admin.close();
        }
    }

    private static Logger log() {
        return Logger.get(JoinPreparation.class);
    }
}
