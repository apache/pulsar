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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.GetStatsOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Samples the workload topics' stats once per second into {@code topic-stats.csv}: each subscription's backlog, the
 * topic's published-message counter and the subscription's dispatched-message counter. The report derives
 * per-second publish and dispatch rates from the counter deltas, because the broker's own rates refresh only once
 * per stats interval, and the sampled maximum backlog, which is not the exact peak between samples.
 *
 * <p>One sample is one REST call per topic, all topics in parallel, that reads the broker's in-memory counters and its
 * estimated backlog (the default, not the precise backlog that scans the ledger), without the stats of the topic's
 * publishers and consumers, so once per second has no measurable cost next to a workload of about 100,000 messages
 * per second. A failed sample is reported and skipped; sampling never fails the
 * run.
 *
 * <p>A broker redirects the stats request of a topic that another broker owns to that broker's name in the cluster's
 * network, which the launcher's host can't resolve. So the sampler asks each broker through its port on the host: for
 * a topic whose request fails, it asks the other brokers, of which only the owner answers, and samples the topic
 * through that broker from then on.
 */
final class TopicStatsSampler implements AutoCloseable {
    // The CSV the run report reads; its name and columns are the report tool's
    static final String FILE_NAME = RunReport.TOPIC_STATS_FILE;
    static final String HEADER = RunReport.TOPIC_STATS_HEADER;
    private static final long INTERVAL_MILLIS = 1000;

    // The first broker's
    private final PulsarAdmin admin;
    // Each broker's, by its name in the cluster's network
    private final Map<String, PulsarAdmin> brokerAdmins;
    // The broker that owns a topic, when it isn't the first
    private final Map<String, PulsarAdmin> owners = new ConcurrentHashMap<>();
    private final List<String> topics;
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;
    private int failedSamples;
    private volatile Backlog latestBacklog;

    /**
     * The backlog of a sampling round: the sum over every topic and subscription, and the largest subscription's,
     * summed over the topics, which is the application that is furthest behind.
     */
    record Backlog(long epochMs, long total, long maxSubscription) {
    }

    private TopicStatsSampler(Map<String, PulsarAdmin> brokerAdmins, List<String> topics, BufferedWriter writer) {
        this.brokerAdmins = brokerAdmins;
        this.admin = brokerAdmins.values().iterator().next();
        this.topics = topics;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "topic-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts sampling {@code topics} through the brokers' HTTP services into {@code runDirectory}.
     *
     * @param brokerHttpUrls each broker's HTTP service on the host, by the broker's name in the cluster's network, the
     *                       first broker first
     */
    static TopicStatsSampler start(Map<String, String> brokerHttpUrls, List<String> topics, Path runDirectory)
            throws IOException {
        Map<String, PulsarAdmin> brokerAdmins = new LinkedHashMap<>();
        for (Map.Entry<String, String> broker : brokerHttpUrls.entrySet()) {
            brokerAdmins.put(broker.getKey(), PulsarAdmin.builder()
                    .serviceHttpUrl(broker.getValue())
                    .connectionTimeout(5, TimeUnit.SECONDS)
                    .readTimeout(5, TimeUnit.SECONDS)
                    .build());
        }
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(HEADER);
        writer.newLine();
        TopicStatsSampler sampler = new TopicStatsSampler(brokerAdmins, topics, writer);
        // Fixed delay on one thread: a slow call delays the next sample instead of piling up requests.
        sampler.executor.scheduleWithFixedDelay(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    private void sample() {
        // One timestamp per round, so that the topics of a round line up in the report.
        long now = System.currentTimeMillis();
        Map<String, Long> backlogs = new HashMap<>();
        boolean complete = true;
        // In parallel, so that a round takes as long as the slowest topic rather than all of them together
        List<CompletableFuture<TopicStats>> requests = topics.stream()
                .map(topic -> owners.getOrDefault(topic, admin).topics().getStatsAsync(topic, STATS_OPTIONS))
                .toList();
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            try {
                TopicStats stats = unwrap(requests.get(i));
                for (Map.Entry<String, ? extends SubscriptionStats> subscription : stats.getSubscriptions()
                        .entrySet()) {
                    backlogs.merge(subscription.getKey(), subscription.getValue().getMsgBacklog(), Long::sum);
                    writer.write(now + "," + topic + "," + subscription.getKey() + ","
                            + subscription.getValue().getMsgBacklog() + "," + stats.getMsgInCounter() + ","
                            + subscription.getValue().getMsgOutCounter());
                    writer.newLine();
                }
                writer.flush();
            } catch (PulsarAdminException.NotFoundException e) {
                // The producer creates the topic with its first message to it
                complete = false;
            } catch (Exception e) {
                complete = false;
                if (executor.isShutdown()) {
                    // close() interrupted the round
                    return;
                }
                if (locateOwner(topic)) {
                    // The next round asks the broker that owns it
                    continue;
                }
                // Failures are worth a line each at first, and then one in sixty.
                if (++failedSamples <= 3 || failedSamples % 60 == 0) {
                    System.out.println("Topic stats sample of " + topic + " failed (" + failedSamples
                            + " so far): " + e);
                }
            }
        }
        if (complete) {
            latestBacklog = new Backlog(now, backlogs.values().stream().mapToLong(Long::longValue).sum(),
                    backlogs.values().stream().mapToLong(Long::longValue).max().orElse(0));
        }
    }

    /**
     * Finds the broker that owns a topic, by asking the brokers other than the one that failed: the others redirect
     * to the owner's name in the cluster's network, which fails on the host. Returns whether it found another broker.
     */
    private boolean locateOwner(String topic) {
        PulsarAdmin failed = owners.getOrDefault(topic, admin);
        for (PulsarAdmin candidate : brokerAdmins.values()) {
            if (candidate == failed) {
                continue;
            }
            try {
                candidate.topics().getStatsAsync(topic, STATS_OPTIONS).get(5, TimeUnit.SECONDS);
                owners.put(topic, candidate);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                // Not the owner, or not answering
            }
        }
        return false;
    }

    // The stats of a topic's publishers and consumers are the bulk of the response, over 1 MB for a topic with
    // 20 applications of 100 consumers, and the sampler needs neither
    private static final GetStatsOptions STATS_OPTIONS =
            GetStatsOptions.builder().excludePublishers(true).excludeConsumers(true).build();

    private static TopicStats unwrap(CompletableFuture<TopicStats> request) throws Exception {
        try {
            return request.get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    /** The backlog of the latest round that sampled every topic, or null before there is one. */
    Backlog latestBacklog() {
        return latestBacklog;
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            writer.close();
        } finally {
            brokerAdmins.values().forEach(PulsarAdmin::close);
        }
    }
}
