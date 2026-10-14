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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Samples the workload topics' stats once per second, or less often with many topics, into {@code topic-stats.csv}:
 * each subscription's backlog, the
 * topic's published-message counter and the subscription's dispatched-message counter. The report derives
 * per-second publish and dispatch rates from the counter deltas, because the broker's own rates refresh only once
 * per stats interval, and the sampled maximum backlog, which is not the exact peak between samples.
 *
 * <p>One sample is one REST call per topic, all topics in parallel, that reads the broker's in-memory counters and its
 * estimated backlog (the default, not the precise backlog that scans the ledger), without the stats of the topic's
 * publishers and consumers, so once per second has no measurable cost next to a workload of about 100,000 messages
 * per second with a few topics. Each call costs the broker an HTTP request and a topic ownership check, though: at
 * 1,000 topics, sampling every second took 16 % of the broker's CPU. So with more topics than
 * {@value #MAX_REQUESTS_PER_SECOND}, the sampler samples every {@code topics / MAX_REQUESTS_PER_SECOND} seconds,
 * rounded up, {@value #MAX_REQUESTS_PER_SECOND} calls per second on average, and a round makes at most
 * {@value #MAX_IN_FLIGHT} calls at a time over all the brokers. A round's samples have the time of its end, which
 * follows all of them. A failed sample is reported and skipped; sampling never fails the run. The random seeks'
 * subscription is left out: it exists only while the seeks run.
 *
 * <p>A broker redirects the stats request of a topic that another broker owns to that broker's name in the cluster's
 * network, which the launcher's host can't resolve. So the sampler asks each broker through its port on the host: for
 * a topic whose request fails, it asks the other brokers, of which only the owner answers, and samples the topic
 * through that broker from then on.
 *
 * <p>With a remote cluster, it also samples each topic's replicators into {@code replication-stats.csv}: the backlog
 * that the topic has yet to replicate, whether the replicator is connected, the delay in seconds of the message it
 * replicates next, and its replicated-message counter.
 */
final class TopicStatsSampler implements AutoCloseable {
    // The CSV the run report reads; its name and columns are the report tool's
    static final String FILE_NAME = RunReport.TOPIC_STATS_FILE;
    static final String HEADER = RunReport.TOPIC_STATS_HEADER;
    private static final long INTERVAL_MILLIS = 1000;
    static final int MAX_REQUESTS_PER_SECOND = 100;
    // The most requests of a round in flight at a time, over all the brokers: the admin clients' own limit of 16
    // connections applies to each broker's client separately
    static final int MAX_IN_FLIGHT = 16;
    static final String REPLICATION_FILE_NAME = RunReport.REPLICATION_STATS_FILE;
    static final String REPLICATION_HEADER = "epochMs,topic,remoteCluster,replicationBacklog,connected,"
            + "replicationDelaySeconds,msgOutCounter";

    // The first broker's
    private final PulsarAdmin admin;
    // Each broker's, by its name in the cluster's network
    private final Map<String, PulsarAdmin> brokerAdmins;
    // The broker that owns a topic, when it isn't the first
    private final Map<String, PulsarAdmin> owners = new ConcurrentHashMap<>();
    private final List<String> topics;
    private final BufferedWriter writer;
    // Null without a remote cluster
    private final BufferedWriter replicationWriter;
    private final ScheduledExecutorService executor;
    private int failedSamples;
    private volatile Backlog latestBacklog;

    /**
     * The backlog of a sampling round: the sum over every topic and subscription, and the largest subscription's,
     * summed over the topics, which is the application that is furthest behind; and the replication backlog summed
     * over the topics and remote clusters, or -1 without a remote cluster.
     */
    record Backlog(long epochMs, long total, long maxSubscription, long replication, long intervalMillis) {
        Backlog(long epochMs, long total, long maxSubscription) {
            this(epochMs, total, maxSubscription, -1);
        }

        Backlog(long epochMs, long total, long maxSubscription, long replication) {
            this(epochMs, total, maxSubscription, replication, INTERVAL_MILLIS);
        }
    }

    private TopicStatsSampler(Map<String, PulsarAdmin> brokerAdmins, List<String> topics, BufferedWriter writer,
                              BufferedWriter replicationWriter) {
        this.brokerAdmins = brokerAdmins;
        this.admin = brokerAdmins.values().iterator().next();
        this.topics = topics;
        this.writer = writer;
        this.replicationWriter = replicationWriter;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "topic-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Every second, or with more topics than {@link #MAX_REQUESTS_PER_SECOND}, every so many seconds. */
    static long intervalMillis(int topicCount) {
        long seconds = Math.max(1, (topicCount + MAX_REQUESTS_PER_SECOND - 1) / MAX_REQUESTS_PER_SECOND);
        return TimeUnit.SECONDS.toMillis(seconds);
    }

    /**
     * Starts sampling {@code topics} through the brokers' HTTP services into the file {@code fileName} of
     * {@code runDirectory}, such as {@code topic-stats.csv}, and with {@code replicated} the topics' replicators into
     * {@code replication-stats.csv}.
     *
     * @param brokerHttpUrls each broker's HTTP service on the host, by the broker's name in the cluster's network, the
     *                       first broker first
     */
    static TopicStatsSampler start(Map<String, String> brokerHttpUrls, List<String> topics, Path runDirectory,
                                   boolean replicated, String fileName) throws IOException {
        Map<String, PulsarAdmin> brokerAdmins = new LinkedHashMap<>();
        BufferedWriter writer = null;
        BufferedWriter replicationWriter = null;
        try {
            for (Map.Entry<String, String> broker : brokerHttpUrls.entrySet()) {
                brokerAdmins.put(broker.getKey(), PulsarAdmin.builder()
                        .serviceHttpUrl(broker.getValue())
                        .connectionTimeout(5, TimeUnit.SECONDS)
                        .readTimeout(5, TimeUnit.SECONDS)
                        .build());
            }
            writer = Files.newBufferedWriter(runDirectory.resolve(fileName));
            writer.write(HEADER);
            writer.newLine();
            if (replicated) {
                replicationWriter = Files.newBufferedWriter(runDirectory.resolve(REPLICATION_FILE_NAME));
                replicationWriter.write(REPLICATION_HEADER);
                replicationWriter.newLine();
            }
        } catch (IOException | RuntimeException e) {
            // The launcher goes on without the sampler, so it releases what it opened
            closeAll(e, writer, replicationWriter, brokerAdmins.values());
            throw e;
        }
        TopicStatsSampler sampler = new TopicStatsSampler(brokerAdmins, topics, writer, replicationWriter);
        // Fixed delay on one thread: a slow call delays the next sample instead of piling up requests.
        long intervalMillis = intervalMillis(topics.size());
        if (intervalMillis > INTERVAL_MILLIS) {
            System.out.println("Sampling the stats of the " + topics.size() + " topics every "
                    + intervalMillis / 1000 + " s, " + MAX_REQUESTS_PER_SECOND + " requests per second on average");
        }
        sampler.executor.scheduleWithFixedDelay(sampler::sample, 0, intervalMillis, TimeUnit.MILLISECONDS);
        return sampler;
    }

    private CompletableFuture<TopicStats> requestStats(String topic) {
        return owners.getOrDefault(topic, admin).topics().getStatsAsync(topic, STATS_OPTIONS);
    }

    private void sample() {
        // One timestamp per round, so that the topics of a round line up in the report: its end, which follows every
        // observation of the round, so that none appears to precede an event that happened during the round
        StringBuilder rows = new StringBuilder();
        StringBuilder replicationRows = new StringBuilder();
        Map<String, Long> backlogs = new HashMap<>();
        long replicationBacklog = 0;
        boolean complete = true;
        // In parallel, so that a round takes as long as the slowest topics rather than all of them together, but at
        // most MAX_IN_FLIGHT at a time, so that many topics' requests don't reach the broker in one burst
        List<CompletableFuture<TopicStats>> requests = new ArrayList<>(Collections.nCopies(topics.size(), null));
        for (int i = 0; i < Math.min(MAX_IN_FLIGHT - 1, topics.size()); i++) {
            requests.set(i, requestStats(topics.get(i)));
        }
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            // With the one awaited next, MAX_IN_FLIGHT at most
            if (i + MAX_IN_FLIGHT - 1 < topics.size()) {
                requests.set(i + MAX_IN_FLIGHT - 1, requestStats(topics.get(i + MAX_IN_FLIGHT - 1)));
            }
            try {
                TopicStats stats = unwrap(requests.get(i));
                for (Map.Entry<String, ? extends SubscriptionStats> subscription : stats.getSubscriptions()
                        .entrySet()) {
                    if (RandomSeeks.SUBSCRIPTION.equals(subscription.getKey())) {
                        continue;
                    }
                    backlogs.merge(subscription.getKey(), subscription.getValue().getMsgBacklog(), Long::sum);
                    rows.append(',').append(topic).append(',').append(subscription.getKey()).append(',')
                            .append(subscription.getValue().getMsgBacklog()).append(',')
                            .append(stats.getMsgInCounter()).append(',')
                            .append(subscription.getValue().getMsgOutCounter()).append('\n');
                }
                if (replicationWriter != null) {
                    for (Map.Entry<String, ? extends ReplicatorStats> replicator : stats.getReplication().entrySet()) {
                        ReplicatorStats replication = replicator.getValue();
                        replicationBacklog += replication.getReplicationBacklog();
                        replicationRows.append(',').append(topic).append(',').append(replicator.getKey()).append(',')
                                .append(replication.getReplicationBacklog()).append(',')
                                .append(replication.isConnected()).append(',')
                                .append(replication.getReplicationDelayInSeconds()).append(',')
                                .append(replication.getMsgOutCount()).append('\n');
                    }
                }
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
        long end = System.currentTimeMillis();
        try {
            writeRows(writer, end, rows);
            if (replicationWriter != null) {
                writeRows(replicationWriter, end, replicationRows);
            }
        } catch (IOException e) {
            complete = false;
            System.out.println("Couldn't write a topic stats sample: " + e);
        }
        if (complete) {
            latestBacklog = new Backlog(end, backlogs.values().stream().mapToLong(Long::longValue).sum(),
                    backlogs.values().stream().mapToLong(Long::longValue).max().orElse(0),
                    replicationWriter != null ? replicationBacklog : -1, intervalMillis(topics.size()));
        }
    }

    // Writes a round's rows, each with the round's timestamp before it
    private static void writeRows(BufferedWriter writer, long epochMs, StringBuilder rows) throws IOException {
        int start = 0;
        while (start < rows.length()) {
            int end = rows.indexOf("\n", start);
            writer.write(Long.toString(epochMs));
            writer.append(rows, start, end);
            writer.newLine();
            start = end + 1;
        }
        writer.flush();
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
        IOException failure = closeAll(null, writer, replicationWriter, brokerAdmins.values());
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Closes each writer and admin client, whichever fails. Adds the failures to {@code failure} when there is one;
     * otherwise returns the first, with the others suppressed by it, or null when everything closed.
     */
    private static IOException closeAll(Exception failure, BufferedWriter writer, BufferedWriter replicationWriter,
                                        Collection<PulsarAdmin> admins) {
        IOException first = null;
        for (BufferedWriter toClose : new BufferedWriter[] {writer, replicationWriter}) {
            if (toClose == null) {
                continue;
            }
            try {
                toClose.close();
            } catch (IOException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
                } else if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        for (PulsarAdmin admin : admins) {
            try {
                admin.close();
            } catch (RuntimeException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
                } else if (first == null) {
                    first = new IOException("Closing an admin client failed", e);
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        return first;
    }
}
