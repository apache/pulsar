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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

/**
 * Samples the workload topics' stats once per second into {@code topic-stats.csv}: each subscription's backlog, the
 * topic's published-message counter and the subscription's dispatched-message counter. The report derives
 * per-second publish and dispatch rates from the counter deltas, because the broker's own rates refresh only once
 * per stats interval, and the sampled maximum backlog, which is not the exact peak between samples.
 *
 * <p>One sample is one REST call per topic that reads the broker's in-memory counters and its estimated backlog
 * (the default, not the precise backlog that scans the ledger), so once per second has no measurable cost next to
 * a workload of about 100,000 messages per second. A failed sample is reported and skipped; sampling never fails the
 * run.
 */
final class TopicStatsSampler implements AutoCloseable {
    static final String FILE_NAME = "topic-stats.csv";
    static final String HEADER = "epochMillis,topic,subscription,msgBacklog,msgInCounter,msgOutCounter";
    private static final long INTERVAL_MILLIS = 1000;

    private final PulsarAdmin admin;
    private final List<String> topics;
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;
    private int failedSamples;

    private TopicStatsSampler(PulsarAdmin admin, List<String> topics, BufferedWriter writer) {
        this.admin = admin;
        this.topics = topics;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "topic-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts sampling {@code topics} through the broker's HTTP service into {@code runDirectory}. */
    static TopicStatsSampler start(String httpServiceUrl, List<String> topics, Path runDirectory) throws IOException {
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(httpServiceUrl)
                .connectionTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(HEADER);
        writer.newLine();
        TopicStatsSampler sampler = new TopicStatsSampler(admin, topics, writer);
        // Fixed delay on one thread: a slow call delays the next sample instead of piling up requests.
        sampler.executor.scheduleWithFixedDelay(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    private void sample() {
        // One timestamp per round, so that the topics of a round line up in the report.
        long now = System.currentTimeMillis();
        for (String topic : topics) {
            try {
                TopicStats stats = admin.topics().getStats(topic);
                for (Map.Entry<String, ? extends SubscriptionStats> subscription : stats.getSubscriptions()
                        .entrySet()) {
                    writer.write(now + "," + topic + "," + subscription.getKey() + ","
                            + subscription.getValue().getMsgBacklog() + "," + stats.getMsgInCounter() + ","
                            + subscription.getValue().getMsgOutCounter());
                    writer.newLine();
                }
                writer.flush();
            } catch (Exception e) {
                // The topic may not exist yet at the first samples; later failures are worth a line each.
                if (++failedSamples <= 3 || failedSamples % 60 == 0) {
                    System.out.println("Topic stats sample of " + topic + " failed (" + failedSamples
                            + " so far): " + e);
                }
            }
        }
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
            admin.close();
        }
    }
}
