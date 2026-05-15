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
package org.apache.pulsar.testclient;

import static org.apache.pulsar.testclient.PerfClientUtils.addShutdownHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "read", description = "Test pulsar reader performance.")
@CustomLog
public class PerformanceReader extends PerformanceTopicListArguments {
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();

    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();

    private static Recorder recorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);
    private static Recorder cumulativeRecorder = new Recorder(TimeUnit.DAYS.toMillis(10), 5);

    @Option(names = {"-r", "--rate"}, description = "Simulate a slow message reader (rate in msg/s)")
    public double rate = 0;

    @Option(names = {"-m",
            "--start-message-id"}, description = "Start message id. This can be either 'earliest', "
            + "'latest' or a specific message id by using 'lid:eid'")
    public String startMessageId = "earliest";

    @Option(names = {"-q", "--receiver-queue-size"}, description = "Size of the receiver queue")
    public int receiverQueueSize = 1000;

    @Option(names = {"-n",
            "--num-messages"}, description = "Number of messages to consume in total. If <= 0, "
            + "it will keep consuming")
    public long numMessages = 0;

    @Option(names = {
            "--use-tls"}, description = "Use TLS encryption on the connection", descriptionKey = "useTls")
    public boolean useTls;

    @Option(names = {"-time",
            "--test-duration"}, description = "Test duration in secs. If <= 0, it will keep consuming")
    public long testTime = 0;
    public PerformanceReader() {
        super("read");
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        if (startMessageId != "earliest" && startMessageId != "latest"
                && (startMessageId.split(":")).length != 2) {
            String errMsg = String.format("invalid start message ID '%s', must be either either 'earliest', "
                    + "'latest' or a specific message id by using 'lid:eid'", startMessageId);
            throw new Exception(errMsg);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void run() throws Exception {
        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar performance reader with config");

        final RateLimiter limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;
        ReaderListener<byte[]> listener = (reader, msg) -> {
            messagesReceived.increment();
            bytesReceived.add(msg.getData().length);

            totalMessagesReceived.increment();
            totalBytesReceived.add(msg.getData().length);

            if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
                log.info().attr("number", this.numMessages).log("DONE (reached the maximum number: of consumption");
                PerfClientUtils.exit(0);
            }

            if (limiter != null) {
                limiter.acquire();
            }

            long latencyMillis = System.currentTimeMillis() - msg.getPublishTime();
            if (latencyMillis >= 0) {
                recorder.recordValue(latencyMillis);
                cumulativeRecorder.recordValue(latencyMillis);
            }
        };

        ClientBuilder clientBuilder = PerfClientUtils.createClientBuilderFromArguments(this)
                .enableTls(this.useTls);

        PulsarClient pulsarClient = clientBuilder.build();

        List<CompletableFuture<Reader<byte[]>>> futures = new ArrayList<>();

        MessageId startMessageId;
        if ("earliest".equals(this.startMessageId)) {
            startMessageId = MessageId.earliest;
        } else if ("latest".equals(this.startMessageId)) {
            startMessageId = MessageId.latest;
        } else {
            String[] parts = this.startMessageId.split(":");
            startMessageId = new MessageIdImpl(Long.parseLong(parts[0]), Long.parseLong(parts[1]), -1);
        }

        ReaderBuilder<byte[]> readerBuilder = pulsarClient.newReader() //
                .readerListener(listener) //
                .receiverQueueSize(this.receiverQueueSize) //
                .startMessageId(startMessageId);

        for (int i = 0; i < this.numTopics; i++) {
            final TopicName topicName = TopicName.get(this.topics.get(i));

            futures.add(readerBuilder.clone().topic(topicName.toString()).createAsync());
        }

        FutureUtil.waitForAll(futures).get();

        log.info().attr("reading", this.numTopics).log("Start reading from topics");

        final long start = System.nanoTime();
        Thread shutdownHookThread = addShutdownHook(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        });

        if (this.testTime > 0) {
            TimerTask timoutTask = new TimerTask() {
                @Override
                public void run() {
                    log.info()
                            .attr("duration", testTime)
                            .log("------------- DONE (reached the maximum duration:"
                                    + " [ seconds] of consumption) --------------");
                    PerfClientUtils.exit(0);
                }
            };
            Timer timer = new Timer();
            timer.schedule(timoutTask, this.testTime * 1000);
        }

        long oldTime = System.nanoTime();
        Histogram reportHistogram = null;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesReceived.sum();
            double rate = messagesReceived.sumThenReset() / elapsed;
            double throughput = bytesReceived.sumThenReset() / elapsed * 8 / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);
            log.infof("Read throughput: %7d msg --- %.3f msg/s --- %.3f Mbit/s"
                            + " --- Latency: mean: %.3f ms - med: %d"
                            + " - 95pct: %d - 99pct: %d"
                            + " - 99.9pct: %d - 99.99pct: %d - Max: %d",
                    total, rate, throughput,
                    reportHistogram.getMean(),
                    reportHistogram.getValueAtPercentile(50),
                    reportHistogram.getValueAtPercentile(95),
                    reportHistogram.getValueAtPercentile(99),
                    reportHistogram.getValueAtPercentile(99.9),
                    reportHistogram.getValueAtPercentile(99.99),
                    reportHistogram.getMaxValue());

            reportHistogram.reset();
            oldTime = now;
        }

        PerfClientUtils.closeClient(pulsarClient);
        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }
    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed * 8 / 1024 / 1024;
        log.infof("Aggregated throughput stats --- %d records received --- %.3f msg/s --- %.3f Mbit/s",
                totalMessagesReceived.sum(), rate, throughput);
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.infof("Aggregated latency stats --- Latency: mean: %.3f ms"
                        + " - med: %d - 95pct: %d - 99pct: %d"
                        + " - 99.9pct: %d - 99.99pct: %d"
                        + " - 99.999pct: %d - Max: %d",
                reportHistogram.getMean(),
                reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95),
                reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9),
                reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999),
                reportHistogram.getMaxValue());
    }
}
