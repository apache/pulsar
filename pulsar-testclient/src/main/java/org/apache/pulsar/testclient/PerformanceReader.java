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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "read", description = "Test pulsar reader performance.")
public class PerformanceReader extends PerformanceTopicListArguments {
    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final DecimalFormat intFormat = new PaddingDecimalFormat("0", 7);
    private static final DecimalFormat dec = new DecimalFormat("0.000");

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
    public void run() throws Exception {
        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting Pulsar performance reader with config: {}", w.writeValueAsString(this));

        final RateLimiter limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;
        ReaderListener<byte[]> listener = (reader, msg) -> {
            messagesReceived.increment();
            bytesReceived.add(msg.getData().length);

            totalMessagesReceived.increment();
            totalBytesReceived.add(msg.getData().length);

            if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
                log.info("------------- DONE (reached the maximum number: [{}] of consumption) --------------",
                        this.numMessages);
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

        log.info("Start reading from {} topics", this.numTopics);

        final long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));

        if (this.testTime > 0) {
            TimerTask timoutTask = new TimerTask() {
                @Override
                public void run() {
                    log.info("------------- DONE (reached the maximum duration: [{} seconds] of consumption) "
                            + "--------------", testTime);
                    PerfClientUtils.exit(0);
                }
            };
            Timer timer = new Timer();
            timer.schedule(timoutTask, this.testTime * 1000);
        }

        long oldTime = System.nanoTime();
        Histogram reportHistogram = null;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            long total = totalMessagesReceived.sum();
            double rate = messagesReceived.sumThenReset() / elapsed;
            double throughput = bytesReceived.sumThenReset() / elapsed * 8 / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);
            log.info(
                    "Read throughput: {} msg --- {}  msg/s -- {} Mbit/s --- Latency: mean: {} ms - med: {} - 95pct: {} "
                            + "- 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    intFormat.format(total),
                    dec.format(rate), dec.format(throughput), dec.format(reportHistogram.getMean()),
                    reportHistogram.getValueAtPercentile(50), reportHistogram.getValueAtPercentile(95),
                    reportHistogram.getValueAtPercentile(99), reportHistogram.getValueAtPercentile(99.9),
                    reportHistogram.getValueAtPercentile(99.99), reportHistogram.getMaxValue());

            reportHistogram.reset();
            oldTime = now;
        }

        pulsarClient.close();
    }
    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed * 8 / 1024 / 1024;
        log.info(
                "Aggregated throughput stats --- {} records received --- {} msg/s --- {} Mbit/s",
                totalMessagesReceived,
                dec.format(rate),
                dec.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                        + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
                dec.format(reportHistogram.getMean()), reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95), reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9), reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999), reportHistogram.getMaxValue());
    }

    private static final Logger log = LoggerFactory.getLogger(PerformanceReader.class);
}
