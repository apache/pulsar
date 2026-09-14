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

import static org.apache.pulsar.testclient.PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS;
import static org.apache.pulsar.testclient.PerfClientUtils.addShutdownHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.github.merlimat.slog.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine.Option;

/**
 * Client-agnostic implementation of the {@code pulsar-perf} reader benchmark.
 *
 * <p>The CLI options, the throughput and latency accounting and the reports live here. Concrete
 * subclasses bind the client types: {@link PerformanceReader} drives the V5 {@code
 * CheckpointConsumer} from dedicated poll threads, and {@link PerformanceReaderV4} drives a v4
 * {@code Reader} through a {@code ReaderListener}.
 *
 * @param <ClientT> the client type ({@code PulsarClient} of the respective API generation)
 * @param <ReaderT> the reader handle
 * @param <MessageT> the received message type
 */
public abstract class PerformanceReaderBase<ClientT, ReaderT, MessageT> extends PerformanceTopicListArguments {

    /**
     * Logger named after the <em>concrete</em> command class rather than this base, so that the
     * report lines keep identifying the subcommand that produced them (the integration tests in
     * {@code PerfToolTest} match on {@code PerformanceReader - Aggregated ...}).
     */
    protected final Logger log = Logger.get(getClass());

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalBytesReceived = new LongAdder();

    protected static final long MAX_LATENCY_MILLIS = TimeUnit.DAYS.toMillis(10);

    private final Recorder recorder = new Recorder(MAX_LATENCY_MILLIS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
    private final Recorder cumulativeRecorder =
            new Recorder(MAX_LATENCY_MILLIS, LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);

    private RateLimiter limiter;

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

    protected PerformanceReaderBase(String cmdName) {
        super(cmdName);
    }

    // ------------------------------------------------------------------------------------------
    // Client-specific seams
    // ------------------------------------------------------------------------------------------

    /** Build and connect the client. */
    protected abstract ClientT createClient() throws Exception;

    /** Close a client built by {@link #createClient()}; must tolerate a {@code null} argument. */
    protected abstract void closeClient(ClientT client);

    /** Create one reader on {@code topic}, positioned according to {@code --start-message-id}. */
    protected abstract CompletableFuture<ReaderT> createReaderAsync(ClientT client, String topic) throws Exception;

    /** Payload size of a received message, in bytes. */
    protected abstract int messageSize(MessageT msg);

    /** Publish timestamp of a received message, in milliseconds since the epoch. */
    protected abstract long publishTimeMillis(MessageT msg);

    /**
     * Start driving the readers. A no-op where the client pushes messages itself (the v4
     * {@code ReaderListener}); V5 has no listener, so it starts one poll thread per reader.
     */
    protected void startReading(List<ReaderT> readers) throws Exception {
    }

    /** Stop whatever {@link #startReading(List)} started, before the client is closed. */
    protected void stopReading() {
    }

    /** Hook for per-client run preparation, e.g. warnings about options with no effect. */
    protected void prepareRun() {
    }

    // ------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        // Dump config variables
        PerfClientUtils.printJVMInformation(log);
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info().attr("config", w.writeValueAsString(this)).log("Starting Pulsar performance reader with config");

        prepareRun();

        this.limiter = this.rate > 0 ? RateLimiter.create(this.rate) : null;

        ClientT client = createClient();

        List<CompletableFuture<ReaderT>> futures = new ArrayList<>();
        for (int i = 0; i < this.numTopics; i++) {
            final TopicName topicName = TopicName.get(this.topics.get(i));
            futures.add(createReaderAsync(client, topicName.toString()));
        }

        final List<ReaderT> readers = new ArrayList<>(futures.size());
        for (CompletableFuture<ReaderT> future : futures) {
            readers.add(future.get());
        }

        startReading(readers);

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

        stopReading();
        closeClient(client);
        PerfClientUtils.removeAndRunShutdownHook(shutdownHookThread);
    }

    /**
     * The per-message handler shared by the v4 reader listener and the V5 poll loop.
     *
     * @return whether the run is done and the caller should stop reading
     */
    protected final boolean handleMessage(MessageT msg) {
        int size = messageSize(msg);
        messagesReceived.increment();
        bytesReceived.add(size);
        totalMessagesReceived.increment();
        totalBytesReceived.add(size);

        if (this.numMessages > 0 && totalMessagesReceived.sum() >= this.numMessages) {
            log.info().attr("number", this.numMessages).log("DONE (reached the maximum number: of consumption");
            PerfClientUtils.exit(0);
            return true;
        }

        if (limiter != null) {
            limiter.acquire();
        }

        long latencyMillis = System.currentTimeMillis() - publishTimeMillis(msg);
        if (latencyMillis >= 0) {
            // Reading a backlog older than the histogram range must not blow up the read loop.
            long clampedLatencyMillis = Math.min(latencyMillis, MAX_LATENCY_MILLIS);
            recorder.recordValue(clampedLatencyMillis);
            cumulativeRecorder.recordValue(clampedLatencyMillis);
        }
        return false;
    }

    private void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesReceived.sum() / elapsed;
        double throughput = totalBytesReceived.sum() / elapsed * 8 / 1024 / 1024;
        log.infof("Aggregated throughput stats --- %d records received --- %.3f msg/s --- %.3f Mbit/s",
                totalMessagesReceived.sum(), rate, throughput);
    }

    private void printAggregatedStats() {
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
