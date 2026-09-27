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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;
import org.HdrHistogram.Histogram;

/**
 * Reports a run's progress on the console, as pulsar-perf does: every interval, a line for the producer and a line
 * for the applications with their throughput, latency percentiles and the subscriptions' backlog.
 *
 * <p>The gateways' and the applications' containers stream their progress from their control port as
 * newline-delimited JSON, a line per second with cumulative counters and the second's latencies as an HdrHistogram,
 * see the tools' {@code ProgressStream}; the applications' lines sum their applications' counters and merge their
 * latencies, and count the applications that have finished. The monitor derives the rates from the counters, so that
 * its lines cover exactly the time between them.
 */
final class ProgressMonitor implements AutoCloseable {
    static final String PROGRESS_PATH = "/progress";
    private static final long STREAM_INTERVAL_MILLIS = 1000;
    // The workloads' latency recorders' maximum
    private static final long MAX_LATENCY_MICROS = TimeUnit.HOURS.toMicros(1);
    private static final String PRODUCER = "producer";
    private static final long STALE_BACKLOG_SECONDS = 5;

    private final ObjectMapper mapper;
    private final PrintStream out;
    private final int payloadBytes;
    private final int applications;
    private final Supplier<TopicStatsSampler.Backlog> backlog;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ScheduledExecutorService printer;
    private final List<Thread> readers = new ArrayList<>();
    private final long startedNanos = System.nanoTime();

    // Guarded by this: the latest line of each source, and the latencies since the last report
    private final Map<String, JsonNode> latest = new LinkedHashMap<>();
    private final Histogram producerLatency = new Histogram(MAX_LATENCY_MICROS, 3);
    private final Histogram consumerLatency = new Histogram(MAX_LATENCY_MICROS, 3);
    private long lastReportNanos = startedNanos;
    private long lastSent;
    private long lastReceived;
    private volatile boolean closed;

    /**
     * @param payloadBytes the size of a message's payload, for the bit rates
     * @param backlog the latest backlog sample, or null when none
     */
    ProgressMonitor(ObjectMapper mapper, PrintStream out, int payloadBytes, int applications,
                    Supplier<TopicStatsSampler.Backlog> backlog) {
        this.mapper = mapper;
        this.out = out;
        this.payloadBytes = payloadBytes;
        this.applications = applications;
        this.backlog = backlog;
        this.printer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "progress-report");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts reporting every {@code intervalSeconds}. */
    void start(int intervalSeconds) {
        printer.scheduleAtFixedRate(this::report, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Follows the progress stream of {@code source} at {@code controlUrl}, reconnecting while {@code running} is
     * true, until the stream's last line.
     */
    void follow(String source, String controlUrl, BooleanSupplier running) {
        HttpRequest request = HttpRequest.newBuilder(URI.create(controlUrl + PROGRESS_PATH
                + "?intervalMillis=" + STREAM_INTERVAL_MILLIS)).GET().build();
        Thread reader = new Thread(() -> {
            boolean ended = false;
            while (!ended && !closed && running.getAsBoolean()) {
                try {
                    HttpResponse<Stream<String>> response = client.send(request, HttpResponse.BodyHandlers.ofLines());
                    try (Stream<String> lines = response.body()) {
                        for (String line : (Iterable<String>) lines::iterator) {
                            JsonNode node = mapper.readTree(line);
                            accept(source, node);
                            ended = node.path("final").asBoolean();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (IOException | RuntimeException e) {
                    // Not serving yet, or the stream broke; the loop checks whether the source still runs
                }
                if (!ended) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }, "progress-" + source);
        reader.setDaemon(true);
        reader.start();
        synchronized (readers) {
            readers.add(reader);
        }
    }

    synchronized void accept(String source, JsonNode line) {
        latest.put(source, line);
        JsonNode histogram = line.path("latency").path("histogram");
        if (histogram.isTextual() && line.path("latency").path("count").asLong() > 0) {
            try {
                Histogram interval = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(Base64.getDecoder().decode(histogram.textValue())), MAX_LATENCY_MICROS);
                (PRODUCER.equals(line.path("role").asText()) ? producerLatency : consumerLatency).add(interval);
            } catch (DataFormatException | RuntimeException e) {
                // A garbled interval only leaves a gap in the latencies
            }
        }
    }

    /** Prints the lines for the time since the previous report. */
    synchronized void report() {
        long now = System.nanoTime();
        double seconds = Math.max(1e-3, (now - lastReportNanos) / 1e9);
        lastReportNanos = now;
        JsonNode producer = latest.get(PRODUCER);
        long sent = producer != null ? producer.path("sent").asLong() : 0;
        long received = 0;
        long receivedTarget = 0;
        long duplicates = 0;
        long orderingViolations = 0;
        int finishedApplications = 0;
        for (Map.Entry<String, JsonNode> entry : latest.entrySet()) {
            JsonNode line = entry.getValue();
            if (!PRODUCER.equals(entry.getKey())) {
                received += line.path("received").asLong();
                receivedTarget += line.path("messageCount").asLong();
                duplicates += line.path("duplicates").asLong();
                orderingViolations += line.path("orderingViolations").asLong();
                finishedApplications += line.path("finishedApplications").asInt();
            }
        }
        String prefix = String.format(Locale.ROOT, "[%s] ", phase(producer));

        StringBuilder produced = new StringBuilder(prefix).append(String.format(Locale.ROOT,
                "Produced: %,d msg%s --- %,.1f msg/s --- %.1f Mbit/s", sent,
                producer != null ? of(producer.path("messageCount").asLong(), sent) : "",
                (sent - lastSent) / seconds, bitRate((sent - lastSent) / seconds)));
        if (producer != null) {
            produced.append(" --- pending: ").append(producer.path("pending").asLong());
        }
        produced.append(latency(producerLatency));
        out.println(produced);

        StringBuilder receivedLine = new StringBuilder(prefix).append(String.format(Locale.ROOT,
                "Received: %,d msg%s --- %,.1f msg/s --- %.1f Mbit/s", received, of(receivedTarget, received),
                (received - lastReceived) / seconds, bitRate((received - lastReceived) / seconds)));
        TopicStatsSampler.Backlog currentBacklog = backlog.get();
        if (currentBacklog != null) {
            receivedLine.append(String.format(Locale.ROOT, " --- backlog: %,d msg (max per application: %,d",
                    currentBacklog.total(), currentBacklog.maxSubscription()));
            // The sampler samples every second, unless the broker answers slowly; an old sample says so
            long ageSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - currentBacklog.epochMs());
            if (ageSeconds >= STALE_BACKLOG_SECONDS) {
                receivedLine.append(", sampled ").append(ageSeconds).append(" s ago");
            }
            receivedLine.append(')');
        }
        if (finishedApplications > 0 && finishedApplications < applications) {
            receivedLine.append(" --- finished applications: ").append(finishedApplications).append('/')
                    .append(applications);
        }
        if (duplicates > 0) {
            receivedLine.append(String.format(Locale.ROOT, " --- duplicates: %,d", duplicates));
        }
        if (orderingViolations > 0) {
            receivedLine.append(String.format(Locale.ROOT, " --- ordering violations: %,d", orderingViolations));
        }
        receivedLine.append(latency(consumerLatency));
        out.println(receivedLine);

        lastSent = sent;
        lastReceived = received;
        producerLatency.reset();
        consumerLatency.reset();
    }

    /** The producer's phase with its progress, such as "warmup round 1/2" or "measurement 45/120 s". */
    private String phase(JsonNode producer) {
        long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedNanos);
        String clock = String.format(Locale.ROOT, "%02d:%02d", elapsed / 60, elapsed % 60);
        if (producer == null) {
            return clock + " starting";
        }
        String phase = producer.path("phase").asText("?");
        return switch (phase) {
            case "warmup" -> clock + " warmup round " + (producer.path("warmupRound").asInt() + 1) + "/"
                    + producer.path("warmupRounds").asInt();
            case "awaiting-measurement-start" -> clock + " cooling down";
            case "measurement" -> {
                long start = producer.path("measurementStartEpochMs").asLong(-1);
                yield start > 0 ? String.format(Locale.ROOT, "%s measurement %d s", clock,
                        TimeUnit.MILLISECONDS.toSeconds(producer.path("epochMs").asLong() - start))
                        : clock + " measurement";
            }
            case "draining" -> clock + " awaiting send receipts";
            case "finished" -> clock + " receiving";
            default -> clock + " " + phase;
        };
    }

    private static String of(long target, long count) {
        // Rounded down, so that 100 % means every message
        return target > 0 ? String.format(Locale.ROOT, " of %,d (%d%%)", target, 100 * count / target) : "";
    }

    private double bitRate(double messagesPerSecond) {
        return messagesPerSecond * payloadBytes * 8 / 1e6;
    }

    /** Latency percentiles in milliseconds, as pulsar-perf prints them, from microsecond values. */
    static String latency(Histogram histogram) {
        if (histogram.getTotalCount() == 0) {
            return "";
        }
        return String.format(Locale.ROOT, " --- Latency: mean: %.3f ms - med: %.3f - 95pct: %.3f - 99pct: %.3f"
                        + " - 99.9pct: %.3f - 99.99pct: %.3f - Max: %.3f",
                histogram.getMean() / 1000, millis(histogram, 50), millis(histogram, 95), millis(histogram, 99),
                millis(histogram, 99.9), millis(histogram, 99.99), histogram.getMaxValue() / 1000.0);
    }

    private static double millis(Histogram histogram, double percentile) {
        return histogram.getValueAtPercentile(percentile) / 1000.0;
    }

    /** Stops reporting, with a last report of the time since the previous one. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        printer.shutdownNow();
        try {
            printer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (readers) {
            readers.forEach(Thread::interrupt);
        }
    }
}
