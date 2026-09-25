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
package org.apache.pulsar.tests.performance.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

/**
 * Records bounded microsecond latency values and writes them to an HdrHistogram interval log, one interval per
 * second, so that the log shows how latency changed over the run, as HistogramLogAnalyzer plots it. Merged, the
 * intervals give the run's whole distribution. A second without recorded values, such as during warmup, writes no
 * interval.
 */
final class HdrLatencyRecorder implements AutoCloseable {
    private static final long MAX_LATENCY_MICROS = TimeUnit.DAYS.toMicros(10);
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final long INTERVAL_MILLIS = 1000;
    private final Recorder recorder = new Recorder(MAX_LATENCY_MICROS, SIGNIFICANT_DIGITS);
    private final PrintStream output;
    private final HistogramLogWriter writer;
    private final ScheduledExecutorService intervals;
    // Reused for each interval, as the Recorder allows; only the interval thread and close() use it
    private Histogram interval;
    private boolean closed;

    /** Starts logging to {@code path}, which is replaced. */
    HdrLatencyRecorder(Path path) throws IOException {
        output = new PrintStream(Files.newOutputStream(path));
        writer = new HistogramLogWriter(output);
        writer.outputLogFormatVersion();
        writer.outputLegend();
        intervals = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "hdr-latency-log");
            thread.setDaemon(true);
            return thread;
        });
        intervals.scheduleAtFixedRate(this::writeInterval, INTERVAL_MILLIS, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    void recordNanos(long latencyNanos) {
        recordMicros(TimeUnit.NANOSECONDS.toMicros(Math.max(0, latencyNanos)));
    }

    void recordMillis(long latencyMillis) {
        recordMicros(TimeUnit.MILLISECONDS.toMicros(Math.max(0, latencyMillis)));
    }

    private void recordMicros(long latencyMicros) {
        recorder.recordValue(Math.min(latencyMicros, MAX_LATENCY_MICROS));
    }

    // The Recorder stamps each interval with its start and end, so the log carries its own timeline
    private synchronized void writeInterval() {
        if (closed) {
            return;
        }
        interval = recorder.getIntervalHistogram(interval);
        if (interval.getTotalCount() > 0) {
            writer.outputIntervalHistogram(interval);
        }
    }

    /** Writes the last interval and closes the log. */
    @Override
    public void close() throws IOException {
        intervals.shutdownNow();
        try {
            intervals.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        writeInterval();
        synchronized (this) {
            closed = true;
            output.close();
        }
        if (output.checkError()) {
            throw new IOException("Writing the latency log failed");
        }
    }
}
