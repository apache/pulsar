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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
 *
 * <p>The latencies are also recorded into a second recorder for each open progress stream, as pulsar-perf keeps a
 * second histogram for its periodic report, so that a stream reads its own intervals without taking them from the
 * log. The progress recorders also get the warmup's latencies, which the log leaves out.
 */
final class HdrLatencyRecorder implements AutoCloseable {
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final long INTERVAL_MILLIS = 1000;
    private final long maxLatencyMicros;
    private final Recorder recorder;
    private final List<Recorder> progressRecorders = new CopyOnWriteArrayList<>();
    private final PrintStream output;
    private final HistogramLogWriter writer;
    private final ScheduledExecutorService intervals;
    // Reused for each interval, as the Recorder allows; only the interval thread and close() use it
    private Histogram interval;
    private boolean closed;

    /**
     * Starts logging to {@code path}, which is replaced. A latency above {@code maxLatencyMicros} is recorded as that
     * maximum; the histograms' size grows with it.
     */
    HdrLatencyRecorder(Path path, long maxLatencyMicros) throws IOException {
        this.maxLatencyMicros = maxLatencyMicros;
        recorder = new Recorder(maxLatencyMicros, SIGNIFICANT_DIGITS);
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
        recordNanos(latencyNanos, true);
    }

    void recordMillis(long latencyMillis) {
        recordMillis(latencyMillis, true);
    }

    /** Records a latency, into the log only when it was {@code measured} rather than a warmup message's. */
    void recordNanos(long latencyNanos, boolean measured) {
        recordMicros(TimeUnit.NANOSECONDS.toMicros(Math.max(0, latencyNanos)), measured);
    }

    /** Records a latency, into the log only when it was {@code measured} rather than a warmup message's. */
    void recordMillis(long latencyMillis, boolean measured) {
        recordMicros(TimeUnit.MILLISECONDS.toMicros(Math.max(0, latencyMillis)), measured);
    }

    private void recordMicros(long latencyMicros, boolean measured) {
        long value = Math.min(latencyMicros, maxLatencyMicros);
        if (measured) {
            recorder.recordValue(value);
        }
        for (Recorder progressRecorder : progressRecorders) {
            progressRecorder.recordValue(value);
        }
    }

    /** A recorder that gets every latency from now on, until it is removed, for a progress stream's intervals. */
    Recorder addProgressRecorder() {
        Recorder progressRecorder = new Recorder(maxLatencyMicros, SIGNIFICANT_DIGITS);
        addProgressRecorder(progressRecorder);
        return progressRecorder;
    }

    /**
     * Records every latency from now on into {@code progressRecorder} too, until it is removed. Several recorders
     * can share one, since a {@link Recorder} takes values from several threads.
     */
    void addProgressRecorder(Recorder progressRecorder) {
        progressRecorders.add(progressRecorder);
    }

    void removeProgressRecorder(Recorder progressRecorder) {
        progressRecorders.remove(progressRecorder);
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
