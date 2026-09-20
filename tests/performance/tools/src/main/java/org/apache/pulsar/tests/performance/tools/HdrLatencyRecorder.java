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
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

/** Records bounded microsecond latency values and writes an HdrHistogram interval log. */
final class HdrLatencyRecorder {
    private static final long MAX_LATENCY_MICROS = TimeUnit.DAYS.toMicros(10);
    private static final int SIGNIFICANT_DIGITS = 3;
    private final Recorder recorder = new Recorder(MAX_LATENCY_MICROS, SIGNIFICANT_DIGITS);

    void recordNanos(long latencyNanos) {
        recordMicros(TimeUnit.NANOSECONDS.toMicros(Math.max(0, latencyNanos)));
    }

    void recordMillis(long latencyMillis) {
        recordMicros(TimeUnit.MILLISECONDS.toMicros(Math.max(0, latencyMillis)));
    }

    private void recordMicros(long latencyMicros) {
        recorder.recordValue(Math.min(latencyMicros, MAX_LATENCY_MICROS));
    }

    void write(Path path, long startEpochMillis, long endEpochMillis) throws IOException {
        Histogram histogram = recorder.getIntervalHistogram();
        if (histogram.getTotalCount() == 0) {
            throw new IOException("Cannot write an empty latency histogram: " + path);
        }
        histogram.setStartTimeStamp(startEpochMillis);
        histogram.setEndTimeStamp(Math.max(startEpochMillis, endEpochMillis));
        try (PrintStream output = new PrintStream(Files.newOutputStream(path))) {
            HistogramLogWriter writer = new HistogramLogWriter(output);
            writer.outputLogFormatVersion();
            writer.outputLegend();
            writer.outputIntervalHistogram(histogram);
        }
    }
}
