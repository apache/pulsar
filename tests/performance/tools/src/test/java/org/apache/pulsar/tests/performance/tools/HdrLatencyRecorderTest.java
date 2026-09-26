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

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class HdrLatencyRecorderTest {
    @Test
    public void writesNoIntervalForRunWithoutMeasuredMessages() throws Exception {
        Path output = Files.createTempFile("empty-latency", ".hdr");
        try {
            new HdrLatencyRecorder(output).close();

            assertThat(read(output)).isEmpty();
        } finally {
            Files.deleteIfExists(output);
        }
    }

    @Test
    public void writesMicrosecondHistogramLog() throws Exception {
        Path output = Files.createTempFile("latency", ".hdr");
        try {
            long before = System.currentTimeMillis();
            HdrLatencyRecorder recorder = new HdrLatencyRecorder(output);
            recorder.recordNanos(1_500_000);
            recorder.recordMillis(2);
            recorder.close();

            List<Histogram> intervals = read(output);
            assertThat(intervals).hasSize(1);
            Histogram histogram = intervals.get(0);
            assertThat(histogram.getTotalCount()).isEqualTo(2);
            assertThat(histogram.getStartTimeStamp()).isGreaterThanOrEqualTo(before);
            assertThat(histogram.getEndTimeStamp()).isGreaterThanOrEqualTo(histogram.getStartTimeStamp());
            assertThat(histogram.getMinValue()).isBetween(1_499L, 1_500L);
            assertThat(histogram.getMaxValue()).isBetween(2_000L, 2_001L);
        } finally {
            Files.deleteIfExists(output);
        }
    }

    @Test
    public void writesAnIntervalPerSecond() throws Exception {
        Path output = Files.createTempFile("latency-intervals", ".hdr");
        try {
            HdrLatencyRecorder recorder = new HdrLatencyRecorder(output);
            recorder.recordMillis(1);
            // The first interval is written about a second later; the next value goes into the second one
            Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions()
                    .untilAsserted(() -> assertThat(read(output)).hasSize(1));
            recorder.recordMillis(3);
            recorder.close();

            List<Histogram> intervals = read(output);
            assertThat(intervals).hasSize(2);
            assertThat(intervals.get(0).getMaxValue()).isBetween(1_000L, 1_001L);
            assertThat(intervals.get(1).getMaxValue()).isBetween(3_000L, 3_002L);
            assertThat(intervals.get(1).getStartTimeStamp()).isGreaterThanOrEqualTo(intervals.get(0).getEndTimeStamp());
        } finally {
            Files.deleteIfExists(output);
        }
    }

    private static List<Histogram> read(Path log) throws Exception {
        List<Histogram> intervals = new ArrayList<>();
        try (HistogramLogReader reader = new HistogramLogReader(log.toFile())) {
            EncodableHistogram interval;
            while ((interval = reader.nextIntervalHistogram()) != null) {
                intervals.add((Histogram) interval);
            }
        }
        return intervals;
    }
}
