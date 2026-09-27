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
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.testng.annotations.Test;

public class HdrLatencyRecorderTest {
    @Test
    public void writesEmptyHistogramForRunWithoutMeasuredMessages() throws Exception {
        Path output = Files.createTempFile("empty-latency", ".hdr");
        try {
            new HdrLatencyRecorder().write(output, 0, 0);
            try (HistogramLogReader reader = new HistogramLogReader(output.toFile())) {
                Histogram histogram = (Histogram) reader.nextIntervalHistogram();
                assertThat(histogram.getTotalCount()).isZero();
                assertThat(reader.nextIntervalHistogram()).isNull();
            }
        } finally {
            Files.deleteIfExists(output);
        }
    }

    @Test
    public void writesMicrosecondHistogramLog() throws Exception {
        Path output = Files.createTempFile("latency", ".hdr");
        try {
            HdrLatencyRecorder recorder = new HdrLatencyRecorder();
            recorder.recordNanos(1_500_000);
            recorder.recordMillis(2);
            recorder.write(output, 1_000, 2_000);

            try (HistogramLogReader reader = new HistogramLogReader(output.toFile())) {
                Histogram histogram = (Histogram) reader.nextIntervalHistogram();
                assertThat(histogram.getTotalCount()).isEqualTo(2);
                assertThat(histogram.getStartTimeStamp()).isEqualTo(1_000);
                assertThat(histogram.getEndTimeStamp()).isEqualTo(2_000);
                assertThat(histogram.getMinValue()).isBetween(1_499L, 1_500L);
                assertThat(histogram.getMaxValue()).isBetween(2_000L, 2_001L);
                assertThat(reader.nextIntervalHistogram()).isNull();
            }
        } finally {
            Files.deleteIfExists(output);
        }
    }
}
