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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.testng.annotations.Test;

public class HdrHistogramRendererTest {
    @Test
    public void rendersMergedProducerAndConsumerHistograms() throws Exception {
        Path directory = Files.createTempDirectory("hdr-render-test");
        try {
            Path producer = writeHistogram(directory.resolve("produce.hdr"), 1_000, 2_000);
            Path consumerOne = writeHistogram(directory.resolve("consume-1.hdr"), 2_000, 4_000);
            Path consumerTwo = writeHistogram(directory.resolve("consume-2.hdr"), 4_000, 8_000);
            Path prefix = directory.resolve("latency");

            HdrHistogramRenderer.render(producer, List.of(consumerOne, consumerTwo), prefix, "Test latency",
                    "lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04");

            byte[] png = Files.readAllBytes(directory.resolve("latency.png"));
            assertTrue(png.length > 8);
            assertEquals(List.of(png[0], png[1], png[2], png[3]),
                    List.of((byte) 0x89, (byte) 'P', (byte) 'N', (byte) 'G'));
            String svg = Files.readString(directory.resolve("latency.svg"));
            assertTrue(svg.contains("Test latency"));
            assertTrue(svg.contains("Produce · send completion"));
            assertTrue(svg.contains("Consume · publish to listener"));
            assertTrue(svg.contains("n=4"));
            assertTrue(svg.contains(">lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04</text>"), svg);
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static Path writeHistogram(Path path, long first, long second) throws Exception {
        Histogram histogram = new Histogram(3);
        histogram.recordValue(first);
        histogram.recordValue(second);
        histogram.setStartTimeStamp(1_000);
        histogram.setEndTimeStamp(2_000);
        try (PrintStream output = new PrintStream(Files.newOutputStream(path))) {
            HistogramLogWriter writer = new HistogramLogWriter(output);
            writer.outputLogFormatVersion();
            writer.outputLegend();
            writer.outputIntervalHistogram(histogram);
        }
        return path;
    }
}
