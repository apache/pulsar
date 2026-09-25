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
package org.apache.pulsar.tests.performance.report;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.testng.annotations.Test;

public class TimeSeriesRendererTest {
    @Test
    public void rendersOneLinePerSeriesSegmentWithTheHistogramWidth() throws IOException {
        Path directory = Files.createTempDirectory("time-series-test");
        try {
            double[] seconds = {-2, -1, 0, 1, 2, 3, 4};
            List<TimeSeriesRenderer.Series> series = List.of(
                    new TimeSeriesRenderer.Series("Producers (published)",
                            new double[] {Double.NaN, 90_000, 100_000, 110_000, 105_000, 0, 0}, true),
                    // A gap splits the line in two
                    new TimeSeriesRenderer.Series("Consumers (dispatched)",
                            new double[] {Double.NaN, 80_000, Double.NaN, 100_000, 104_000, 20_000, 0}));

            TimeSeriesRenderer.render(directory.resolve("throughput"), "Throughput", "Messages per second", seconds,
                    series, 2.5, "lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04");

            String svg = Files.readString(directory.resolve("throughput.svg"));
            assertTrue(svg.contains("width=\"" + HdrHistogramRenderer.WIDTH + "\""), svg);
            assertEquals(svg.split("<polyline", -1).length - 1, 3, svg);
            // The producers' line and its legend are dotted, the consumers' solid
            assertEquals(svg.split("stroke-dasharray=\"0.1 5\"", -1).length - 1, 2, svg);
            assertTrue(svg.contains("text-anchor=\"end\" font-size=\"10\">lh-branch@1ebd73f2 2026-09-25"
                    + " 13:35:22-13:39:04</text>"), svg);
            assertTrue(svg.contains("Seconds since the measurement start"), svg);
            assertTrue(svg.contains("producers finished"), svg);
            assertTrue(svg.contains(">warmup<"), svg);
            assertTrue(svg.contains(">Consumers (dispatched)<"), svg);
            assertTrue(Files.size(directory.resolve("throughput.png")) > 0);
        } finally {
            for (String file : List.of("throughput.svg", "throughput.png")) {
                Files.deleteIfExists(directory.resolve(file));
            }
            Files.delete(directory);
        }
    }

    @Test
    public void choosesReadableAxisSteps() {
        assertEquals(TimeSeriesRenderer.niceCeiling(118_000), 200_000.0);
        assertEquals(TimeSeriesRenderer.niceCeiling(4.2), 5.0);
        assertEquals(TimeSeriesRenderer.axisMaximum(201_700), 250_000.0);
        assertEquals(TimeSeriesRenderer.axisMaximum(118_000), 150_000.0);
        assertEquals(TimeSeriesRenderer.axisMaximum(53_000), 60_000.0);
        assertEquals(TimeSeriesRenderer.compact(120_000), "120k");
        assertEquals(TimeSeriesRenderer.compact(1_500_000), "1.5M");
        assertEquals(TimeSeriesRenderer.compact(40), "40");
    }
}
