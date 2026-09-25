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

import static org.assertj.core.api.Assertions.assertThat;
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
            assertThat(svg).contains("width=\"" + ChartStyle.WIDTH + "\"");
            assertThat(svg.split("<polyline", -1).length - 1).isEqualTo(3);
            // The producers' line and its legend are dotted, the consumers' solid
            assertThat(svg.split("stroke-dasharray=\"0.1 5\"", -1).length - 1).isEqualTo(2);
            assertThat(svg).contains("text-anchor=\"end\" font-size=\"10\">lh-branch@1ebd73f2 2026-09-25"
                    + " 13:35:22-13:39:04</text>");
            assertThat(svg).contains("Seconds since the measurement start");
            assertThat(svg).contains("producers finished");
            assertThat(svg).contains(">warmup<");
            assertThat(svg).contains(">Consumers (dispatched)<");
            assertThat(Files.size(directory.resolve("throughput.png"))).isPositive();
        } finally {
            for (String file : List.of("throughput.svg", "throughput.png")) {
                Files.deleteIfExists(directory.resolve(file));
            }
            Files.delete(directory);
        }
    }

    @Test
    public void choosesReadableAxisSteps() {
        assertThat(TimeSeriesRenderer.niceCeiling(118_000)).isEqualTo(200_000.0);
        assertThat(TimeSeriesRenderer.niceCeiling(4.2)).isEqualTo(5.0);
        assertThat(TimeSeriesRenderer.axisMaximum(201_700)).isEqualTo(250_000.0);
        assertThat(TimeSeriesRenderer.axisMaximum(118_000)).isEqualTo(150_000.0);
        assertThat(TimeSeriesRenderer.axisMaximum(53_000)).isEqualTo(60_000.0);
        assertThat(TimeSeriesRenderer.compact(120_000)).isEqualTo("120k");
        assertThat(TimeSeriesRenderer.compact(1_500_000)).isEqualTo("1.5M");
        assertThat(TimeSeriesRenderer.compact(40)).isEqualTo("40");
    }
}
