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
import static org.assertj.core.api.Assertions.within;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import javax.imageio.ImageIO;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HdrHistogramRendererTest {
    private Path directory;

    @BeforeMethod
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("hdr-render-test");
    }

    @AfterMethod(alwaysRun = true)
    public void deleteDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void plotsPercentilesAndIntervalMaximaAsSvgAndPng() throws Exception {
        Path producer = writeLog(directory.resolve("producer/produce-latency.hdr"), 1_000, 2_000);
        Path consumerOne = writeLog(directory.resolve("iot-application-0/consume-latency.hdr"), 2_000, 4_000);
        Path consumerTwo = writeLog(directory.resolve("iot-application-1/consume-latency.hdr"), 4_000, 8_000);

        List<Path> charts = HdrHistogramRenderer.render(producer, List.of(consumerOne, consumerTwo),
                List.of("iot-application-0", "iot-application-1"),
                directory.resolve("latency"), 1_000, "lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04");

        assertThat(charts).containsExactly(directory.resolve("latency-percentiles.svg"),
                directory.resolve("latency-timeline.svg"), directory.resolve("latency-percentiles.png"),
                directory.resolve("latency-timeline.png"));
        // XChart draws its text as outlines; the footer below the chart is text
        for (Path chart : charts.subList(0, 2)) {
            String svg = Files.readString(chart);
            assertThat(svg).as(chart.toString()).contains("viewBox=\"0 0 " + ChartStyle.WIDTH + " ")
                    .contains("lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04");
            // No fixed size, so that the chart scales to the browser window or the page
            String root = svg.substring(svg.indexOf("<svg"), svg.indexOf('>', svg.indexOf("<svg")));
            assertThat(root).as(chart.toString()).doesNotContain(" width=").doesNotContain(" height=");
        }
        for (Path chart : charts.subList(2, 4)) {
            BufferedImage image = ImageIO.read(chart.toFile());
            assertThat(image.getWidth()).as(chart.toString()).isEqualTo(ChartStyle.WIDTH);
        }
    }

    @Test
    public void readsEachIntervalsMaximum() throws Exception {
        Path log = writeLog(directory.resolve("produce-latency.hdr"), 1_000, 2_000);

        assertThat(HdrHistogramRenderer.readIntervals(log))
                .containsExactly(new HdrHistogramRenderer.Interval(1_000, 2_000, 2_000));
    }

    @Test
    public void writesThePercentileDistributionThatPlottersRead() throws Exception {
        Path log = writeLog(directory.resolve("consume-latency.hdr"), 2_000, 4_000);

        Path distribution = HdrHistogramRenderer.writePercentileDistribution(log);

        assertThat(distribution).isEqualTo(directory.resolve("consume-latency.hgrm"));
        String text = Files.readString(distribution);
        // HdrHistogram's percentile output, in milliseconds: value, percentile, count, 1/(1-percentile)
        assertThat(text).contains("Value     Percentile TotalCount 1/(1-Percentile)");
        assertThat(text).contains("#[Max     =        4.001, Total count    =            2]");
    }

    @DataProvider
    public Object[][] percentileAxis() {
        return new Object[][] {
                {0.0, 1.0, "0%"},
                {90.0, 10.0, "90%"},
                {99.0, 100.0, "99%"},
                {99.9, 1_000.0, "99.9%"},
                {99.9999, 1_000_000.0, "99.9999%"},
        };
    }

    @Test(dataProvider = "percentileAxis")
    public void spreadsTheTailOverThePercentileAxis(double percentile, double position, String label) {
        assertThat(HdrHistogramRenderer.percentileAxisPosition(percentile))
                .isCloseTo(position, within(position * 1e-9));
        assertThat(HdrHistogramRenderer.percentileAxisLabel(position)).isEqualTo(label);
    }

    private static Path writeLog(Path path, long first, long second) throws Exception {
        Files.createDirectories(path.getParent());
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
