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
import com.fasterxml.jackson.databind.node.MissingNode;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ComparisonChartsTest {
    private static final long START = 1_000_000;
    private Path directory;

    @BeforeMethod
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("comparison-charts-test");
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
    public void writesCombinedAndSeparateChartsWithTheSameAxesForBothRuns() throws Exception {
        // A publishes 1,000 msg/s for 10 s and its consumers receive nothing; B publishes and consumes 4,000 msg/s
        // for 5 s
        Path a = writeRun("baseline/run-a", 1_000, 0, 10, 2_000, 200_000);
        Path b = writeRun("change/run-b", 4_000, 4_000, 5, 1_000, 2_000);

        List<Path> charts = ComparisonCharts.render(ComparisonCharts.read(a, "4.0.13"),
                ComparisonCharts.read(b, null), directory.resolve("charts"), false);

        List<String> names = charts.stream().map(path -> path.getFileName().toString()).toList();
        assertThat(names).containsExactly("throughput.svg", "throughput-separate.svg", "backlog.svg",
                "backlog-separate.svg", "latency-percentiles.svg", "latency-percentiles-separate.svg",
                "latency-percentiles-log.svg", "latency-percentiles-log-separate.svg");
        String combined = Files.readString(directory.resolve("charts/throughput.svg"));
        // The runs' labels: given for A, the run's name, its directory's parent, for B
        assertThat(combined).contains("A (4.0.13): Published").contains("B (change): Consumed");
        // A's lines are blue and B's orange, both thin
        String aColor = ComparisonRenderer.hex(ComparisonRenderer.A_COLOR);
        String bColor = ComparisonRenderer.hex(ComparisonRenderer.B_COLOR);
        assertThat(combined).contains("stroke=\"" + aColor + "\" stroke-width=\"" + ComparisonRenderer.STROKE)
                .contains("stroke=\"" + bColor + "\" stroke-width=\"" + ComparisonRenderer.STROKE);
        // The value axis reaches past B's 4,000 msg/s in both forms, and the separate form's panels share it
        String separate = Files.readString(directory.resolve("charts/throughput-separate.svg"));
        assertThat(yAxisLabels(combined)).containsExactly("0", "1k", "2k", "3k", "4k", "5k");
        assertThat(yAxisLabels(separate)).containsExactly("0", "1k", "2k", "3k", "4k", "5k", "0", "1k", "2k", "3k",
                "4k", "5k");
        // Each run's gateways and consumers finished, A's consumers 12 s after the start
        assertThat(combined).contains(">A: gateways finished</text>").contains(">A: consumers finished</text>")
                .contains(">B: gateways finished</text>").contains(">B: consumers finished</text>");
        assertThat(separate).contains(">gateways finished</text>").contains(">consumers finished</text>");
        // Each panel is tinted in its run's color and labeled with it
        assertThat(separate).contains("fill=\"" + ComparisonRenderer.hex(ComparisonRenderer.A_TINT) + "\"")
                .contains("fill=\"" + ComparisonRenderer.hex(ComparisonRenderer.B_TINT) + "\"")
                .contains(">A: 4.0.13</text>").contains(">B: change</text>");
        // The logarithmic latency axis spans A's 200 ms and B's 2 ms by decades
        String log = Files.readString(directory.resolve("charts/latency-percentiles-log.svg"));
        assertThat(yAxisLabels(log)).containsExactly("0.1", "1", "10", "100", "1k");
        assertThat(Files.readString(directory.resolve("charts/latency-percentiles.svg")))
                .contains("A (4.0.13): End-to-end").contains("B (change): Publish");
    }

    @Test
    public void namesTheChartsAfterTheRunsLabels() throws Exception {
        Path a = writeRun("baseline/run-a", 1_000, 0, 10, 2_000, 200_000);
        Path b = writeRun("change/run-b", 4_000, 4_000, 5, 1_000, 2_000);

        List<Path> charts = ComparisonCharts.render(ComparisonCharts.read(a, "4.0.13"),
                ComparisonCharts.read(b, "lh/branch x"), directory.resolve("charts"), true);

        assertThat(charts.stream().map(path -> path.getFileName().toString()).toList().subList(0, 2))
                .containsExactly("throughput-4.0.13-vs-lh_branch_x.svg",
                        "throughput-4.0.13-vs-lh_branch_x-separate.svg");
    }

    @Test
    public void stacksTheLabelsOfCloseMarkersInRowsOfTheirOwn() {
        ComparisonRenderer.Chart chart = new ComparisonRenderer.Chart("Throughput", "Messages per second",
                ComparisonRenderer.XAxis.seconds(0, 100), 10, false, 0, List.of(), List.of(
                        new ComparisonRenderer.Marker(ComparisonRenderer.Side.A,
                                ComparisonRenderer.Event.GATEWAYS_FINISHED, 30),
                        new ComparisonRenderer.Marker(ComparisonRenderer.Side.A,
                                ComparisonRenderer.Event.CONSUMERS_FINISHED, 31),
                        new ComparisonRenderer.Marker(ComparisonRenderer.Side.B,
                                ComparisonRenderer.Event.GATEWAYS_FINISHED, 32),
                        new ComparisonRenderer.Marker(ComparisonRenderer.Side.B,
                                ComparisonRenderer.Event.CONSUMERS_FINISHED, 90)));

        String svg = ComparisonRenderer.combined(chart, "4.0.13", "5.0.0", "");

        // The three close labels are in three rows; B's consumers' label, far from them, goes back to the first
        Matcher matcher = Pattern.compile("y=\"(\\d+)\"(?: text-anchor=\"end\")? font-size=\"12\""
                + " style=\"fill:#[0-9a-f]{6}\">([AB]: [a-z ]+)</text>").matcher(svg);
        Map<String, Integer> rows = new HashMap<>();
        while (matcher.find()) {
            rows.put(matcher.group(2), Integer.parseInt(matcher.group(1)));
        }
        assertThat(rows).hasSize(4);
        assertThat(Set.of(rows.get("A: gateways finished"), rows.get("A: consumers finished"),
                rows.get("B: gateways finished"))).hasSize(3);
        assertThat(rows.get("B: consumers finished")).isEqualTo(rows.get("A: gateways finished"));
    }

    @Test
    public void spreadsPercentilesByNinesAndSecondsByRoundSteps() {
        ComparisonRenderer.XAxis percentiles = ComparisonRenderer.XAxis.percentiles(1_000_000);
        assertThat(ComparisonRenderer.xTicks(percentiles))
                .containsExactly(1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0);
        assertThat(ComparisonRenderer.x(percentiles, 1_000)).isEqualTo((ComparisonRenderer.x(percentiles, 1)
                + ComparisonRenderer.x(percentiles, 1_000_000)) / 2);
        assertThat(ComparisonRenderer.xTicks(ComparisonRenderer.XAxis.seconds(0, 250)))
                .containsExactly(0.0, 50.0, 100.0, 150.0, 200.0, 250.0);
    }

    // The value axis's labels, in the order they are drawn
    private static List<String> yAxisLabels(String svg) {
        Matcher matcher = Pattern.compile("text-anchor=\"end\" font-size=\"12\">([^<]+)</text>").matcher(svg);
        List<String> labels = new ArrayList<>();
        while (matcher.find()) {
            labels.add(matcher.group(1));
        }
        return labels;
    }

    /**
     * Writes a run directory: topic stats sampled once a second, the gateways' summary and latency logs, and one
     * application's end-to-end latency log.
     */
    private Path writeRun(String name, int publishedPerSecond, int dispatchedPerSecond, int seconds,
                          long publishMicros, long endToEndMicros) throws Exception {
        Path run = directory.resolve(name);
        Files.createDirectories(run.resolve("gateways"));
        StringBuilder csv = new StringBuilder("epochMillis,topic,subscription,msgBacklog,msgInCounter,msgOutCounter\n");
        for (int second = 0; second <= seconds + 2; second++) {
            long in = (long) publishedPerSecond * Math.min(second, seconds);
            long out = (long) dispatchedPerSecond * Math.min(second, seconds);
            csv.append(START + second * 1_000L).append(",persistent://public/default/t,sub,").append(in - out)
                    .append(',').append(in).append(',').append(out).append('\n');
        }
        Files.writeString(run.resolve(RunReport.TOPIC_STATS_FILE), csv);
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"measurementStartEpochMs\":" + START
                + ",\"measurementEndEpochMs\":" + (START + seconds * 1_000L) + "}");
        Path application = RunReport.applicationDirectory(run, MissingNode.getInstance(), 0);
        Files.createDirectories(application);
        Files.writeString(application.resolve("application-summary.json"),
                "{\"lastMeasurementMessageReceivedEpochMs\":" + (START + (seconds + 2) * 1_000L) + "}");
        writeLog(run.resolve("gateways/gateways-latency.hdr"), publishMicros);
        writeLog(RunReport.applicationDirectory(run, MissingNode.getInstance(), 0)
                .resolve("application-latency.hdr"), endToEndMicros);
        return run;
    }

    private static void writeLog(Path path, long micros) throws Exception {
        Files.createDirectories(path.getParent());
        Histogram histogram = new Histogram(3);
        histogram.recordValue(micros / 2);
        histogram.recordValue(micros);
        histogram.setStartTimeStamp(START);
        histogram.setEndTimeStamp(START + 1_000);
        try (PrintStream output = new PrintStream(Files.newOutputStream(path))) {
            HistogramLogWriter writer = new HistogramLogWriter(output);
            writer.outputLogFormatVersion();
            writer.outputLegend();
            writer.outputIntervalHistogram(histogram);
        }
    }
}
