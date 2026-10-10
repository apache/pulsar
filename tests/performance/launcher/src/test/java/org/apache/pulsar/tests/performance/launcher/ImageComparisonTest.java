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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.apache.pulsar.tests.performance.launcher.ImageComparison.Measure;
import org.apache.pulsar.tests.performance.launcher.ImageComparison.Measures;
import org.apache.pulsar.tests.performance.launcher.ImageComparison.Result;
import org.apache.pulsar.tests.performance.launcher.ImageComparison.Run;
import org.apache.pulsar.tests.performance.launcher.ImageComparison.Side;
import org.testng.annotations.Test;

public class ImageComparisonTest {
    private static final Side A = new Side("A", "apachepulsar/pulsar:4.0.14", "apachepulsar/java-test-image:cluster-a");
    private static final Side B = new Side("B", "apachepulsar/pulsar:5.0.0", "apachepulsar/java-test-image:cluster-b");
    private static final Side CHECKOUT = new Side("B", null, null);

    @Test
    public void alternatesWhichSideRunsFirst() {
        assertThat(ImageComparison.runOrder(A, B, 3)).containsExactly(A, B, B, A, A, B);
    }

    @Test
    public void labelsASideByItsImageTag() {
        assertThat(A.label()).isEqualTo("4.0.14");
        assertThat(new Side("A", "localhost:5000/pulsar", "x").label()).isEqualTo("pulsar");
        assertThat(CHECKOUT.label()).isEqualTo("checkout");
    }

    @Test
    public void runsTheLauncherWithTheSidesClusterImageAndThePerformanceProperties() {
        Map<Object, Object> properties = Map.of(
                "performance.reports.dir", "/reports",
                "performance.compare.repetitions", "3",
                "performance.cluster.pulsarImage", "ignored",
                "user.home", "/home/u");
        String[] launcherArgs = {"--scenario", "s.yaml", "--name", "ab"};

        assertThat(ImageComparison.command(A, "/jdk", "cp", properties, launcherArgs)).containsExactly(
                Path.of("/jdk", "bin", "java").toString(),
                "-Dperformance.reports.dir=/reports",
                "-Dperformance.cluster.pulsarImage=apachepulsar/pulsar:4.0.14",
                "-Dperformance.cluster.image=apachepulsar/java-test-image:cluster-a",
                "-cp", "cp", PerformanceLauncher.class.getName(), "--scenario", "s.yaml", "--name", "ab");
        // the checkout runs its own test image for the cluster too
        assertThat(ImageComparison.command(CHECKOUT, "/jdk", "cp", properties, launcherArgs))
                .doesNotContain("-Dperformance.cluster.pulsarImage=apachepulsar/pulsar:4.0.14")
                .noneMatch(arg -> arg.startsWith("-Dperformance.cluster."));
    }

    @Test
    public void readsARunsMeasures() throws IOException {
        Path run = Files.createTempDirectory("image-comparison");
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"messagesPerSecond\": 110548.4}");
        // 1,000 latencies of 1 to 1,000 ms, so that the 99th percentile is 990 ms
        writeLatencies(run.resolve("gateways/gateways-latency.hdr"), 1);
        for (String application : List.of("iot-application-0", "iot-application-1")) {
            Files.createDirectories(run.resolve("applications").resolve(application));
        }
        writeLatencies(run.resolve("applications/iot-application-0/application-latency.hdr"), 2);
        writeLatencies(run.resolve("applications/iot-application-1/application-latency.hdr"), 3);
        Files.writeString(run.resolve("container-summary.json"), "{\"containers\": {"
                + "\"broker-0\": {\"cpuSecondsPerMillionMessages\": 40.0},"
                + "\"broker-1\": {\"cpuSecondsPerMillionMessages\": 2.5},"
                + "\"bookie-0\": {\"cpuSecondsPerMillionMessages\": 30.0}}}");

        Measures measures = ImageComparison.measures(run);
        assertThat(measures.throughput()).isEqualTo(110548.4);
        assertThat(measures.publishP99Millis()).isCloseTo(990, within(1.0));
        // the slowest application's
        assertThat(measures.endToEndP99Millis()).isCloseTo(2970, within(3.0));
        assertThat(measures.brokerCpuSecondsPerMillion()).isEqualTo(42.5);
    }

    @Test
    public void aMeasureWithoutItsOutputsIsNaN() throws IOException {
        Path run = Files.createTempDirectory("image-comparison");
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "not json");

        PrintStream err = System.err;
        try {
            System.setErr(new PrintStream(PrintStream.nullOutputStream()));
            assertThat(ImageComparison.measures(run))
                    .isEqualTo(new Measures(Double.NaN, Double.NaN, Double.NaN, Double.NaN));
        } finally {
            System.setErr(err);
        }
    }

    // a latency log as the gateways and the applications write it, in microseconds: 1 to 1,000 ms times the factor
    private static void writeLatencies(Path log, int factor) throws IOException {
        Histogram histogram = new Histogram(3);
        for (int millis = 1; millis <= 1000; millis++) {
            histogram.recordValue(millis * factor * 1000L);
        }
        try (PrintStream out = new PrintStream(Files.newOutputStream(log))) {
            HistogramLogWriter writer = new HistogramLogWriter(out);
            writer.outputLogFormatVersion();
            writer.outputLegend();
            writer.outputIntervalHistogram(histogram);
        }
    }

    @Test
    public void recognizesARunWhoseApplicationsReceivedMessagesOutOfOrder() throws IOException {
        Path run = Files.createTempDirectory("image-comparison");
        for (String application : List.of("iot-application-0", "iot-application-1")) {
            Files.createDirectories(run.resolve("applications").resolve(application));
        }
        Files.writeString(run.resolve("applications/iot-application-0/application-summary.json"),
                "{\"duplicates\": 3, \"orderingViolations\": 0, \"invalidMessages\": 0}");
        Path second = run.resolve("applications/iot-application-1/application-summary.json");
        Files.writeString(second, "{\"duplicates\": 0, \"orderingViolations\": 0, \"invalidMessages\": 0}");
        // duplicates are a redelivery, which doesn't make the measures wrong
        assertThat(ImageComparison.deliveredInvalidly(run)).isFalse();

        Files.writeString(second, "{\"duplicates\": 0, \"orderingViolations\": 2, \"invalidMessages\": 0}");
        assertThat(ImageComparison.deliveredInvalidly(run)).isTrue();
        Files.writeString(second, "{\"duplicates\": 0, \"orderingViolations\": 0, \"invalidMessages\": 1}");
        assertThat(ImageComparison.deliveredInvalidly(run)).isTrue();
    }

    @Test
    public void picksTheMedianValidRunOfASide() {
        Run a1 = run(A, 1, Result.VALID, 100);
        Run a2 = run(A, 4, Result.VALID, 120);
        Run a3 = run(A, 5, Result.VALID, 110);
        Run a4 = run(A, 7, Result.FAILED, 999);
        Run a5 = run(A, 8, Result.INVALID, 105);
        Run a6 = run(A, 9, Result.VALID, Double.NaN);
        Run b1 = run(B, 2, Result.VALID, 200);
        Run b2 = run(B, 3, Result.VALID, 190);
        List<Run> runs = List.of(a1, b1, b2, a2, a3, a4, a5, a6);

        // neither the failed and invalid runs nor the run without the measure count
        assertThat(ImageComparison.median(runs, A, Measure.THROUGHPUT)).isSameAs(a3);
        // the lower of the middle two
        assertThat(ImageComparison.median(runs, B, Measure.THROUGHPUT)).isSameAs(b2);
        assertThat(ImageComparison.median(List.of(a4, a5, a6), A, Measure.THROUGHPUT)).isNull();
    }

    private static Run run(Side side, int order, Result result, double throughput) {
        return new Run(side, order, Path.of("/reports/2026-10-06/pulsar-x/ab/10-06-10-00-0" + order), result,
                new Measures(throughput, 10, 20, 30));
    }

    @Test
    public void writesTheComparisonBesideTheRunsDays() {
        Path firstRun = Path.of("/reports/2026-10-06/pulsar-4.0.14/ab/10-06-10-00-01");

        assertThat(ImageComparison.outputDirectory(firstRun, ZonedDateTime.parse("2026-10-06T09:59:58+03:00")))
                .isEqualTo(Path.of("/reports/2026-10-06/comparisons/ab/10-06-09-59-58"));
    }

    @Test
    public void summarizesTheMedianRunsAndEveryRun() {
        Measures none = new Measures(Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        Run a = new Run(A, 1, Path.of("/reports/2026-10-06/pulsar-4.0.14/ab/10-06-10-00-01"), Result.VALID,
                new Measures(100_000, 10, 20, 40));
        Run b = new Run(B, 2, Path.of("/reports/2026-10-06/pulsar-5.0.0/ab/10-06-10-05-01"), Result.VALID,
                new Measures(110_000, 9, 18, 36));
        Run failed = new Run(B, 3, Path.of("/reports/2026-10-06/pulsar-5.0.0/ab/10-06-10-10-01"), Result.FAILED,
                none);
        Run invalid = new Run(A, 4, Path.of("/reports/2026-10-06/pulsar-4.0.14/ab/10-06-10-15-01"), Result.INVALID,
                new Measures(90_000, 10, 20, 40));

        String summary = ImageComparison.summary(A, B, new String[] {"--scenario", "s.yaml"}, 2,
                Measure.THROUGHPUT, List.of(a, b, failed, invalid), a, b,
                List.of("throughput-4.0.14-vs-5.0.0.svg", "latency-percentiles-log-4.0.14-vs-5.0.0.svg"),
                Path.of("/reports/2026-10-06/comparisons/ab/10-06-09-59-58"));

        assertThat(summary)
                .contains("# Comparison: 4.0.14 (A) and 5.0.0 (B)")
                .contains("- **A:** `apachepulsar/pulsar:4.0.14`")
                .contains("| Throughput, msg/s | 100,000 | 110,000 | +10.0 % |")
                .contains("| Broker CPU per million messages, s | 40.0 | 36.0 | -10.0 % |")
                .contains("| 1 | A | [Report of run 1 (A)](../../../pulsar-4.0.14/ab/10-06-10-00-01/README.md) | median"
                        + " |")
                .contains("each side's median valid run by throughput, the lower of the middle two for an even count,"
                        + " which the charts compare")
                .contains("| 3 | B | [Console log of run 3 (B)]"
                        + "(../../../pulsar-5.0.0/ab/10-06-10-10-01/console.log.txt) | failed | – | – | – | – |")
                .contains("| 4 | A | [Report of run 4 (A)](../../../pulsar-4.0.14/ab/10-06-10-15-01/README.md)"
                        + " | invalid: ordering violations or invalid messages | 90,000 |")
                .contains("![Throughput of 4.0.14 and 5.0.0](throughput-4.0.14-vs-5.0.0.svg)")
                .contains("![Latency percentiles on a logarithmic scale of 4.0.14 and 5.0.0]"
                        + "(latency-percentiles-log-4.0.14-vs-5.0.0.svg)");
    }

    @Test
    public void tellsTheSidesApartWhenTheirImagesHaveTheSameTag() {
        Side mirror = new Side("B", "localhost:5000/pulsar:4.0.14", "x");
        assertThat(ImageComparison.comparisonLabel(A, mirror)).isEqualTo("4.0.14-B");
        assertThat(ImageComparison.comparisonLabel(A, B)).isEqualTo("5.0.0");
    }

    @Test
    public void ordersTheChartsAsARunsReport() {
        List<String> charts = new ArrayList<>(List.of("backlog-a-vs-b.svg", "latency-percentiles-log-a-vs-b.svg",
                "throughput-a-vs-b.svg", "latency-percentiles-a-vs-b.svg"));
        charts.sort(Comparator.comparingInt(ImageComparison::chartRank));
        assertThat(charts).containsExactly("throughput-a-vs-b.svg", "latency-percentiles-a-vs-b.svg",
                "latency-percentiles-log-a-vs-b.svg", "backlog-a-vs-b.svg");
    }

    @Test
    public void rejectsAnUnknownMedianMeasure() {
        assertThat(Measure.of("e2e-p99")).isEqualTo(Measure.END_TO_END_P99);
        assertThatThrownBy(() -> Measure.of("latency")).hasMessageContaining("throughput, publish-p99");
    }
}
