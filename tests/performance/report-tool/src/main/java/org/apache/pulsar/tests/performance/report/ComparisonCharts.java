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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.Chart;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.Event;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.Line;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.Marker;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.Side;
import org.apache.pulsar.tests.performance.report.ComparisonRenderer.XAxis;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Compares two finished IoT runs, a baseline (A) and a comparison (B), in charts with the same axes for both: the
 * throughput (published and consumed), the backlog, and the publish and end-to-end latency by percentile, on a
 * linear and on a logarithmic axis, since latencies that differ by orders of magnitude flatten on a linear one. Each
 * chart is written twice: {@code <chart>-<baselineLabel>-vs-<comparisonLabel>.svg} with both runs in one diagram,
 * and {@code <chart>-<baselineLabel>-vs-<comparisonLabel>-separate.svg} with A above B in panels of their own, unless
 * {@code --no-labels-in-file-names} leaves the labels out. The charts are read from the run
 * directories' files: {@code topic-stats.csv}, {@code gateways/gateways-summary.json}, the applications' summaries and
 * the HDR latency logs.
 */
@Command(name = "compare-runs", mixinStandardHelpOptions = true,
        description = "Chart a baseline run (A) against a comparison run (B) with the same axes for both")
public final class ComparisonCharts implements Callable<Integer> {
    static final String THROUGHPUT = "throughput";
    static final String BACKLOG = "backlog";
    static final String LATENCY = "latency-percentiles";
    static final String LATENCY_LOG = "latency-percentiles-log";
    static final String SEPARATE_SUFFIX = "-separate";
    // The percentile axis ends at six nines, as the run report's latency chart does
    private static final double MAX_PERCENTILE = 99.9999;
    private static final int PERCENTILE_TICKS_PER_HALF_DISTANCE = 5;
    // The lowest value of the logarithmic latency axis, in milliseconds
    private static final double LOG_LATENCY_MIN = 0.1;

    @Option(names = "--baseline", required = true, description = "The baseline (A) run directory")
    private Path baseline;

    @Option(names = "--comparison", required = true, description = "The comparison (B) run directory")
    private Path comparison;

    @Option(names = "--baseline-label", description = "A's name in the charts; defaults to its run's name")
    private String baselineLabel;

    @Option(names = "--comparison-label", description = "B's name in the charts; defaults to its run's name")
    private String comparisonLabel;

    @Option(names = "--output", required = true, description = "The directory to write the charts to")
    private Path output;

    @Option(names = "--labels-in-file-names", negatable = true, defaultValue = "true", fallbackValue = "true",
            description = "Name the charts <chart>-<A label>-vs-<B label>[-separate].svg; with"
                    + " --no-labels-in-file-names, <chart>[-separate].svg. Default: ${DEFAULT-VALUE}")
    private boolean labelsInFileNames;

    /**
     * A run's data, with times in seconds since its measurement start.
     *
     * @param gatewaysFinishedSeconds when the gateways finished publishing
     * @param consumersFinishedSeconds when the last application received its last measured message, {@link Double#NaN}
     *     when unknown
     * @param consumed the rate that the subscriptions' consumers received messages at, summed over the subscriptions
     * @param latencies the publish latency first, then each application's end-to-end latency, by percentile
     */
    record RunData(String label, String footer, double gatewaysFinishedSeconds, double consumersFinishedSeconds,
                   double[] seconds, double[] published, double[] consumed, double[] backlog, List<Curve> latencies) {
    }

    /** A named line of a latency chart: percentile axis positions and latencies in milliseconds. */
    record Curve(String name, double[] positions, double[] millis) {
    }

    private ComparisonCharts() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ComparisonCharts()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        RunData a = read(baseline, baselineLabel);
        RunData b = read(comparison, comparisonLabel);
        for (Path chart : render(a, b, output, labelsInFileNames)) {
            System.out.println(chart);
        }
        return 0;
    }

    /**
     * Reads a run directory.
     *
     * @param label the run's name in the charts, or {@code null} for the run's name, its directory's parent
     */
    static RunData read(Path runDirectory, String label) throws IOException {
        Path run = runDirectory.toAbsolutePath().normalize();
        JsonNode producer = new ObjectMapper().readTree(run.resolve("gateways/gateways-summary.json").toFile());
        long start = producer.path("measurementStartEpochMs").asLong();
        double finished = (producer.path("measurementEndEpochMs").asLong() - start) / 1000.0;
        RunReport.Samples samples = RunReport.readSamples(run.resolve(RunReport.TOPIC_STATS_FILE));
        double[] seconds = new double[samples.epochMillis().length];
        for (int round = 0; round < seconds.length; round++) {
            seconds[round] = (samples.epochMillis()[round] - start) / 1000.0;
        }
        String name = label != null ? label : run.getParent().getFileName().toString();
        return new RunData(name, name + " " + run.getFileName(), finished, consumersFinished(run, start), seconds,
                samples.published(), sum(samples.dispatched(), seconds.length), sum(samples.backlog(), seconds.length),
                latencies(run));
    }

    // When the last application received its last measured message, from the applications' summaries
    private static double consumersFinished(Path run, long start) throws IOException {
        JsonNode workload = workload(run);
        ObjectMapper mapper = new ObjectMapper();
        long last = -1;
        for (int application = 0; ; application++) {
            Path summary = RunReport.applicationDirectory(run, workload, application)
                    .resolve("application-summary.json");
            if (!Files.isRegularFile(summary)) {
                break;
            }
            last = Math.max(last, mapper.readTree(summary.toFile()).path("lastMeasurementMessageReceivedEpochMs")
                    .asLong(-1));
        }
        return last < 0 ? Double.NaN : (last - start) / 1000.0;
    }

    // The run's IoT workload settings, which name the applications' directories
    private static JsonNode workload(Path run) throws IOException {
        Path resolvedConfig = run.resolve(RunReport.RESOLVED_CONFIG);
        return Files.isRegularFile(resolvedConfig)
                ? new YAMLMapper().readTree(resolvedConfig.toFile()).path("workloads").path("iotTelemetry")
                : MissingNode.getInstance();
    }

    /**
     * Writes the charts.
     *
     * @param labelsInFileNames whether the file names include {@code -<baselineLabel>-vs-<comparisonLabel>}
     * @return the charts written: each chart's combined form followed by its separate form
     */
    static List<Path> render(RunData a, RunData b, Path output, boolean labelsInFileNames) throws IOException {
        Files.createDirectories(output);
        String labels = labelsInFileNames ? "-" + fileNamePart(a.label()) + "-vs-" + fileNamePart(b.label()) : "";
        String footer = "A: " + a.footer() + "    B: " + b.footer();
        double end = Math.max(last(a.seconds()), last(b.seconds()));
        XAxis time = XAxis.seconds(0, end);
        List<Line> throughput = List.of(
                new Line("Published", Side.A, a.seconds(), a.published(), true),
                new Line("Consumed", Side.A, a.seconds(), a.consumed(), false),
                new Line("Published", Side.B, b.seconds(), b.published(), true),
                new Line("Consumed", Side.B, b.seconds(), b.consumed(), false));
        List<Line> backlog = List.of(
                new Line("Backlog", Side.A, a.seconds(), a.backlog(), false),
                new Line("Backlog", Side.B, b.seconds(), b.backlog(), false));
        List<Line> latency = new ArrayList<>();
        addCurves(latency, a, Side.A);
        addCurves(latency, b, Side.B);
        XAxis percentiles = XAxis.percentiles(HdrHistogramRenderer.percentileAxisPosition(MAX_PERCENTILE));
        double latencyMax = TimeSeriesRenderer.axisMaximum(max(latency, percentiles) * 1.05);
        List<Marker> markers = List.of(
                new Marker(Side.A, Event.GATEWAYS_FINISHED, a.gatewaysFinishedSeconds()),
                new Marker(Side.A, Event.CONSUMERS_FINISHED, a.consumersFinishedSeconds()),
                new Marker(Side.B, Event.GATEWAYS_FINISHED, b.gatewaysFinishedSeconds()),
                new Marker(Side.B, Event.CONSUMERS_FINISHED, b.consumersFinishedSeconds()));
        List<Path> written = new ArrayList<>();
        write(written, output, THROUGHPUT + labels, new Chart("Throughput",
                "Messages per second, sampled once per second", time,
                TimeSeriesRenderer.axisMaximum(max(throughput, time) * 1.05), false, 0, throughput, markers),
                a, b, footer);
        write(written, output, BACKLOG + labels, new Chart("Backlog",
                "Messages in the backlog, sampled once per second", time,
                TimeSeriesRenderer.axisMaximum(max(backlog, time) * 1.05), false, 0, backlog, markers),
                a, b, footer);
        write(written, output, LATENCY + labels, new Chart("Latency by percentile", "Latency (ms)", percentiles,
                latencyMax, false, 0, latency, List.of()), a, b, footer);
        write(written, output, LATENCY_LOG + labels, new Chart("Latency by percentile",
                "Latency (ms), logarithmic scale", percentiles, Math.pow(10, Math.ceil(Math.log10(latencyMax))), true,
                LOG_LATENCY_MIN, latency, List.of()), a, b, footer);
        return written;
    }

    // A label as part of a file name: letters, digits, dots and underscores, anything else as an underscore
    static String fileNamePart(String label) {
        return label.replaceAll("[^A-Za-z0-9._]", "_");
    }

    private static void write(List<Path> written, Path output, String name, Chart chart, RunData a, RunData b,
                              String footer) throws IOException {
        Path combined = output.resolve(name + ".svg");
        Path separate = output.resolve(name + SEPARATE_SUFFIX + ".svg");
        Files.writeString(combined, ComparisonRenderer.combined(chart, a.label(), b.label(), footer));
        Files.writeString(separate, ComparisonRenderer.separate(chart, a.label(), b.label(), footer));
        written.add(combined);
        written.add(separate);
    }

    private static void addCurves(List<Line> lines, RunData run, Side side) {
        for (int index = 0; index < run.latencies().size(); index++) {
            Curve curve = run.latencies().get(index);
            // Publish dashed, as the published rate is in the throughput chart
            lines.add(new Line(curve.name(), side, curve.positions(), curve.millis(), index == 0));
        }
    }

    // The publish latency, then each application's end-to-end latency; one application is "End-to-end"
    private static List<Curve> latencies(Path run) throws IOException {
        List<Curve> curves = new ArrayList<>();
        Path publish = run.resolve("gateways/gateways-latency.hdr");
        if (!Files.isRegularFile(publish)) {
            return curves;
        }
        curves.add(curve("Publish", publish));
        JsonNode workload = workload(run);
        List<Path> logs = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int application = 0; ; application++) {
            Path directory = RunReport.applicationDirectory(run, workload, application);
            Path log = directory.resolve("application-latency.hdr");
            if (!Files.isRegularFile(log)) {
                break;
            }
            logs.add(log);
            names.add(directory.getFileName().toString());
        }
        for (int index = 0; index < logs.size(); index++) {
            curves.add(curve(logs.size() == 1 ? "End-to-end" : "End-to-end, " + names.get(index), logs.get(index)));
        }
        return curves;
    }

    private static Curve curve(String name, Path log) throws IOException {
        Histogram histogram = HdrHistogramRenderer.readMerged(List.of(log));
        List<Double> positions = new ArrayList<>();
        List<Double> millis = new ArrayList<>();
        for (HistogramIterationValue value : histogram.percentiles(PERCENTILE_TICKS_PER_HALF_DISTANCE)) {
            double percentile = Math.min(value.getPercentileLevelIteratedTo(), MAX_PERCENTILE);
            positions.add(HdrHistogramRenderer.percentileAxisPosition(percentile));
            millis.add(value.getValueIteratedTo() / 1000.0);
            if (percentile >= MAX_PERCENTILE) {
                break;
            }
        }
        return new Curve(name, positions.stream().mapToDouble(Double::doubleValue).toArray(),
                millis.stream().mapToDouble(Double::doubleValue).toArray());
    }

    // The subscriptions' values summed per sample, NaN where none has a value
    private static double[] sum(Map<String, double[]> bySubscription, int rounds) {
        double[] total = new double[rounds];
        Arrays.fill(total, Double.NaN);
        for (double[] values : bySubscription.values()) {
            for (int round = 0; round < rounds; round++) {
                if (!Double.isNaN(values[round])) {
                    total[round] = (Double.isNaN(total[round]) ? 0 : total[round]) + values[round];
                }
            }
        }
        return total;
    }

    // The largest value of the lines within the x axis, at least 1
    private static double max(List<Line> lines, XAxis axis) {
        double max = 1;
        for (Line line : lines) {
            for (int index = 0; index < line.x().length; index++) {
                if (line.x()[index] >= axis.min() && line.x()[index] <= axis.max() && !Double.isNaN(line.y()[index])) {
                    max = Math.max(max, line.y()[index]);
                }
            }
        }
        return max;
    }

    private static double last(double[] values) {
        return values.length == 0 ? 0 : values[values.length - 1];
    }
}
