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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.ComparisonCharts;
import org.apache.pulsar.tests.performance.report.HdrHistogramRenderer;
import org.apache.pulsar.tests.performance.report.MarkdownPages;

/**
 * Compares two Pulsar clusters on the same scenario: two released Pulsar images, or a released Pulsar image and this
 * checkout. It runs the scenario on each side in turn, alternating the order (A B, B A, A B, ...) so that a drift of
 * the host affects both sides alike, picks each side's median run, charts the two median runs with
 * {@link ComparisonCharts}, and writes a summary of every run's main measures.
 *
 * <p>Each run is a {@link PerformanceLauncher} in a JVM of its own, with this JVM's classpath and its
 * {@code performance.*} system properties. A side with a released Pulsar image runs ZooKeeper, the bookies and the
 * brokers in the test image built on it, as {@code -Pperformance.clusterPulsarImage} does; the gateways and the
 * applications always run this checkout's test image, and with it its Pulsar client. The arguments are the
 * launcher's, such as {@code --scenario} and {@code --name}.
 *
 * <p>System properties, which {@code :tests:performance:launcher:compareImages} and {@code compareImageWithCheckout}
 * set from Gradle properties:
 * <ul>
 *   <li>{@code performance.compare.baseline.pulsarImage} and {@code .image}: the baseline's (A's) released Pulsar
 *   image and the test image built on it</li>
 *   <li>{@code performance.compare.candidate.pulsarImage} and {@code .image}: the same for the candidate (B); without
 *   them, B is this checkout</li>
 *   <li>{@code performance.compare.repetitions}: the runs per side, 3 by default</li>
 *   <li>{@code performance.compare.medianBy}: the measure that picks each side's median run, one of
 *   {@link Measure}'s names, {@code throughput} by default</li>
 * </ul>
 */
public final class ImageComparison {
    static final String RUN_DIRECTORY_PREFIX = "Run directory: ";
    static final String SUMMARY_FILE = "README.md";
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("MM-dd-HH-mm-ss");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** The launcher that runs now, which a shutdown hook stops when the comparison is interrupted. */
    private static volatile Process current;

    /** A side of the comparison: a released Pulsar image, or this checkout when {@code pulsarImage} is null. */
    record Side(String name, String pulsarImage, String clusterImage) {
        boolean checkout() {
            return pulsarImage == null;
        }

        /** The side's name in the charts and the summary: the image's tag, or {@code checkout}. */
        String label() {
            if (checkout()) {
                return "checkout";
            }
            int slash = pulsarImage.lastIndexOf('/');
            int colon = pulsarImage.lastIndexOf(':');
            return colon > slash ? pulsarImage.substring(colon + 1) : pulsarImage.substring(slash + 1);
        }

        String description() {
            return checkout() ? "this checkout" : "`" + pulsarImage + "`";
        }
    }

    /** A run's main measures; NaN when its outputs don't have one. */
    record Measures(double throughput, double publishP99Millis, double endToEndP99Millis,
                    double brokerCpuSecondsPerMillion) {
    }

    /** The measures that the summary compares, and that can pick each side's median run. */
    enum Measure {
        THROUGHPUT("throughput", "throughput", "Throughput, msg/s", Measures::throughput, "%,.0f"),
        PUBLISH_P99("publish-p99", "publish p99", "Publish p99, ms", Measures::publishP99Millis, "%,.1f"),
        END_TO_END_P99("e2e-p99", "end-to-end p99", "End-to-end p99 (slowest application), ms",
                Measures::endToEndP99Millis, "%,.1f"),
        BROKER_CPU("broker-cpu", "broker CPU per message", "Broker CPU per million messages, s",
                Measures::brokerCpuSecondsPerMillion, "%,.1f");

        final String option;
        final String name;
        final String title;
        final ToDoubleFunction<Measures> value;
        final String format;

        Measure(String option, String name, String title, ToDoubleFunction<Measures> value, String format) {
            this.option = option;
            this.name = name;
            this.title = title;
            this.value = value;
            this.format = format;
        }

        static Measure of(String option) {
            for (Measure measure : values()) {
                if (measure.option.equals(option)) {
                    return measure;
                }
            }
            throw new IllegalArgumentException("performance.compare.medianBy must be one of throughput, publish-p99,"
                    + " e2e-p99 or broker-cpu, not '" + option + "'");
        }

        String format(double v) {
            return Double.isNaN(v) ? "–" : String.format(Locale.ROOT, format, v);
        }
    }

    /** How a run ended. Only valid runs count in the comparison. */
    enum Result {
        VALID("ok"),
        FAILED("failed"),
        /** The applications received messages out of order or invalid messages, so the run measured a bug. */
        INVALID("invalid: ordering violations or invalid messages");

        final String text;

        Result(String text) {
            this.text = text;
        }
    }

    /** A run of the comparison: its side, its run directory, how it ended, and its measures. */
    record Run(Side side, int order, Path directory, Result result, Measures measures) {
        boolean valid() {
            return result == Result.VALID;
        }
    }

    private static final Measures NO_MEASURES = new Measures(Double.NaN, Double.NaN, Double.NaN, Double.NaN);

    private ImageComparison() {
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    static int run(String[] launcherArgs) throws Exception {
        for (String arg : launcherArgs) {
            if (arg.equals("--output") || arg.startsWith("--output=")) {
                throw new IllegalArgumentException("A comparison writes each run to its own directory; don't pass"
                        + " --output");
            }
        }
        Side baseline = side("A", "baseline");
        if (baseline == null) {
            throw new IllegalArgumentException("Set performance.compare.baseline.pulsarImage, such as with"
                    + " -Pperformance.compare.baselineImage=apachepulsar/pulsar:4.0.14");
        }
        Side candidate = side("B", "candidate");
        if (candidate == null) {
            candidate = new Side("B", null, null);
        }
        int repetitions = Integer.parseInt(System.getProperty("performance.compare.repetitions", "3"));
        if (repetitions < 1) {
            throw new IllegalArgumentException("performance.compare.repetitions must be at least 1");
        }
        Measure medianBy = Measure.of(System.getProperty("performance.compare.medianBy", "throughput"));
        ZonedDateTime started = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        // Stop the launcher that runs when the comparison is interrupted, such as by Ctrl-C or a cancelled Gradle
        // build, so that it stops its containers instead of running on without anyone waiting for it
        Runtime.getRuntime().addShutdownHook(new Thread(ImageComparison::stopCurrent, "stop-comparison-run"));

        List<Run> runs = new ArrayList<>();
        List<Side> order = runOrder(baseline, candidate, repetitions);
        for (int i = 0; i < order.size(); i++) {
            Side side = order.get(i);
            System.out.printf("=== Comparison run %d of %d: %s (%s)%n", i + 1, order.size(), side.name(),
                    side.checkout() ? "this checkout" : side.pulsarImage());
            Run run = launch(side, i + 1, launcherArgs);
            runs.add(run);
            if (run.directory() == null) {
                // The launcher failed before it started the run, such as on an invalid argument, which fails the other
                // runs alike
                System.err.println("The launcher failed before it started run " + run.order()
                        + "; the comparison stops");
                return 1;
            }
        }

        Path firstRun = runs.get(0).directory();
        Path output = outputDirectory(firstRun, started);
        Files.createDirectories(output);
        Run medianA = median(runs, baseline, medianBy);
        Run medianB = median(runs, candidate, medianBy);
        List<String> charts = List.of();
        boolean charted = false;
        if (medianA != null && medianB != null) {
            String labelA = baseline.label();
            String labelB = comparisonLabel(baseline, candidate);
            int chartsExit = ComparisonCharts.execute("--baseline",
                    medianA.directory().toString(), "--comparison", medianB.directory().toString(),
                    "--baseline-label", labelA, "--comparison-label", labelB, "--output", output.toString());
            charted = chartsExit == 0;
            if (!charted) {
                System.err.println("Charting the median runs failed");
            }
            try (Stream<Path> files = Files.list(output)) {
                charts = files.map(p -> p.getFileName().toString())
                        .filter(f -> f.endsWith(".svg") && !f.endsWith("-separate.svg"))
                        .sorted(Comparator.comparingInt(ImageComparison::chartRank)
                                .thenComparing(Comparator.naturalOrder()))
                        .toList();
            }
        }
        Path summary = output.resolve(SUMMARY_FILE);
        Files.writeString(summary, summary(baseline, candidate, launcherArgs, repetitions, medianBy, runs, medianA,
                medianB, charts, output));
        // index.html beside it, with the links to the runs' reports pointing to their pages, as the runs have
        MarkdownPages.renderHtml(summary, output, "Comparison of " + baseline.label() + " and " + candidate.label());
        System.out.println("Comparison: " + output.resolve(SUMMARY_FILE));
        if (medianA == null || medianB == null) {
            System.err.println("A side has no valid run with the measure " + medianBy.name + " to compare");
        }
        return charted ? 0 : 1;
    }

    private static void stopCurrent() {
        Process process = current;
        if (process == null || !process.isAlive()) {
            return;
        }
        // The launcher's own shutdown hook stops the run's containers, which takes a while
        process.destroy();
        try {
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
    }

    private static Side side(String name, String key) {
        String pulsarImage = System.getProperty("performance.compare." + key + ".pulsarImage");
        String clusterImage = System.getProperty("performance.compare." + key + ".image");
        if ((pulsarImage == null) != (clusterImage == null)) {
            throw new IllegalArgumentException("Set both performance.compare." + key + ".pulsarImage and .image, or"
                    + " neither; the compareImages and compareImageWithCheckout tasks set both");
        }
        return pulsarImage == null ? null : new Side(name, pulsarImage, clusterImage);
    }

    /** The sides in the order to run them: A B, then B A, and so on, so that each side runs first as often. */
    static List<Side> runOrder(Side a, Side b, int repetitions) {
        List<Side> order = new ArrayList<>();
        for (int i = 0; i < repetitions; i++) {
            order.add(i % 2 == 0 ? a : b);
            order.add(i % 2 == 0 ? b : a);
        }
        return order;
    }

    /** B's label in the charts, which tells it apart from A's when the images have the same tag. */
    static String comparisonLabel(Side a, Side b) {
        return b.label().equals(a.label()) ? b.label() + "-B" : b.label();
    }

    /** The charts in the order of a run's report: throughput, latency, then backlog. */
    static int chartRank(String chart) {
        if (chart.startsWith("throughput")) {
            return 0;
        } else if (chart.startsWith("latency-percentiles-log")) {
            return 2;
        } else if (chart.startsWith("latency-percentiles")) {
            return 1;
        } else if (chart.startsWith("backlog")) {
            return 3;
        }
        return 4;
    }

    /** The JVM command line that runs the launcher for a side, with this JVM's performance.* properties. */
    static List<String> command(Side side, String javaHome, String classpath, Map<Object, Object> properties,
                                String[] launcherArgs) {
        List<String> command = new ArrayList<>();
        command.add(Path.of(javaHome, "bin", "java").toString());
        properties.entrySet().stream()
                .filter(e -> e.getKey() instanceof String key && key.startsWith("performance.")
                        && !key.startsWith("performance.compare.") && !key.startsWith("performance.cluster."))
                .sorted(Comparator.comparing(e -> (String) e.getKey()))
                .forEach(e -> command.add("-D" + e.getKey() + "=" + e.getValue()));
        if (!side.checkout()) {
            command.add("-Dperformance.cluster.pulsarImage=" + side.pulsarImage());
            command.add("-Dperformance.cluster.image=" + side.clusterImage());
        }
        command.add("-cp");
        command.add(classpath);
        command.add(PerformanceLauncher.class.getName());
        command.addAll(List.of(launcherArgs));
        return command;
    }

    private static Run launch(Side side, int order, String[] launcherArgs) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder(command(side, System.getProperty("java.home"),
                System.getProperty("java.class.path"), System.getProperties(), launcherArgs))
                .redirectErrorStream(true);
        Process process = builder.start();
        current = process;
        Path directory = null;
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = output.readLine()) != null) {
                System.out.println(line);
                if (directory == null && line.startsWith(RUN_DIRECTORY_PREFIX)) {
                    directory = Path.of(line.substring(RUN_DIRECTORY_PREFIX.length()).trim());
                }
            }
        } catch (IOException e) {
            process.destroy();
            throw e;
        }
        int exit;
        try {
            exit = process.waitFor();
        } catch (InterruptedException e) {
            process.destroy();
            throw e;
        } finally {
            current = null;
        }
        if (exit != 0 || directory == null) {
            System.out.printf("=== Run %d of %s failed with exit code %d; the comparison leaves it out%n", order,
                    side.name(), exit);
            return new Run(side, order, directory, Result.FAILED, NO_MEASURES);
        }
        Result result = Result.VALID;
        if (deliveredInvalidly(directory)) {
            System.out.printf("=== Run %d of %s received messages out of order or invalid messages; the comparison"
                    + " leaves it out%n", order, side.name());
            result = Result.INVALID;
        }
        return new Run(side, order, directory, result, measures(directory));
    }

    /** Whether an application of the run received messages out of order or invalid messages. */
    static boolean deliveredInvalidly(Path run) {
        try (Stream<Path> summaries = applicationFiles(run, "application-summary.json")) {
            for (Iterator<Path> it = summaries.iterator(); it.hasNext(); ) {
                JsonNode summary = MAPPER.readTree(it.next().toFile());
                if (summary.path("orderingViolations").asLong() > 0 || summary.path("invalidMessages").asLong() > 0) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            System.err.println("Reading the applications' summaries of " + run + " failed: " + e);
            return true;
        }
    }

    /** A run's measures from its outputs; a measure is NaN when its outputs are missing or can't be read. */
    static Measures measures(Path run) {
        return new Measures(measure(run, "throughput", () -> {
            JsonNode summary = MAPPER.readTree(run.resolve("gateways").resolve("gateways-summary.json").toFile());
            return summary.path("messagesPerSecond").asDouble(Double.NaN);
        }), measure(run, "publish p99", () -> p99Millis(List.of(run.resolve("gateways")
                .resolve("gateways-latency.hdr")))), measure(run, "end-to-end p99", () -> {
            double slowest = Double.NaN;
            try (Stream<Path> logs = applicationFiles(run, "application-latency.hdr")) {
                for (Iterator<Path> it = logs.iterator(); it.hasNext(); ) {
                    double p99 = p99Millis(List.of(it.next()));
                    slowest = Double.isNaN(slowest) ? p99 : Math.max(slowest, p99);
                }
            }
            return slowest;
        }), measure(run, "broker CPU", () -> {
            double brokerCpu = Double.NaN;
            for (Map.Entry<String, JsonNode> e : MAPPER.readTree(run.resolve("container-summary.json").toFile())
                    .path("containers").properties()) {
                double v = e.getValue().path("cpuSecondsPerMillionMessages").asDouble(Double.NaN);
                if (e.getKey().startsWith("broker") && !Double.isNaN(v)) {
                    brokerCpu = Double.isNaN(brokerCpu) ? v : brokerCpu + v;
                }
            }
            return brokerCpu;
        }));
    }

    private interface MeasureReader {
        double read() throws IOException;
    }

    private static double measure(Path run, String name, MeasureReader reader) {
        try {
            return reader.read();
        } catch (IOException | RuntimeException e) {
            System.err.println("Reading the " + name + " of " + run + " failed: " + e);
            return Double.NaN;
        }
    }

    /** The files of that name in the run's applications' directories. */
    private static Stream<Path> applicationFiles(Path run, String name) throws IOException {
        Path applications = run.resolve("applications");
        if (!Files.isDirectory(applications)) {
            return Stream.empty();
        }
        return Files.list(applications).map(dir -> dir.resolve(name)).filter(Files::isRegularFile).sorted();
    }

    /** The 99th percentile of HdrHistogram latency logs, which record microseconds, in milliseconds. */
    static double p99Millis(List<Path> logs) throws IOException {
        return HdrHistogramRenderer.valueAtPercentileMillis(logs, 99.0);
    }

    /**
     * The side's median valid run by the measure, the lower one of the middle two for an even count, leaving out the
     * runs without the measure; null when no run has it.
     */
    static Run median(List<Run> runs, Side side, Measure by) {
        List<Run> valid = runs.stream()
                .filter(r -> r.side() == side && r.valid() && !Double.isNaN(by.value.applyAsDouble(r.measures())))
                .sorted(Comparator.comparingDouble(r -> by.value.applyAsDouble(r.measures()))).toList();
        return valid.isEmpty() ? null : valid.get((valid.size() - 1) / 2);
    }

    /** {@code <reports root>/<day>/comparisons/<run name>/<time>}, beside the runs' days in the reports root. */
    static Path outputDirectory(Path firstRun, ZonedDateTime started) {
        // <reports root>/<day>/<branch or image>/<name>/<time>
        Path name = firstRun.getParent();
        Path reportsRoot = name.getParent().getParent().getParent();
        return reportsRoot.resolve(DAY.format(started)).resolve("comparisons").resolve(name.getFileName())
                .resolve(TIME.format(started));
    }

    static String summary(Side a, Side b, String[] launcherArgs, int repetitions, Measure medianBy, List<Run> runs,
                          Run medianA, Run medianB, List<String> charts, Path output) {
        StringBuilder s = new StringBuilder();
        s.append("# Comparison: ").append(a.label()).append(" (A) and ").append(b.label()).append(" (B)\n\n");
        s.append("- **A:** ").append(a.description()).append("\n");
        s.append("- **B:** ").append(b.description()).append("\n");
        s.append("- **Clusters:** ZooKeeper, the bookies and the brokers run each side's Pulsar. The gateways and the"
                + " applications run this checkout's test image, and with it its Pulsar client.\n");
        s.append("- **Runs:** ").append(repetitions).append(" per side, alternating the order (A B, B A, ...): `")
                .append(String.join(" ", launcherArgs)).append("`\n");
        s.append("- **Median run:** each side's median valid run by ").append(medianBy.name)
                .append(", the lower of the middle two for an even count, which the charts compare. A run that failed,"
                        + " or whose applications received messages out of order or invalid messages, doesn't"
                        + " count.\n\n");

        s.append("| Median run | A | B | Change |\n|---|---:|---:|---:|\n");
        for (Measure measure : Measure.values()) {
            double va = medianA != null ? measure.value.applyAsDouble(medianA.measures()) : Double.NaN;
            double vb = medianB != null ? measure.value.applyAsDouble(medianB.measures()) : Double.NaN;
            String change = Double.isNaN(va) || Double.isNaN(vb) || va == 0 ? "–"
                    : String.format(Locale.ROOT, "%+.1f %%", (vb - va) * 100 / va);
            s.append("| ").append(measure.title).append(" | ").append(measure.format(va)).append(" | ")
                    .append(measure.format(vb)).append(" | ").append(change).append(" |\n");
        }

        s.append("\n## Runs\n\n| # | Side | Run | Result |");
        for (Measure measure : Measure.values()) {
            s.append(' ').append(measure.title).append(" |");
        }
        s.append("\n|---:|---|---|---|");
        s.append("---:|".repeat(Measure.values().length)).append('\n');
        for (Run run : runs) {
            String result = run == medianA || run == medianB ? "median" : run.result().text;
            s.append("| ").append(run.order()).append(" | ").append(run.side().name()).append(" | ")
                    .append(runLink(run, output))
                    .append(" | ").append(result).append(" |");
            for (Measure measure : Measure.values()) {
                s.append(' ').append(measure.format(measure.value.applyAsDouble(run.measures()))).append(" |");
            }
            s.append('\n');
        }
        if (!charts.isEmpty()) {
            s.append("\n## The median runs\n\n");
            String labels = a.label() + " and " + comparisonLabel(a, b);
            for (String chart : charts) {
                s.append("![").append(chartTitle(chart)).append(" of ").append(labels).append("](").append(chart)
                        .append(")\n\n");
            }
        }
        return s.toString();
    }

    /**
     * A link to the run's report, or to its console log when it failed, since a failed run may have no report; a dash
     * when the launcher didn't start the run.
     */
    private static String runLink(Run run, Path output) {
        if (run.directory() == null) {
            return "–";
        }
        boolean failed = run.result() == Result.FAILED;
        Path target = run.directory().resolve(failed ? "console.log.txt" : SUMMARY_FILE);
        return "[" + (failed ? "Console log" : "Report") + " of run " + run.order() + " (" + run.side().name()
                + ")](" + output.relativize(target).toString().replace('\\', '/') + ")";
    }

    /** What a chart of {@link ComparisonCharts} shows, from its file name. */
    static String chartTitle(String chart) {
        if (chart.startsWith("throughput")) {
            return "Throughput";
        } else if (chart.startsWith("latency-percentiles-log")) {
            return "Latency percentiles on a logarithmic scale";
        } else if (chart.startsWith("latency-percentiles")) {
            return "Latency percentiles";
        } else if (chart.startsWith("backlog")) {
            return "Backlog";
        }
        return chart.substring(0, chart.length() - ".svg".length());
    }
}
