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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;

/**
 * Writes the run report into a run directory: the scenario, correctness, throughput and latency, the sampled backlog
 * and per-second rates with their charts, and links to the profile reports. The report is the run directory's
 * {@code README.md}, and its HTML page the directory's {@code index.html}, so that a directory of runs served by an
 * HTTP server, or pushed to a GitHub repository, opens each run on its report.
 */
public final class RunReport {
    public static final String FILE_NAME = "README.md";
    static final String LATENCY_CHART = "latency";
    static final String THROUGHPUT_CHART = "throughput";
    static final String BACKLOG_CHART = "backlog";
    static final String TEMPERATURE_CHART = "host-temperature";
    static final String FREQUENCY_CHART = "host-frequency";
    public static final String RESOLVED_CONFIG = "resolved-config.yaml";
    /** The broker's topic stats sampled during a run, one row per topic, subscription and sample. */
    public static final String TOPIC_STATS_FILE = "topic-stats.csv";
    public static final String TOPIC_STATS_HEADER =
            "epochMillis,topic,subscription,msgBacklog,msgInCounter,msgOutCounter";
    /**
     * The host's thermal state sampled during a run, one row per second: temperatures in °C, frequencies in MHz, the
     * thermal throttle counters since boot (summed over the cores, and over the packages) and the fastest fan in rpm.
     * A value the host doesn't provide is empty.
     */
    public static final String HOST_STATS_FILE = "host-stats.csv";
    public static final String HOST_STATS_HEADER =
            "epochMillis,packageCelsius,coreCelsius,meanMHz,minMHz,coreThrottles,packageThrottles,fanRpm";
    // .txt, so that an HTTP server such as Python's shows the log as text instead of offering a download
    public static final String CONTAINER_LOG = "container.log.txt";
    private static final String MEASUREMENT_RECORDING_SUFFIX = ".measurement.jfr";
    private static final DateTimeFormatter FOOTER_START = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // The guide to the performance tests, which the report links to
    static final String README_URL = "https://github.com/apache/pulsar/tree/master/tests/performance";
    private static final DateTimeFormatter FOOTER_END = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * The run the report describes.
     *
     * @param info where, by whom and from which code the run was made; {@code null} leaves it out
     * @param finished when the workload finished, for the charts' footer; {@code null} leaves the end out
     * @param cooldowns how the launcher waited for the CPU to cool down, in order; empty if it didn't
     */
    public record Run(String scenario, String runId, String image, JsonNode cluster, JsonNode workload,
                      RunInfo info, ZonedDateTime finished, List<Cooldown> cooldowns) {
    }

    /**
     * A wait of the launcher for the CPU package to cool down to {@code targetCelsius}.
     *
     * @param phase when the launcher waited: {@link #BEFORE_RUN} or {@link #BEFORE_MEASUREMENT}
     * @param reached whether the package reached the target before the wait timed out
     * @param startEpochMillis when the wait started
     * @param endEpochMillis when the wait ended
     */
    public record Cooldown(String phase, double targetCelsius, double initialCelsius, double finalCelsius,
                           double waitedSeconds, boolean reached, long startEpochMillis, long endEpochMillis) {
        /** Before the cluster started. */
        public static final String BEFORE_RUN = "Before the run";
        /** After the warmup rounds had been received, before the first measured message. */
        public static final String BEFORE_MEASUREMENT = "After the warmup, before the measurement";
    }

    /** The sampled host stats of a run: one value per sample and column of {@link #HOST_STATS_HEADER}, or NaN. */
    record HostSamples(long[] epochMillis, double[] packageCelsius, double[] coreCelsius, double[] meanMegaHertz,
                       double[] minMegaHertz, double[] coreThrottles, double[] packageThrottles, double[] fanRpm) {
    }

    /**
     * The host's state during the measurement, summarized from its samples. The throttle counts are
     * {@link #UNKNOWN} when the host has no such counters or wasn't sampled during the measurement.
     */
    record HostSummary(double startCelsius, Stats packageCelsius, Stats coreCelsius, Stats meanMegaHertz,
                       Stats minMegaHertz, Stats fanRpm, long coreThrottles, long packageThrottles) {
        static final long UNKNOWN = -1;

        boolean throttled() {
            return coreThrottles > 0 || packageThrottles > 0;
        }

        /** Whether it is known if the CPU throttled: a counter was sampled during the measurement. */
        boolean throttlingKnown() {
            return coreThrottles != UNKNOWN || packageThrottles != UNKNOWN;
        }
    }

    /** The mean, minimum and maximum of a column during the measurement, NaN when it has no values. */
    record Stats(double mean, double min, double max) {
    }

    /**
     * The charts' footer, which says what a chart shows when it is looked at on its own: the branch, the short
     * commit (marked {@code -dirty} with uncommitted changes) and the run's start and end, such as
     * {@code lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04}. The date is not repeated for the end.
     */
    /**
     * The report's title: the run's start, the branch, the commit and the scenario, such as "Pulsar performance test
     * run 2026-09-26 20:35:06 lh-branch 0123456789ab iot-telemetry", so that the reports of different runs and
     * revisions can be told apart, for example in browser tabs. A commit that no branch contains has no branch, and
     * a commit with uncommitted changes ends in {@code -dirty}, as in the chart footer. The commit is abbreviated to
     * 12 characters, as in the {@code detached-<commit>} directory of a commit that no branch contains; 7 are
     * sometimes ambiguous in Pulsar's history.
     */
    static String title(String scenario, RunInfo info) {
        StringBuilder title = new StringBuilder("Pulsar performance test run");
        if (info != null) {
            title.append(' ').append(FOOTER_START.format(info.started()));
            if (!info.gitBranch().isEmpty() && !info.gitBranch().equals(RunInfo.DETACHED_HEAD)) {
                title.append(' ').append(info.gitBranch());
            }
            if (!info.gitCommit().isEmpty()) {
                title.append(' ').append(info.gitCommit(), 0, Math.min(12, info.gitCommit().length()))
                        .append(info.gitDirty() ? "-dirty" : "");
            }
        }
        return title.append(' ').append(scenario.replaceFirst("\\.ya?ml$", "")).toString();
    }

    static String chartFooter(RunInfo info, ZonedDateTime finished) {
        if (info == null) {
            return "";
        }
        StringBuilder footer = new StringBuilder();
        if (!info.gitBranch().isEmpty()) {
            footer.append(info.gitBranch());
        }
        if (!info.gitCommit().isEmpty()) {
            footer.append(footer.isEmpty() ? "" : "@")
                    .append(info.gitCommit(), 0, Math.min(8, info.gitCommit().length()))
                    .append(info.gitDirty() ? "-dirty" : "");
        }
        footer.append(footer.isEmpty() ? "" : " ").append(FOOTER_START.format(info.started()));
        if (finished != null) {
            footer.append('-').append(FOOTER_END.format(finished.withZoneSameInstant(info.started().getZone())));
        }
        return footer.toString();
    }

    // A cool-down before the measurement at least this long is cut out of the throughput and backlog charts
    private static final long CHART_CUT_MIN_MILLIS = 10_000;

    /**
     * The time axis of a chart: the sample rounds it shows, their seconds since the measurement start, and the cut
     * where a long cool-down before the measurement was left out. A round of -1 is a break in the lines at the cut.
     */
    record ChartTimeline(int[] rounds, double[] seconds, TimeSeriesRenderer.Cut cut) {
        double[] select(double[] values) {
            double[] selected = new double[rounds.length];
            for (int index = 0; index < rounds.length; index++) {
                selected[index] = rounds[index] < 0 ? Double.NaN : values[rounds[index]];
            }
            return selected;
        }
    }

    /**
     * The charts' time axis. The launcher's cool-down wait between the warmup and the measurement, when it took at
     * least 10 s, is left out: its samples are dropped and the samples before it move up by its length, so that the
     * measurement follows the warmup, and its fixed delay, instead of a long flat stretch. The axis breaks there and
     * shows the real time left of the break.
     */
    static ChartTimeline chartTimeline(long[] epochMillis, long measurementStart, List<Cooldown> cooldowns) {
        Cooldown gap = cooldowns == null ? null : cooldowns.stream()
                .filter(cooldown -> Cooldown.BEFORE_MEASUREMENT.equals(cooldown.phase())
                        && cooldown.endEpochMillis() - cooldown.startEpochMillis() >= CHART_CUT_MIN_MILLIS)
                .findFirst().orElse(null);
        List<Integer> rounds = new ArrayList<>();
        List<Double> seconds = new ArrayList<>();
        TimeSeriesRenderer.Cut cut = null;
        for (int round = 0; round < epochMillis.length; round++) {
            long epoch = epochMillis[round];
            if (gap != null && epoch >= gap.startEpochMillis() && epoch < gap.endEpochMillis()) {
                continue;
            }
            if (gap != null && cut == null && epoch >= gap.endEpochMillis()) {
                cut = new TimeSeriesRenderer.Cut((gap.endEpochMillis() - measurementStart) / 1000.0,
                        (gap.endEpochMillis() - gap.startEpochMillis()) / 1000.0);
                rounds.add(-1);
                seconds.add(cut.atSeconds());
            }
            long shift = gap != null && epoch < gap.startEpochMillis()
                    ? gap.endEpochMillis() - gap.startEpochMillis() : 0;
            rounds.add(round);
            seconds.add((epoch + shift - measurementStart) / 1000.0);
        }
        return new ChartTimeline(rounds.stream().mapToInt(Integer::intValue).toArray(),
                seconds.stream().mapToDouble(Double::doubleValue).toArray(), cut);
    }

    /** The sampled topic stats of a run: one row per sample round. */
    record Samples(long[] epochMillis, double[] published, Map<String, double[]> dispatched,
                   Map<String, double[]> backlog) {
    }

    private RunReport() {
    }

    public static Path write(Path runDirectory, Run run, ObjectMapper mapper) throws IOException {
        JsonNode producer = mapper.readTree(runDirectory.resolve("producer/producer-summary.json").toFile());
        List<JsonNode> consumers = new ArrayList<>();
        List<Path> consumerHistograms = new ArrayList<>();
        for (int application = 0; ; application++) {
            Path directory = applicationDirectory(runDirectory, run.workload(), application);
            if (!Files.isRegularFile(directory.resolve("consumer-summary.json"))) {
                break;
            }
            consumers.add(mapper.readTree(directory.resolve("consumer-summary.json").toFile()));
            if (Files.isRegularFile(directory.resolve("consume-latency.hdr"))) {
                consumerHistograms.add(directory.resolve("consume-latency.hdr"));
            }
        }
        long measurementStart = producer.path("measurementStartEpochMs").asLong();
        long measurementEnd = producer.path("measurementEndEpochMs").asLong();
        Path hostStats = runDirectory.resolve(HOST_STATS_FILE);
        HostSamples hostSamples = Files.isRegularFile(hostStats) ? readHostSamples(hostStats) : null;
        HostSummary host = hostSamples != null ? summarize(hostSamples, measurementStart, measurementEnd) : null;
        StringBuilder report = new StringBuilder();
        report.append("# ").append(title(run.scenario(), run.info())).append("\n\n");
        appendRun(report, runDirectory, run, producer, host);
        // The profiles come first so that a profiled run leads to its flame graphs
        appendProfiles(report, runDirectory, mapper);
        appendCorrectness(report, run.workload(), consumers);
        appendThroughput(report, producer, consumers);
        appendLatency(report, runDirectory, run, consumerHistograms, measurementStart);
        Path stats = runDirectory.resolve(TOPIC_STATS_FILE);
        if (Files.isRegularFile(stats)) {
            appendTopicStats(report, runDirectory, readSamples(stats), measurementStart, measurementEnd,
                    run.cooldowns(), chartFooter(run.info(), run.finished()));
        }
        if (hostSamples != null) {
            appendHost(report, runDirectory, hostSamples, host, run.cooldowns(), measurementStart, measurementEnd,
                    chartFooter(run.info(), run.finished()));
        }
        appendFiles(report, runDirectory, run.workload());
        appendFooter(report, run);
        Path file = runDirectory.resolve(FILE_NAME);
        Files.writeString(file, report);
        MarkdownPages.renderHtml(file, runDirectory, title(run.scenario(), run.info()));
        return file;
    }

    private static void appendRun(StringBuilder report, Path runDirectory, Run run, JsonNode producer,
                                  HostSummary host) {
        JsonNode workload = run.workload();
        JsonNode cluster = run.cluster();
        // The launcher copies the scenario file and writes its resolved form into the run directory
        String scenarioName = run.scenario().replaceFirst("\\.ya?ml$", "");
        boolean resolved = Files.isRegularFile(runDirectory.resolve(RESOLVED_CONFIG));
        report.append("Run `").append(run.runId()).append("`, image `").append(run.image()).append("`. The [Pulsar")
                .append(" performance testing README](").append(README_URL).append(") describes the tests and how")
                .append(" to read this report.\n\n")
                .append("| Setting | Value |\n|---|---|\n")
                .append(row("Scenario", Files.isRegularFile(runDirectory.resolve(run.scenario()))
                        ? "[" + scenarioName + "](" + run.scenario() + ")" : scenarioName))
                .append(row("Cluster", cluster.path("brokers").asInt() + " broker(s), "
                        + cluster.path("bookies").asInt() + " bookies"
                        + (resolved ? ", [configuration](" + RESOLVED_CONFIG + ")" : "")));
        JsonNode brokerEnvs = cluster.path("brokerEnvs");
        if (brokerEnvs.has("managedLedgerDefaultEnsembleSize")) {
            report.append(row("Ledger replication", "E=" + brokerEnvs.path("managedLedgerDefaultEnsembleSize")
                    .asText() + ", W=" + brokerEnvs.path("managedLedgerDefaultWriteQuorum").asText() + ", A="
                    + brokerEnvs.path("managedLedgerDefaultAckQuorum").asText()));
        }
        report.append(row("Producers", workload.path("gatewayCount").asInt() + " gateways × "
                        + workload.path("topicCount").asInt() + " topic(s)"))
                .append(row("Applications", workload.path("applicationCount").asInt() + " × "
                        + workload.path("clientsPerApplication").asInt() + " consumers, Key_Shared"))
                .append(row("Messages", String.format(Locale.ROOT, "%,d measured, %,d warmup",
                        messageCount(producer, "measurementMessages", workload.path("numberOfMessages").asLong()),
                        messageCount(producer, "warmupMessages", workload.path("warmupMessages").asLong()
                                * Math.max(1, workload.path("warmupRounds").asInt())))))
                .append(row("Payload", workload.path("payloadBytes").asInt() + " bytes, batching "
                        + (workload.path("batchingEnabled").asBoolean() ? "on" : "off")))
                .append(row("Rate limit", workload.path("rate").asLong() > 0
                        ? String.format(Locale.ROOT, "%,d msg/s", workload.path("rate").asLong()) : "none"));
        if (host != null) {
            report.append(row("Host CPU", hostSummaryLine(host)));
        }
        appendRunInfo(report, run.info());
    }

    /**
     * One line on the host's thermal state for the settings table, such as "68 °C at the start, at most 78 °C and
     * 3,100 MHz on average during the measurement, no thermal throttling".
     */
    static String hostSummaryLine(HostSummary host) {
        List<String> parts = new ArrayList<>();
        if (!Double.isNaN(host.startCelsius())) {
            parts.add(String.format(Locale.ROOT, "%.0f °C at the start", host.startCelsius()));
        }
        List<String> measurement = new ArrayList<>();
        if (!Double.isNaN(host.packageCelsius().max())) {
            measurement.add(String.format(Locale.ROOT, "at most %.0f °C", host.packageCelsius().max()));
        }
        if (!Double.isNaN(host.meanMegaHertz().mean())) {
            measurement.add(String.format(Locale.ROOT, "%,.0f MHz on average", host.meanMegaHertz().mean()));
        }
        if (!measurement.isEmpty()) {
            parts.add(String.join(" and ", measurement) + " during the measurement");
        }
        parts.add(host.throttled() ? "**thermal throttling**"
                : host.throttlingKnown() ? "no thermal throttling" : "thermal throttling unknown");
        return String.join(", ", parts);
    }

    // The run's own records, collapsed at the end, for a reader who browses the run directory, for example over HTTP
    private static void appendFiles(StringBuilder report, Path runDirectory, JsonNode workload) {
        List<String> links = new ArrayList<>();
        // The scenario and its resolved configuration are linked from the settings table
        for (String name : List.of(RunInfo.FILE_NAME, "producer/producer-summary.json",
                "producer/" + CONTAINER_LOG)) {
            Path file = runDirectory.resolve(name);
            if (Files.isRegularFile(file)) {
                links.add(link(runDirectory, file));
            }
        }
        for (int application = 0; Files.isDirectory(applicationDirectory(runDirectory, workload, application));
                application++) {
            for (String name : List.of("consumer-summary.json", CONTAINER_LOG)) {
                Path file = applicationDirectory(runDirectory, workload, application).resolve(name);
                if (Files.isRegularFile(file)) {
                    links.add(link(runDirectory, file));
                }
            }
        }
        if (!links.isEmpty()) {
            report.append("\n<details><summary>Files</summary>\n\n");
            for (String link : links) {
                report.append("- ").append(link).append('\n');
            }
            report.append("\n</details>\n");
        }
    }

    /**
     * Repeats the title, the run ID and the link to the guide below a horizontal line, so that they are at hand at
     * the end of a long report. The blank line before the dashes keeps them a line: directly below a paragraph they
     * would make it a heading.
     */
    static void appendFooter(StringBuilder report, Run run) {
        report.append("\n------------\n\n").append(title(run.scenario(), run.info())).append(" · run `")
                .append(run.runId()).append("` · [Pulsar performance testing README](").append(README_URL)
                .append(")\n");
    }

    /** A link to {@code file}, relative to the run directory, named by its path. */
    private static String link(Path runDirectory, Path file) {
        String relative = runDirectory.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize())
                .toString().replace('\\', '/');
        return "[" + relative + "](" + relative + ")";
    }

    // Where, by whom and from which code the run was made; the same values are in run-info.json
    static void appendRunInfo(StringBuilder report, RunInfo info) {
        if (info == null) {
            return;
        }
        String user = info.user();
        if (!info.gitUserName().isEmpty()) {
            user += " (git: " + info.gitUserName()
                    + (info.gitUserEmail().isEmpty() ? "" : " <" + info.gitUserEmail() + ">") + ")";
        }
        String hostDetails = info.hostDetails().summary();
        report.append(row("Started", info.started().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
                .append(row("Host", hostDetails.isEmpty() ? info.host() : info.host() + ": " + hostDetails));
        if (info.dockerEngine() != null) {
            report.append(row("Docker engine", info.dockerEngine().summary()));
        }
        report.append(row("User", user))
                .append(row("Project directory", code(info.projectDirectory().toString())))
                .append(row("Git branch", code(info.gitBranch())
                        + (info.gitDetached() && !info.gitBranch().equals(RunInfo.DETACHED_HEAD)
                        ? ", a detached HEAD at a commit of this branch" : "")))
                .append(row("Git commit", info.gitCommit().isEmpty() ? ""
                        : code(info.gitCommit()) + (info.gitDirty() ? ", with uncommitted changes" : "")))
                .append(row("Pulsar version", info.version()));
    }

    private static String code(String value) {
        return value.isEmpty() ? "" : "`" + value + "`";
    }

    private static void appendCorrectness(StringBuilder report, JsonNode workload, List<JsonNode> consumers) {
        report.append("\n## Correctness\n\n| Application | Unique messages | Duplicates | Ordering violations |"
                + " Invalid |\n|---|---:|---:|---:|---:|\n");
        boolean clean = true;
        for (JsonNode consumer : consumers) {
            long duplicates = consumer.path("duplicates").asLong();
            long violations = consumer.path("orderingViolations").asLong();
            long invalid = consumer.path("invalidMessages").asLong();
            clean &= duplicates == 0 && violations == 0 && invalid == 0;
            report.append(String.format(Locale.ROOT, "| %s | %,d | %,d | %,d | %,d |%n",
                    applicationName(workload, consumer.path("applicationIndex").asInt()),
                    consumer.path("uniqueMessages").asLong(),
                    duplicates, violations, invalid));
        }
        report.append(clean ? "\nNo duplicates, ordering violations or invalid messages.\n"
                : "\n**Duplicates, ordering violations or invalid messages were received.**\n");
    }

    private static void appendThroughput(StringBuilder report, JsonNode producer, List<JsonNode> consumers) {
        long measured = producer.path("measurementMessages").asLong();
        long start = producer.path("measurementStartEpochMs").asLong();
        long end = producer.path("measurementEndEpochMs").asLong();
        long lastReceived = consumers.stream().mapToLong(c -> c.path("lastMeasurementMessageReceivedEpochMs")
                .asLong()).max().orElse(end);
        report.append("\n## Throughput\n\n| Measure | Value |\n|---|---:|\n")
                .append(row("Producer throughput", String.format(Locale.ROOT, "%,.0f msg/s",
                        producer.path("messagesPerSecond").asDouble())))
                .append(row("Delivered throughput (until the slowest application received the last message)",
                        String.format(Locale.ROOT, "%,.0f msg/s", measured * 1000.0 / (lastReceived - start))))
                .append(row("Measurement", String.format(Locale.ROOT, "%,d messages in %.1f s", measured,
                        producer.path("measurementElapsedSeconds").asDouble())))
                .append(row("Consumers still draining after the producers finished",
                        String.format(Locale.ROOT, "%.1f s", Math.max(0, lastReceived - end) / 1000.0)));
    }

    /**
     * An application's name: its subscription, the workload's {@code subscriptionPrefix} and its index, such as
     * {@code iot-application-0}, as the throughput and backlog charts name it. Each application consumes through
     * {@code clientsPerApplication} consumers that record into one latency log.
     */
    public static String applicationName(JsonNode workload, int index) {
        String prefix = workload.path("subscriptionPrefix").asText("");
        return (prefix.isEmpty() ? "application-" : prefix) + index;
    }

    /** Where an application's outputs are: a directory named after the application, such as iot-application-0/. */
    public static Path applicationDirectory(Path runDirectory, JsonNode workload, int index) {
        return runDirectory.resolve(applicationName(workload, index));
    }

    // The application a log belongs to, from the directory it is in
    private static String applicationOf(Path log) {
        return log.toAbsolutePath().getParent().getFileName().toString();
    }

    private static void appendLatency(StringBuilder report, Path runDirectory, Run run, List<Path> consumers,
                                      long measurementStart) throws IOException {
        Path publish = runDirectory.resolve("producer/produce-latency.hdr");
        if (!Files.isRegularFile(publish) || consumers.isEmpty()) {
            return;
        }
        report.append("\n## Latency\n\n| Latency (ms) | Count | Min | p50 | p90 | p99 | p99.9 | Max |\n"
                + "|---|---:|---:|---:|---:|---:|---:|---:|\n");
        Histogram published = HdrHistogramRenderer.readMerged(List.of(publish));
        report.append(latencyRow("Publish (send to acknowledgment)", published));
        // Each application consumes on its own, so its end-to-end latency is reported on its own: a distribution
        // merged across applications would describe none of them
        List<String> applications = new ArrayList<>();
        for (Path consumer : consumers) {
            String application = applicationOf(consumer);
            applications.add(application);
            report.append(latencyRow(application + " (publish to consume)",
                    HdrHistogramRenderer.readMerged(List.of(consumer))));
        }
        List<Path> charts = HdrHistogramRenderer.render(publish, consumers, applications,
                runDirectory.resolve(LATENCY_CHART), measurementStart, chartFooter(run.info(), run.finished()));
        report.append("\n![Latency by percentile](").append(charts.get(0).getFileName())
                .append(")\n\n![Maximum latency per interval](").append(charts.get(1).getFileName()).append(")\n");
        List<Path> logs = new ArrayList<>();
        logs.add(publish);
        logs.addAll(consumers);
        // Collapsed, as the files are for tools rather than for reading: the interval logs for HistogramLogAnalyzer,
        // and the percentile distributions (.hgrm) for HdrHistogram's plotter, plotFiles.html
        report.append("\n<details><summary>HDR histogram logs and percentile distributions</summary>\n\n");
        for (Path log : logs) {
            report.append("- ").append(link(runDirectory, log)).append(" · ")
                    .append(link(runDirectory, HdrHistogramRenderer.writePercentileDistribution(log))).append('\n');
        }
        report.append("\n</details>\n");
    }

    private static void appendTopicStats(StringBuilder report, Path runDirectory, Samples samples,
                                         long measurementStart, long measurementEnd, List<Cooldown> cooldowns,
                                         String footer)
            throws IOException {
        int rounds = samples.epochMillis().length;
        if (rounds < 3) {
            return;
        }
        double[] seconds = new double[rounds];
        for (int round = 0; round < rounds; round++) {
            seconds[round] = (samples.epochMillis()[round] - measurementStart) / 1000.0;
        }
        double finished = (measurementEnd - measurementStart) / 1000.0;
        double[] totalDispatched = new double[rounds];
        for (double[] rates : samples.dispatched().values()) {
            for (int round = 0; round < rounds; round++) {
                totalDispatched[round] += rates[round];
            }
        }
        report.append("\n## Backlog and rates\n\nSampled once per second from the broker's topic stats; the rates are"
                + " per-second deltas of its message counters, and a sampled maximum is not the exact peak"
                + " between samples. The rates are those of the whole seconds within the measurement (0 to ")
                .append(String.format(Locale.ROOT, "%.1f", finished))
                .append(" s), leaving out its first and last second, where the producers start and finish. The")
                .append(" [sampled topic stats](").append(TOPIC_STATS_FILE).append(") are a CSV file.\n\n")
                .append("| Measure | Median | Minimum |\n|---|---:|---:|\n")
                .append(rateRow("Published msg/s", samples.published(), seconds, finished))
                .append(rateRow("Dispatched msg/s, all subscriptions", totalDispatched, seconds, finished))
                .append("\n| Subscription | Sampled maximum backlog during the measurement | At |"
                        + " Backlog when the producers finished |\n|---|---:|---:|---:|\n");
        for (Map.Entry<String, double[]> entry : samples.backlog().entrySet()) {
            double[] backlog = entry.getValue();
            int atFinish = rounds - 1;
            for (int round = 0; round < rounds; round++) {
                if (seconds[round] >= finished) {
                    atFinish = round;
                    break;
                }
            }
            // The measurement ends with the first sample at or after the producers finished, the one reported
            // as the backlog when they finished, so the maximum is never below it.
            int peak = -1;
            for (int round = 0; round <= atFinish; round++) {
                if (seconds[round] >= 0 && !Double.isNaN(backlog[round])
                        && (peak < 0 || backlog[round] > backlog[peak])) {
                    peak = round;
                }
            }
            if (peak < 0) {
                continue;
            }
            report.append(String.format(Locale.ROOT, "| `%s` | %,.0f | %.0f s | %,.0f |%n", entry.getKey(),
                    backlog[peak], seconds[peak], backlog[atFinish]));
        }
        ChartTimeline timeline = chartTimeline(samples.epochMillis(), measurementStart, cooldowns);
        List<TimeSeriesRenderer.Series> throughput = new ArrayList<>();
        // Dotted, so that the consumer lines it usually overlaps stay visible
        throughput.add(new TimeSeriesRenderer.Series("Producers (published)", timeline.select(samples.published()),
                true));
        for (Map.Entry<String, double[]> entry : samples.dispatched().entrySet()) {
            // One line per subscription, named by it alone so that the legend fits
            throughput.add(new TimeSeriesRenderer.Series(samples.dispatched().size() == 1
                    ? "Consumers (dispatched)" : entry.getKey(), timeline.select(entry.getValue())));
        }
        TimeSeriesRenderer.render(runDirectory.resolve(THROUGHPUT_CHART), "Throughput", "Messages per second",
                timeline.seconds(), throughput, finished, timeline.cut(), footer);
        List<TimeSeriesRenderer.Series> backlog = samples.backlog().entrySet().stream()
                .map(entry -> new TimeSeriesRenderer.Series(entry.getKey(), timeline.select(entry.getValue())))
                .toList();
        TimeSeriesRenderer.render(runDirectory.resolve(BACKLOG_CHART), "Backlog", "Messages in the backlog",
                timeline.seconds(), backlog, finished, timeline.cut(), footer);
        report.append("\n![Throughput over time](").append(THROUGHPUT_CHART).append(".svg)\n\n![Backlog over time](")
                .append(BACKLOG_CHART).append(".svg)\n");
    }

    private static void appendHost(StringBuilder report, Path runDirectory, HostSamples samples, HostSummary host,
                                   List<Cooldown> cooldowns, long measurementStart, long measurementEnd,
                                   String footer)
            throws IOException {
        report.append("\n## Host\n\nThe host's CPU, sampled once per second from Linux's sysfs files. The")
                .append(" [sampled host stats](").append(HOST_STATS_FILE).append(") are a CSV file.\n\n");
        for (Cooldown cooldown : cooldowns != null ? cooldowns : List.<Cooldown>of()) {
            report.append(cooldown.reached()
                    ? String.format(Locale.ROOT, "%s, the launcher waited %.0f s for the CPU package to cool down"
                            + " from %.0f °C to %.0f °C.%n%n", cooldown.phase(), cooldown.waitedSeconds(),
                            cooldown.initialCelsius(), cooldown.targetCelsius())
                    : String.format(Locale.ROOT, "%s, the launcher waited %.0f s for the CPU package to cool down"
                            + " to %.0f °C, and went on at %.0f °C when the wait timed out.%n%n", cooldown.phase(),
                            cooldown.waitedSeconds(), cooldown.targetCelsius(), cooldown.finalCelsius()));
        }
        if (host.throttled()) {
            List<String> events = new ArrayList<>();
            if (host.coreThrottles() != HostSummary.UNKNOWN) {
                events.add(String.format(Locale.ROOT, "%,d core", host.coreThrottles()));
            }
            if (host.packageThrottles() != HostSummary.UNKNOWN) {
                events.add(String.format(Locale.ROOT, "%,d package", host.packageThrottles()));
            }
            report.append("**The CPU throttled during the measurement:** ").append(String.join(" and ", events))
                    .append(" thermal throttle events, so the host ran below its capacity for part of the run.\n\n");
        } else if (host.throttlingKnown()) {
            report.append("No thermal throttling during the measurement.\n\n");
        } else {
            report.append("The host doesn't report thermal throttle counters, so whether the CPU throttled during the"
                    + " measurement is unknown.\n\n");
        }
        report.append("| Measure | At the start | Mean | Minimum | Maximum |\n|---|---:|---:|---:|---:|\n")
                .append(hostRow("CPU package temperature (°C)", samples.packageCelsius()[0], host.packageCelsius(),
                        "%.0f"))
                .append(hostRow("Hottest core temperature (°C)", samples.coreCelsius()[0], host.coreCelsius(),
                        "%.0f"))
                .append(hostRow("Core frequency, mean over the cores (MHz)", samples.meanMegaHertz()[0],
                        host.meanMegaHertz(), "%,.0f"))
                .append(hostRow("Lowest core frequency (MHz)", samples.minMegaHertz()[0], host.minMegaHertz(),
                        "%,.0f"))
                .append(hostRow("Fastest fan (rpm)", samples.fanRpm()[0], host.fanRpm(), "%,.0f"));
        report.append("\nThe mean, minimum and maximum are those of the measurement; the start is the first sample,"
                + " taken when the cluster started.\n");
        int rounds = samples.epochMillis().length;
        if (rounds < 2) {
            return;
        }
        double[] seconds = new double[rounds];
        for (int round = 0; round < rounds; round++) {
            seconds[round] = (samples.epochMillis()[round] - measurementStart) / 1000.0;
        }
        double finished = (measurementEnd - measurementStart) / 1000.0;
        List<String> charts = new ArrayList<>();
        List<String> shown = new ArrayList<>();
        if (hasValues(samples.packageCelsius()) || hasValues(samples.coreCelsius())) {
            List<TimeSeriesRenderer.Series> temperatures = new ArrayList<>();
            if (hasValues(samples.packageCelsius())) {
                temperatures.add(new TimeSeriesRenderer.Series("CPU package", samples.packageCelsius()));
            }
            if (hasValues(samples.coreCelsius())) {
                temperatures.add(new TimeSeriesRenderer.Series("Hottest core", samples.coreCelsius(), true));
            }
            // The host charts keep the whole time axis: the cooling down is what they show
            TimeSeriesRenderer.render(runDirectory.resolve(TEMPERATURE_CHART), "CPU temperature", "°C", seconds,
                    temperatures, finished, footer);
            charts.add("![CPU temperature over time](" + TEMPERATURE_CHART + ".svg)");
            shown.add("temperature");
        }
        if (hasValues(samples.meanMegaHertz())) {
            TimeSeriesRenderer.render(runDirectory.resolve(FREQUENCY_CHART), "CPU frequency", "MHz", seconds,
                    List.of(new TimeSeriesRenderer.Series("Mean over the cores", samples.meanMegaHertz()),
                            new TimeSeriesRenderer.Series("Lowest core", samples.minMegaHertz(), true)),
                    finished, footer);
            charts.add("![CPU frequency over time](" + FREQUENCY_CHART + ".svg)");
            shown.add("frequency");
        }
        // Collapsed, since the table above summarizes them; without data for any chart, there's no section
        if (!charts.isEmpty()) {
            report.append("\n<details><summary>CPU ").append(String.join(" and ", shown))
                    .append(" over time</summary>\n\n").append(String.join("\n\n", charts))
                    .append("\n\n</details>\n");
        }
    }

    private static String hostRow(String label, double start, Stats stats, String format) {
        if (Double.isNaN(start) && Double.isNaN(stats.mean())) {
            return "";
        }
        return "| " + label + " | " + formatValue(start, format) + " | " + formatValue(stats.mean(), format) + " | "
                + formatValue(stats.min(), format) + " | " + formatValue(stats.max(), format) + " |\n";
    }

    private static String formatValue(double value, String format) {
        return Double.isNaN(value) ? "" : String.format(Locale.ROOT, format, value);
    }

    private static boolean hasValues(double[] values) {
        return Arrays.stream(values).anyMatch(value -> !Double.isNaN(value));
    }

    /** Reads {@code host-stats.csv}; an empty value is NaN. */
    static HostSamples readHostSamples(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        int rounds = lines.size() - 1;
        long[] epochMillis = new long[rounds];
        double[][] columns = new double[7][rounds];
        for (int round = 0; round < rounds; round++) {
            String[] fields = lines.get(round + 1).split(",", -1);
            epochMillis[round] = Long.parseLong(fields[0]);
            for (int column = 0; column < columns.length; column++) {
                String field = column + 1 < fields.length ? fields[column + 1] : "";
                columns[column][round] = field.isEmpty() ? Double.NaN : Double.parseDouble(field);
            }
        }
        return new HostSamples(epochMillis, columns[0], columns[1], columns[2], columns[3], columns[4], columns[5],
                columns[6]);
    }

    /**
     * Summarizes the samples within the measurement. The throttle events are the counters' growth from the last
     * sample before the measurement, or the first sample, to the last sample within it.
     */
    static HostSummary summarize(HostSamples samples, long measurementStart, long measurementEnd) {
        long[] epochs = samples.epochMillis();
        int first = -1;
        int last = -1;
        int before = 0;
        for (int round = 0; round < epochs.length; round++) {
            if (epochs[round] < measurementStart) {
                before = round;
            } else if (epochs[round] <= measurementEnd) {
                if (first < 0) {
                    first = round;
                }
                last = round;
            }
        }
        double startCelsius = epochs.length > 0 ? samples.packageCelsius()[0] : Double.NaN;
        if (first < 0) {
            Stats none = new Stats(Double.NaN, Double.NaN, Double.NaN);
            return new HostSummary(startCelsius, none, none, none, none, none, HostSummary.UNKNOWN,
                    HostSummary.UNKNOWN);
        }
        return new HostSummary(startCelsius, stats(samples.packageCelsius(), first, last),
                stats(samples.coreCelsius(), first, last), stats(samples.meanMegaHertz(), first, last),
                stats(samples.minMegaHertz(), first, last), stats(samples.fanRpm(), first, last),
                growth(samples.coreThrottles(), before, last), growth(samples.packageThrottles(), before, last));
    }

    private static Stats stats(double[] values, int first, int last) {
        double sum = 0;
        double min = Double.NaN;
        double max = Double.NaN;
        int count = 0;
        for (int round = first; round <= last; round++) {
            double value = values[round];
            if (!Double.isNaN(value)) {
                sum += value;
                min = Double.isNaN(min) ? value : Math.min(min, value);
                max = Double.isNaN(max) ? value : Math.max(max, value);
                count++;
            }
        }
        return new Stats(count > 0 ? sum / count : Double.NaN, min, max);
    }

    private static long growth(double[] counters, int from, int to) {
        if (Double.isNaN(counters[from]) || Double.isNaN(counters[to])) {
            return HostSummary.UNKNOWN;
        }
        return Math.max(0, (long) (counters[to] - counters[from]));
    }

    private static void appendProfiles(StringBuilder report, Path runDirectory, ObjectMapper mapper)
            throws IOException {
        List<Path> profileReports;
        try (Stream<Path> directories = Files.list(runDirectory)) {
            profileReports = directories.map(directory -> directory.resolve(ProfileReport.FILE_NAME))
                    .filter(Files::isRegularFile).sorted().toList();
        }
        if (profileReports.isEmpty()) {
            return;
        }
        report.append("\n## Profiles\n\nEach profile report has the off-CPU digest, which ranks where threads were"
                + " blocked, the off-CPU flame graphs, and the CPU, allocation and other flame graphs, split by thread"
                + " and as heatmaps over time.\n\n| Profile report | Blocked off-CPU time (without idle waits)"
                + " | JFR recordings |\n|---|---:|---|\n");
        for (Path profileReport : profileReports) {
            Path directory = profileReport.getParent();
            double blockedSeconds = 0;
            boolean offCpuCaptured = false;
            try (Stream<Path> slices = Files.list(directory)) {
                for (Path offCpu : slices.filter(path -> path.getFileName().toString()
                        .endsWith(OffCpuFlamegraphs.OUTPUT_SUFFIX)).toList()) {
                    Path json = offCpu.resolve(OffCpuFlamegraphs.NO_IDLE_SLICE + ".json");
                    if (Files.isRegularFile(json)) {
                        offCpuCaptured = true;
                        blockedSeconds += Long.parseLong(mapper.readTree(json.toFile()).path("totalNanos")
                                .asText("0")) / 1e9;
                    }
                }
            }
            String name = directory.getFileName().toString();
            report.append("| [").append(componentName(name)).append("](").append(name).append("/")
                    .append(ProfileReport.FILE_NAME)
                    .append(") | ")
                    // Without off-CPU capture, the profile has only its JFR views.
                    .append(offCpuCaptured ? String.format(Locale.ROOT, "%.1f s", blockedSeconds) : "not captured")
                    .append(" | ").append(recordingLinks(name, directory)).append(" |\n");
        }
    }

    /** The profiled component a profile directory belongs to: {@code broker-profile} is "Broker". */
    static String componentName(String directoryName) {
        if (!directoryName.endsWith("-profile") && !directoryName.equals("producer")) {
            // A consumer application's directory is named after the application, as the rest of the report names it
            return directoryName;
        }
        String name = directoryName.replaceFirst("-profile$", "").replace('-', ' ');
        return name.isEmpty() ? directoryName : Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    // Direct downloads of a profile's recordings: each complete recording and its measurement cut, where kept
    private static String recordingLinks(String name, Path directory) throws IOException {
        List<Path> recordings;
        try (Stream<Path> files = Files.list(directory)) {
            recordings = files.filter(path -> path.getFileName().toString().endsWith(".jfr")).sorted().toList();
        }
        List<String> links = new ArrayList<>();
        for (Path recording : recordings) {
            String file = recording.getFileName().toString();
            String text = file.endsWith(MEASUREMENT_RECORDING_SUFFIX) ? "measurement period" : "complete";
            links.add("[" + text + "](" + name + "/" + file + ")");
        }
        return String.join(" · ", links);
    }

    /**
     * The count the producer reports, which a scenario limited by duration and rate has, rather than the configured
     * count, which such a scenario leaves at 0; the configured count for a producer summary without it.
     */
    private static long messageCount(JsonNode producer, String field, long configured) {
        return producer.has(field) ? producer.path(field).asLong() : configured;
    }

    /**
     * Reads {@code topic-stats.csv} into per-round published and dispatched rates and backlogs. The dispatched rates
     * and the backlogs are those of each subscription, summed over its topics: an application consumes all of its
     * topics through one subscription, and a line for each topic and subscription would make the charts and the
     * backlog table unreadable for many topics. {@code topic-stats.csv} keeps the values of each topic.
     */
    static Samples readSamples(Path csv) throws IOException {
        // epoch -> topic -> in counter; epoch -> [topic, subscription] -> [backlog, out counter]
        TreeMap<Long, Map<String, Long>> inCounters = new TreeMap<>();
        TreeMap<Long, Map<List<String>, long[]>> subscriptions = new TreeMap<>();
        List<String> lines = Files.readAllLines(csv);
        for (String line : lines.subList(1, lines.size())) {
            String[] fields = line.split(",");
            long epoch = Long.parseLong(fields[0]);
            inCounters.computeIfAbsent(epoch, e -> new LinkedHashMap<>()).put(fields[1], Long.parseLong(fields[4]));
            subscriptions.computeIfAbsent(epoch, e -> new HashMap<>())
                    .put(List.of(fields[1], fields[2]), new long[] {Long.parseLong(fields[3]),
                            Long.parseLong(fields[5])});
        }
        long[] epochs = inCounters.keySet().stream().mapToLong(Long::longValue).toArray();
        int rounds = epochs.length;
        double[] published = new double[rounds];
        Arrays.fill(published, Double.NaN);
        Map<String, double[]> dispatched = new TreeMap<>();
        Map<String, double[]> backlog = new TreeMap<>();
        for (int round = 0; round < rounds; round++) {
            Map<List<String>, long[]> current = subscriptions.get(epochs[round]);
            for (Map.Entry<List<String>, long[]> entry : current.entrySet()) {
                double[] values = backlog.computeIfAbsent(entry.getKey().get(1), k -> nanArray(rounds));
                values[round] = (Double.isNaN(values[round]) ? 0 : values[round]) + entry.getValue()[0];
                dispatched.computeIfAbsent(entry.getKey().get(1), k -> nanArray(rounds));
            }
            if (round == 0) {
                continue;
            }
            double elapsed = (epochs[round] - epochs[round - 1]) / 1000.0;
            Map<String, Long> previousIn = inCounters.get(epochs[round - 1]);
            Map<String, Long> currentIn = inCounters.get(epochs[round]);
            if (previousIn.keySet().equals(currentIn.keySet())) {
                long delta = 0;
                for (Map.Entry<String, Long> entry : currentIn.entrySet()) {
                    delta += entry.getValue() - previousIn.get(entry.getKey());
                }
                // A negative delta means a counter restarted, for example after a topic was reloaded.
                published[round] = delta >= 0 ? delta / elapsed : Double.NaN;
            }
            // A subscription's rate is known when every one of its topics has a rate, so that a partial sum isn't
            // mistaken for a drop in the rate
            Map<List<String>, long[]> previous = subscriptions.get(epochs[round - 1]);
            Map<String, Double> rates = new HashMap<>();
            Set<String> incomplete = new HashSet<>();
            for (Map.Entry<List<String>, long[]> entry : current.entrySet()) {
                String subscription = entry.getKey().get(1);
                long[] before = previous.get(entry.getKey());
                if (before != null && entry.getValue()[1] >= before[1]) {
                    rates.merge(subscription, (entry.getValue()[1] - before[1]) / elapsed, Double::sum);
                } else {
                    incomplete.add(subscription);
                }
            }
            for (Map.Entry<String, Double> rate : rates.entrySet()) {
                if (!incomplete.contains(rate.getKey())) {
                    dispatched.get(rate.getKey())[round] = rate.getValue();
                }
            }
        }
        return new Samples(epochs, published, dispatched, backlog);
    }

    private static double[] nanArray(int length) {
        double[] values = new double[length];
        Arrays.fill(values, Double.NaN);
        return values;
    }

    private static String rateRow(String label, double[] rates, double[] seconds, double finished) {
        double[] window = new double[rates.length];
        int count = 0;
        for (int round = 1; round < rates.length; round++) {
            // The interval since the previous sample lies within the measurement without its first and last
            // second, where the producers ramp up and finish their last messages.
            if (seconds[round - 1] >= 1 && seconds[round] <= finished - 1 && !Double.isNaN(rates[round])) {
                window[count++] = rates[round];
            }
        }
        if (count == 0) {
            return "| " + label + " | | |\n";
        }
        double[] sorted = Arrays.copyOf(window, count);
        Arrays.sort(sorted);
        return String.format(Locale.ROOT, "| %s | %,.0f | %,.0f |%n", label, sorted[count / 2], sorted[0]);
    }

    private static String latencyRow(String label, Histogram histogram) {
        return String.format(Locale.ROOT, "| %s | %,d | %s | %s | %s | %s | %s | %s |%n", label,
                histogram.getTotalCount(), millis(histogram.getMinValue()), millis(histogram.getValueAtPercentile(50)),
                millis(histogram.getValueAtPercentile(90)), millis(histogram.getValueAtPercentile(99)),
                millis(histogram.getValueAtPercentile(99.9)), millis(histogram.getMaxValue()));
    }

    // The workload records latencies in microseconds.
    private static String millis(long micros) {
        return String.format(Locale.ROOT, "%,.1f", micros / 1000.0);
    }

    private static String row(String name, String value) {
        return "| " + name + " | " + value + " |\n";
    }
}
