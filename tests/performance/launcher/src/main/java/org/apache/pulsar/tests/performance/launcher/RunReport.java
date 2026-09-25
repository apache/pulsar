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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;

/**
 * Writes {@code run-report.md} and its HTML page into a run directory: the scenario, correctness, throughput and
 * latency, the sampled backlog and per-second rates with their charts, and links to the profile reports.
 */
final class RunReport {
    static final String FILE_NAME = "run-report.md";
    static final String LATENCY_CHART = "latency-histograms";
    static final String THROUGHPUT_CHART = "throughput";
    static final String BACKLOG_CHART = "backlog";
    static final String RESOLVED_CONFIG = "resolved-config.yaml";
    private static final String MEASUREMENT_RECORDING_SUFFIX = ".measurement.jfr";
    private static final DateTimeFormatter FOOTER_START = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FOOTER_END = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * The run the report describes.
     *
     * @param info where, by whom and from which code the run was made; {@code null} leaves it out
     * @param finished when the workload finished, for the charts' footer; {@code null} leaves the end out
     */
    record Run(String scenario, String runId, String image, JsonNode cluster, JsonNode workload, RunInfo info,
               ZonedDateTime finished) {
    }

    /**
     * The charts' footer, which says what a chart shows when it is looked at on its own: the branch, the short
     * commit (marked {@code -dirty} with uncommitted changes) and the run's start and end, such as
     * {@code lh-branch@1ebd73f2 2026-09-25 13:35:22-13:39:04}. The date is not repeated for the end.
     */
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

    /** The sampled topic stats of a run: one row per sample round. */
    record Samples(long[] epochMillis, double[] published, Map<String, double[]> dispatched,
                   Map<String, double[]> backlog) {
    }

    private RunReport() {
    }

    static Path write(Path runDirectory, Run run, ObjectMapper mapper) throws IOException {
        JsonNode producer = mapper.readTree(runDirectory.resolve("producer/producer-summary.json").toFile());
        List<JsonNode> consumers = new ArrayList<>();
        List<Path> consumerHistograms = new ArrayList<>();
        for (int application = 0; ; application++) {
            Path directory = runDirectory.resolve("consumer-" + application);
            if (!Files.isRegularFile(directory.resolve("consumer-summary.json"))) {
                break;
            }
            consumers.add(mapper.readTree(directory.resolve("consumer-summary.json").toFile()));
            if (Files.isRegularFile(directory.resolve("consume-latency.hdr"))) {
                consumerHistograms.add(directory.resolve("consume-latency.hdr"));
            }
        }
        StringBuilder report = new StringBuilder();
        report.append("# Run report: ").append(run.scenario()).append("\n\n");
        appendRun(report, runDirectory, run);
        // The profiles come first so that a profiled run leads to its flame graphs
        appendProfiles(report, runDirectory, mapper);
        appendCorrectness(report, consumers);
        long measurementStart = producer.path("measurementStartEpochMs").asLong();
        long measurementEnd = producer.path("measurementEndEpochMs").asLong();
        appendThroughput(report, producer, consumers);
        appendLatency(report, runDirectory, run, consumerHistograms);
        Path stats = runDirectory.resolve(TopicStatsSampler.FILE_NAME);
        if (Files.isRegularFile(stats)) {
            appendTopicStats(report, runDirectory, readSamples(stats), measurementStart, measurementEnd,
                    chartFooter(run.info(), run.finished()));
        }
        appendFiles(report, runDirectory);
        Path file = runDirectory.resolve(FILE_NAME);
        Files.writeString(file, report);
        MarkdownPages.renderHtml(file, runDirectory, "Run report: " + run.scenario());
        return file;
    }

    private static void appendRun(StringBuilder report, Path runDirectory, Run run) {
        JsonNode workload = run.workload();
        JsonNode cluster = run.cluster();
        // The launcher copies the scenario file and writes its resolved form into the run directory
        String scenarioName = run.scenario().replaceFirst("\\.ya?ml$", "");
        boolean resolved = Files.isRegularFile(runDirectory.resolve(RESOLVED_CONFIG));
        report.append("Run `").append(run.runId()).append("`, image `").append(run.image()).append("`.\n\n")
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
                        workload.path("numberOfMessages").asLong(), workload.path("warmupMessages").asLong()
                                * Math.max(1, workload.path("warmupRounds").asInt()))))
                .append(row("Payload", workload.path("payloadBytes").asInt() + " bytes, batching "
                        + (workload.path("batchingEnabled").asBoolean() ? "on" : "off")))
                .append(row("Rate limit", workload.path("rate").asLong() > 0
                        ? String.format(Locale.ROOT, "%,d msg/s", workload.path("rate").asLong()) : "none"));
        appendRunInfo(report, run.info());
    }

    // The run's own records, collapsed at the end, for a reader who browses the run directory, for example over HTTP
    private static void appendFiles(StringBuilder report, Path runDirectory) {
        List<String> links = new ArrayList<>();
        // The scenario and its resolved configuration are linked from the settings table
        for (String name : List.of(RunInfo.FILE_NAME, "producer/producer-summary.json",
                "producer/" + PerformanceLauncher.CONTAINER_LOG)) {
            Path file = runDirectory.resolve(name);
            if (Files.isRegularFile(file)) {
                links.add(link(runDirectory, file));
            }
        }
        for (int application = 0; Files.isDirectory(runDirectory.resolve("consumer-" + application)); application++) {
            for (String name : List.of("consumer-summary.json", PerformanceLauncher.CONTAINER_LOG)) {
                Path file = runDirectory.resolve("consumer-" + application).resolve(name);
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

    /** A link to {@code file}, relative to the run directory, named by its path. */
    private static String link(Path runDirectory, Path file) {
        String relative = runDirectory.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize())
                .toString().replace('\\', '/');
        return "[" + relative + "](" + relative + ")";
    }

    // Where, by whom and from which code the run was made; the same values are in run-info.json
    private static void appendRunInfo(StringBuilder report, RunInfo info) {
        if (info == null) {
            return;
        }
        String user = info.user();
        if (!info.gitUserName().isEmpty()) {
            user += " (git: " + info.gitUserName()
                    + (info.gitUserEmail().isEmpty() ? "" : " <" + info.gitUserEmail() + ">") + ")";
        }
        report.append(row("Started", info.started().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
                .append(row("Host", info.host()))
                .append(row("User", user))
                .append(row("Project directory", code(info.projectDirectory().toString())))
                .append(row("Git branch", code(info.gitBranch())))
                .append(row("Git commit", info.gitCommit().isEmpty() ? ""
                        : code(info.gitCommit()) + (info.gitDirty() ? ", with uncommitted changes" : "")))
                .append(row("Pulsar version", info.version()));
    }

    private static String code(String value) {
        return value.isEmpty() ? "" : "`" + value + "`";
    }

    private static void appendCorrectness(StringBuilder report, List<JsonNode> consumers) {
        report.append("\n## Correctness\n\n| Application | Unique messages | Duplicates | Ordering violations |"
                + " Invalid |\n|---|---:|---:|---:|---:|\n");
        boolean clean = true;
        for (JsonNode consumer : consumers) {
            long duplicates = consumer.path("duplicates").asLong();
            long violations = consumer.path("orderingViolations").asLong();
            long invalid = consumer.path("invalidMessages").asLong();
            clean &= duplicates == 0 && violations == 0 && invalid == 0;
            report.append(String.format(Locale.ROOT, "| %d | %,d | %,d | %,d | %,d |%n",
                    consumer.path("applicationIndex").asInt(), consumer.path("uniqueMessages").asLong(),
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

    private static void appendLatency(StringBuilder report, Path runDirectory, Run run, List<Path> consumers)
            throws IOException {
        Path publish = runDirectory.resolve("producer/produce-latency.hdr");
        if (!Files.isRegularFile(publish) || consumers.isEmpty()) {
            return;
        }
        report.append("\n## Latency\n\n| Latency (ms) | Count | Min | p50 | p90 | p99 | p99.9 | Max |\n"
                + "|---|---:|---:|---:|---:|---:|---:|---:|\n");
        Histogram published = HdrHistogramRenderer.readMerged(List.of(publish));
        report.append(latencyRow("Publish (send to acknowledgment)", published));
        Histogram endToEnd = HdrHistogramRenderer.readMerged(consumers);
        if (consumers.size() > 1) {
            for (Path consumer : consumers) {
                report.append(latencyRow("End to end, " + consumer.getParent().getFileName(),
                        HdrHistogramRenderer.readMerged(List.of(consumer))));
            }
        }
        report.append(latencyRow("End to end (publish to listener)", endToEnd));
        report.append(String.format(Locale.ROOT, "%nDelivery after the publish is acknowledged: about %s ms"
                        + " (end-to-end p50 − publish p50).%n",
                millis(endToEnd.getValueAtPercentile(50) - published.getValueAtPercentile(50))));
        HdrHistogramRenderer.render(publish, consumers, runDirectory.resolve(LATENCY_CHART), "Latency",
                chartFooter(run.info(), run.finished()));
        report.append("\n![Latency distributions](").append(LATENCY_CHART).append(".svg)\n");
        List<Path> logs = new ArrayList<>();
        logs.add(publish);
        logs.addAll(consumers);
        // Collapsed, as the logs are for tools such as HdrHistogram's plotter rather than for reading
        report.append("\n<details><summary>HDR histogram logs</summary>\n\n");
        for (Path log : logs) {
            report.append("- ").append(link(runDirectory, log)).append('\n');
        }
        report.append("\n</details>\n");
    }

    private static void appendTopicStats(StringBuilder report, Path runDirectory, Samples samples,
                                         long measurementStart, long measurementEnd, String footer)
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
                .append(" [sampled topic stats](").append(TopicStatsSampler.FILE_NAME).append(") are a CSV file.\n\n")
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
        List<TimeSeriesRenderer.Series> throughput = new ArrayList<>();
        // Dotted, so that the consumer lines it usually overlaps stay visible
        throughput.add(new TimeSeriesRenderer.Series("Producers (published)", samples.published(), true));
        for (Map.Entry<String, double[]> entry : samples.dispatched().entrySet()) {
            // One line per subscription, named by it alone so that the legend fits
            throughput.add(new TimeSeriesRenderer.Series(samples.dispatched().size() == 1
                    ? "Consumers (dispatched)" : entry.getKey(), entry.getValue()));
        }
        TimeSeriesRenderer.render(runDirectory.resolve(THROUGHPUT_CHART), "Throughput", "Messages per second",
                seconds, throughput, finished, footer);
        List<TimeSeriesRenderer.Series> backlog = samples.backlog().entrySet().stream()
                .map(entry -> new TimeSeriesRenderer.Series(entry.getKey(), entry.getValue())).toList();
        TimeSeriesRenderer.render(runDirectory.resolve(BACKLOG_CHART), "Backlog", "Messages in the backlog",
                seconds, backlog, finished, footer);
        report.append("\n![Throughput over time](").append(THROUGHPUT_CHART).append(".svg)\n\n![Backlog over time](")
                .append(BACKLOG_CHART).append(".svg)\n");
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

    /** Reads {@code topic-stats.csv} into per-round published and dispatched rates and backlogs. */
    static Samples readSamples(Path csv) throws IOException {
        // epoch -> topic -> in counter; epoch -> subscription key -> [backlog, out counter]
        TreeMap<Long, Map<String, Long>> inCounters = new TreeMap<>();
        TreeMap<Long, Map<String, long[]>> subscriptions = new TreeMap<>();
        List<String> lines = Files.readAllLines(csv);
        boolean singleTopic = lines.stream().skip(1).map(line -> line.split(",")[1]).distinct().count() <= 1;
        for (String line : lines.subList(1, lines.size())) {
            String[] fields = line.split(",");
            long epoch = Long.parseLong(fields[0]);
            String key = singleTopic ? fields[2] : fields[1] + " " + fields[2];
            inCounters.computeIfAbsent(epoch, e -> new LinkedHashMap<>()).put(fields[1], Long.parseLong(fields[4]));
            subscriptions.computeIfAbsent(epoch, e -> new TreeMap<>())
                    .put(key, new long[] {Long.parseLong(fields[3]), Long.parseLong(fields[5])});
        }
        long[] epochs = inCounters.keySet().stream().mapToLong(Long::longValue).toArray();
        int rounds = epochs.length;
        double[] published = new double[rounds];
        Arrays.fill(published, Double.NaN);
        Map<String, double[]> dispatched = new TreeMap<>();
        Map<String, double[]> backlog = new TreeMap<>();
        for (int round = 0; round < rounds; round++) {
            Map<String, long[]> current = subscriptions.get(epochs[round]);
            for (Map.Entry<String, long[]> entry : current.entrySet()) {
                backlog.computeIfAbsent(entry.getKey(), k -> nanArray(rounds))[round] = entry.getValue()[0];
                dispatched.computeIfAbsent(entry.getKey(), k -> nanArray(rounds));
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
            Map<String, long[]> previous = subscriptions.get(epochs[round - 1]);
            for (Map.Entry<String, long[]> entry : current.entrySet()) {
                long[] before = previous.get(entry.getKey());
                if (before != null && entry.getValue()[1] >= before[1]) {
                    dispatched.get(entry.getKey())[round] = (entry.getValue()[1] - before[1]) / elapsed;
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
