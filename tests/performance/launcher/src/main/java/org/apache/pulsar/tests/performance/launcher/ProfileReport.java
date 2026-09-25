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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Writes {@code profile-report.md} into a directory of profiled recordings, such as {@code broker-profile/}: the run
 * it belongs to, and for each recording links to the off-CPU digest, the off-CPU flame graphs with their totals, and
 * the CPU, allocation, lock or wall-clock views that were rendered. Links are relative, so the directory can be moved
 * or archived, and only files that exist are listed.
 */
final class ProfileReport {
    static final String FILE_NAME = "profile-report.md";

    /** The run that produced the recordings. */
    record Run(String scenario, String runId, Instant from, Instant to, double producerMessagesPerSecond) {
    }

    private record Slice(String name, String label, boolean idleLeftOut) {
    }

    private static final List<Slice> SLICES = List.of(
            new Slice(OffCpuFlamegraphs.ALL_SLICE, "All off-CPU time", false),
            new Slice(OffCpuFlamegraphs.NO_IDLE_SLICE, "Without idle waits", true),
            new Slice(OffCpuFlamegraphs.APP_ROOT_SLICE, "From the application's first frame", false),
            new Slice(OffCpuFlamegraphs.NO_IDLE_APP_ROOT_SLICE,
                    "Without idle waits, from the application's first frame", true));

    private ProfileReport() {
    }

    /**
     * Writes the report for the recordings in {@code directory}.
     *
     * <p>The report and each off-CPU digest also get an HTML page beside them, in which links to the other
     * Markdown pages point to their HTML pages and absolute paths inside {@code root} are relative.
     *
     * @param recordings the original recordings in the directory, which name their output directories; the files
     *                   themselves may have been removed by retention
     * @param root the run directory
     * @return the report file
     */
    static Path write(Path directory, List<Path> recordings, Run run, ObjectMapper mapper, Path root)
            throws IOException {
        StringBuilder report = new StringBuilder();
        report.append("# Profile report: ").append(directory.getFileName()).append("\n\n");
        Duration window = Duration.between(run.from(), run.to());
        report.append("Scenario `").append(run.scenario()).append("`, run `").append(run.runId()).append("`.\n")
                .append("Measurement window ").append(run.from()).append(" to ").append(run.to())
                .append(String.format(Locale.ROOT, " (%.1f s)", window.toMillis() / 1000.0));
        if (run.producerMessagesPerSecond() > 0) {
            report.append(String.format(Locale.ROOT, "; producer throughput %,.0f msg/s",
                    run.producerMessagesPerSecond()));
        }
        report.append(".\n");
        for (Path recording : recordings) {
            String base = base(recording);
            report.append("\n## ").append(base).append("\n");
            appendOffCpu(report, directory, base, mapper);
            appendViews(report, directory, base);
        }
        Path file = directory.resolve(FILE_NAME);
        Files.writeString(file, report);
        MarkdownPages.renderHtml(file, root, "Profile report: " + directory.getFileName());
        for (Path recording : recordings) {
            Path digest = directory.resolve(base(recording) + OffCpuFlamegraphs.OUTPUT_SUFFIX)
                    .resolve(OffCpuFlamegraphs.SUMMARY_FILE);
            if (Files.isRegularFile(digest)) {
                // The digest names every frame in full; its page abbreviates them as the flame graphs do
                MarkdownPages.renderHtml(digest, root, "Off-CPU digest: " + base(recording), true);
            }
        }
        return file;
    }

    private static void appendOffCpu(StringBuilder report, Path directory, String base, ObjectMapper mapper)
            throws IOException {
        String offCpu = base + OffCpuFlamegraphs.OUTPUT_SUFFIX;
        if (!Files.isDirectory(directory.resolve(offCpu))) {
            return;
        }
        report.append("\n### Off-CPU time\n\n");
        String summary = offCpu + "/" + OffCpuFlamegraphs.SUMMARY_FILE;
        if (Files.isRegularFile(directory.resolve(summary))) {
            report.append("Start with the digest, [").append(OffCpuFlamegraphs.SUMMARY_FILE).append("](")
                    .append(summary).append("): the blocked time ranked by the application method that waited,")
                    .append(" where the time went and the capture's coverage, leaving out the idle waits listed in [")
                    .append(OffCpuFlamegraphs.IDLE_WAITS_FILE).append("](").append(offCpu).append("/")
                    .append(OffCpuFlamegraphs.IDLE_WAITS_FILE).append(").\n\n");
        }
        report.append("| Flame graph | Off-CPU s | Intervals | Left out as idle s |\n|---|---:|---:|---:|\n");
        for (Slice slice : SLICES) {
            String html = offCpu + "/" + slice.name() + ".html";
            Path json = directory.resolve(offCpu).resolve(slice.name() + ".json");
            if (!Files.isRegularFile(directory.resolve(html)) || !Files.isRegularFile(json)) {
                continue;
            }
            JsonNode totals = mapper.readTree(json.toFile());
            report.append("| [").append(slice.label()).append("](").append(html).append(") | ")
                    .append(seconds(totals.path("totalNanos"))).append(" | ")
                    .append(count(totals.path("intervals"))).append(" | ")
                    .append(slice.idleLeftOut() ? seconds(totals.path("filtered").path("totalNanos")) : "")
                    .append(" |\n");
        }
        report.append("\nThe application's first frame is the root-most frame matching `")
                .append(OffCpuFlamegraphs.APPLICATION_ROOT).append("` once the frames that only dispatch work, listed")
                .append(" in [").append(OffCpuFlamegraphs.DISPATCH_HIDE_FILE).append("](").append(offCpu).append("/")
                .append(OffCpuFlamegraphs.DISPATCH_HIDE_FILE).append(") and jonoffcpu's `jvm-dispatch` preset, are")
                .append(" hidden; stacks without one are grouped as")
                .append(" `[no application frame]`. Each flame graph has a `.collapsed` file with full names and a")
                .append(" `.json` summary beside it.\n");
    }

    private static void appendViews(StringBuilder report, Path directory, String base) {
        String views = base + JfrFlamegraphViews.OUTPUT_SUFFIX;
        if (!Files.isDirectory(directory.resolve(views))) {
            return;
        }
        StringBuilder rows = new StringBuilder();
        List<String> rendered = new ArrayList<>();
        for (JfrFlamegraphViews.View view : JfrFlamegraphViews.View.values()) {
            String label = view.label();
            if (!Files.isRegularFile(directory.resolve(views).resolve(label + ".html"))) {
                continue;
            }
            rendered.add(view.description());
            rows.append("| ").append(label).append(" | ")
                    .append(link(directory, views, label + ".html", "flame graph")).append(" | ")
                    .append(link(directory, views, label + JfrFlamegraphViews.THREADS_SUFFIX + ".html",
                            "by thread")).append(" | ")
                    .append(link(directory, views, label + JfrFlamegraphViews.HEATMAP_SUFFIX + ".html",
                            "heatmap")).append(" | ")
                    .append(link(directory, views, label + ".collapsed", "collapsed")).append(" |\n");
        }
        if (!rows.isEmpty()) {
            report.append("\n### ").append(capitalize(inProse(rendered)))
                    .append(rendered.size() == 1 ? " view\n\n" : " views\n\n")
                    .append("| View | Flame graph | By thread | Over time | Stacks |\n|---|---|---|---|---|\n")
                    .append(rows);
        }
    }

    /** "CPU", "CPU and allocation", "CPU, allocation and lock". */
    static String inProse(List<String> items) {
        if (items.size() <= 1) {
            return String.join("", items);
        }
        return String.join(", ", items.subList(0, items.size() - 1)) + " and " + items.get(items.size() - 1);
    }

    private static String capitalize(String text) {
        return text.isEmpty() ? text : Character.toUpperCase(text.charAt(0)) + text.substring(1);
    }

    private static String link(Path directory, String subdirectory, String file, String text) {
        return Files.isRegularFile(directory.resolve(subdirectory).resolve(file))
                ? "[" + text + "](" + subdirectory + "/" + file + ")" : "";
    }

    // The slice summaries print their 64-bit counters as JSON strings.
    private static String seconds(JsonNode nanos) {
        return nanos.isMissingNode() ? "" : String.format(Locale.ROOT, "%,.1f",
                Long.parseLong(nanos.asText()) / 1e9);
    }

    private static String count(JsonNode value) {
        return value.isMissingNode() ? "" : String.format(Locale.ROOT, "%,d", Long.parseLong(value.asText()));
    }

    private static String base(Path recording) {
        String name = recording.getFileName().toString();
        return name.endsWith(".jfr") ? name.substring(0, name.length() - ".jfr".length()) : name;
    }
}
