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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Writes the profile report into a directory of profiled recordings, such as {@code broker-profile/}: the run
 * it belongs to, and for each recording tables linking its files (the off-CPU digest, the JFR recordings, the capture
 * stream and the patterns used), the off-CPU flame graphs with their totals, and the CPU, allocation, lock or
 * wall-clock views that were rendered, followed by where to open the JFR recordings when any remain. Links are
 * relative, so the directory can be moved or archived, and only files that exist are listed.
 */
public final class ProfileReport {
    // The directory's README.md, with its HTML page the directory's index.html, so that the directory opens on it
    public static final String FILE_NAME = "README.md";
    static final String ECLIPSE_MISSION_CONTROL = "https://adoptium.net/jmc";
    // The JDK troubleshooting guide's chapter on finding performance issues in a recording with JDK Mission Control
    static final String JFR_TROUBLESHOOTING_GUIDE = "https://docs.oracle.com/en/java/javase/25/troubleshoot/"
            + "troubleshoot-performance-issues-using-jfr.html#GUID-0FE29092-18B5-4BEB-8D8D-0CBA7A4FEA1D";

    /** The run that produced the recordings. */
    public record Run(String scenario, String runId, Instant from, Instant to, double producerMessagesPerSecond) {
    }

    private record Slice(String name, String label, boolean idleLeftOut) {
    }

    // The blocked time first: the time that can limit throughput, where the process waited while it had work
    private static final List<Slice> SLICES = List.of(
            new Slice(OffCpuFlamegraphs.NO_IDLE_SLICE, "Blocked time", true),
            new Slice(OffCpuFlamegraphs.NO_IDLE_APP_ROOT_SLICE,
                    "Blocked time, from where threads entered Pulsar or BookKeeper code", true),
            new Slice(OffCpuFlamegraphs.ALL_SLICE, "All off-CPU time, with idle waits", false),
            new Slice(OffCpuFlamegraphs.APP_ROOT_SLICE,
                    "All off-CPU time, from where threads entered Pulsar or BookKeeper code", false));

    private ProfileReport() {
    }

    /** The profiled process, as the report's text names it: {@code broker-profile} is "the broker". */
    static String processName(Path directory) {
        String name = directory.getFileName().toString();
        if (name.startsWith("broker")) {
            return "the broker";
        } else if (name.equals("gateways")) {
            return "the gateways' Pulsar clients";
        } else if (name.equals(RunReport.APPLICATIONS_DIRECTORY)) {
            return "the applications' Pulsar clients";
        }
        return "the profiled process";
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
    public static Path write(Path directory, List<Path> recordings, Run run, ObjectMapper mapper, Path root)
            throws IOException {
        StringBuilder report = new StringBuilder();
        report.append("# Profile report: ").append(directory.getFileName()).append("\n\n");
        Duration window = Duration.between(run.from(), run.to());
        report.append("Scenario `").append(run.scenario()).append("`, run `").append(run.runId()).append('`');
        Path runDirectory = root.toAbsolutePath().normalize();
        Path profileDirectory = directory.toAbsolutePath().normalize();
        if (!profileDirectory.equals(runDirectory) && profileDirectory.startsWith(runDirectory)) {
            String runReport = profileDirectory.relativize(runDirectory.resolve(RunReport.FILE_NAME)).toString()
                    .replace('\\', '/');
            report.append(", see [the run report](").append(runReport).append(')');
        }
        report.append(".\n")
                .append("Measurement window ").append(run.from()).append(" to ").append(run.to())
                .append(String.format(Locale.ROOT, " (%.1f s)", window.toMillis() / 1000.0));
        if (run.producerMessagesPerSecond() > 0) {
            report.append(String.format(Locale.ROOT, "; gateways' throughput %,.0f msg/s",
                    run.producerMessagesPerSecond()));
        }
        report.append(".\n");
        for (int index = 0; index < recordings.size(); index++) {
            String base = base(recordings.get(index));
            // The recording's generated name says nothing to a reader; several recordings are just numbered
            if (recordings.size() > 1) {
                report.append("\n## Recording ").append(index + 1).append("\n");
            }
            appendFiles(report, directory, base);
            appendOffCpu(report, directory, base, mapper);
            appendViews(report, directory, base);
        }
        if (recordings.stream().anyMatch(recording -> hasJfrRecording(directory, base(recording)))) {
            // A section of its own among numbered recordings, like the sections of a single recording otherwise
            appendMissionControl(report, recordings.size() > 1 ? "## " : "### ");
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

    // The digest, the recordings and the patterns the off-CPU outputs used; retention may have removed some
    private static void appendFiles(StringBuilder report, Path directory, String base) {
        String offCpu = base + OffCpuFlamegraphs.OUTPUT_SUFFIX + "/";
        String digest = offCpu + OffCpuFlamegraphs.SUMMARY_FILE;
        StringBuilder rows = new StringBuilder();
        for (String[] file : new String[][] {
                // The place to start reading a profile, so it stands out, and its description links it too
                {digest, "**jonoffcpu report (off-CPU summary)**", "**[Start here](" + digest + ")**: the"
                        + " methods where the threads of " + processName(directory) + " blocked, what they blocked on"
                        + " and for how long, ranked by their blocked time, which points to the contention that can"
                        + " limit throughput; the blocked time flame graphs below show the code paths that lead to"
                        + " them"},
                {base + ".jfr", "JFR recording", "The complete recording, for JDK Mission Control or the converter"},
                {base + ".measurement.jfr", "JFR recording for the measurement period", "Cut to the measurement"
                        + " window; the CPU, allocation, lock and wall-clock views are rendered from it"},
                {base + OffCpuFlamegraphs.CAPTURE_SUFFIX, "Off-CPU capture stream", "The kernel's off-CPU intervals;"
                        + " correlating it with the JFR recording again reproduces the off-CPU outputs"},
                {offCpu + OffCpuFlamegraphs.IDLE_WAITS_FILE, "Idle-wait patterns", "The waits for work that the"
                        + " digest and the flame graphs without idle waits leave out"},
                {offCpu + OffCpuFlamegraphs.DISPATCH_HIDE_FILE, "Dispatch frames", "Frames that only dispatch work,"
                        + " hidden with jonoffcpu's `jvm-dispatch` preset before stacks start at the application"}}) {
            if (Files.isRegularFile(directory.resolve(file[0]))) {
                // A bold name makes the whole link bold: **[name](file)**
                String text = file[1];
                String link = text.startsWith("**")
                        ? "**[" + text.substring(2, text.length() - 2) + "](" + file[0] + ")**"
                        : "[" + text + "](" + file[0] + ")";
                rows.append("| ").append(link).append(" | ").append(file[2]).append(" |\n");
            }
        }
        if (!rows.isEmpty()) {
            report.append("\n| File | Contents |\n|---|---|\n").append(rows);
        }
    }

    // Retention may have removed both recordings, which leaves nothing to open
    private static boolean hasJfrRecording(Path directory, String base) {
        return Files.isRegularFile(directory.resolve(base + ".jfr"))
                || Files.isRegularFile(directory.resolve(base + ".measurement.jfr"));
    }

    private static void appendMissionControl(StringBuilder report, String heading) {
        report.append('\n').append(heading).append("Opening the recordings in JDK Mission Control\n\n")
                .append("The JFR recordings open in JDK Mission Control, whose OpenJDK distribution is [Eclipse")
                .append(" Mission Control](").append(ECLIPSE_MISSION_CONTROL).append("). The JDK's [Troubleshoot")
                .append(" Performance Issues Using Flight Recorder](").append(JFR_TROUBLESHOOTING_GUIDE)
                .append(") guide describes finding performance issues in a recording with it.\n");
    }

    private static void appendOffCpu(StringBuilder report, Path directory, String base, ObjectMapper mapper)
            throws IOException {
        String offCpu = base + OffCpuFlamegraphs.OUTPUT_SUFFIX;
        if (!Files.isDirectory(directory.resolve(offCpu))) {
            return;
        }
        report.append("\n### Off-CPU flame graphs: where threads waited\n\n");
        String process = processName(directory);
        report.append("jonoffcpu's off-CPU capture records the time the threads of ").append(process)
                .append(" weren't running on a CPU. These flame graphs show which code paths the threads were in")
                .append(" during that time, as call trees where a box's width is off-CPU time: the wider a path, the")
                .append(" more time threads spent stopped in it. The blocked time flame graphs show where ")
                .append(process)
                .append(" waited while it had work to do, such as on a lock, a monitor or I/O, which is the waiting")
                .append(" that can limit throughput; the jonoffcpu report ranks the same time by the method that")
                .append(" waited. The flame graphs of all off-CPU time also show the threads that were idle, waiting")
                .append(" for new work.\n\n");
        report.append("| Flame graph | Off-CPU s | Intervals | Left out as idle s | Left out without an application"
                + " frame s |\n|---|---:|---:|---:|---:|\n");
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
                    .append(" | ").append(seconds(totals.path("rootAtUnmatchedHidden").path("totalNanos")))
                    .append(" |\n");
        }
        report.append("\n<details><summary>How the off-CPU time is split</summary>\n\n")
                .append("- The width of a box is the observed off-CPU time of the waits that the capture recorded.")
                .append(" The scenario's off-CPU sampling policy can record long waits in full and only a share of")
                .append(" the short ones, so the observed time under-weights short waits; the correlator's")
                .append(" `--weights estimated` corrects for that sampling. Waits shorter than the policy's minimum")
                .append(" aren't recorded at all.\n")
                .append("- The blocked time leaves out the idle waits, the threads that waited for new work, such as")
                .append(" Netty event loops in `epollWait` and executor workers waiting for a task. The patterns in `")
                .append(OffCpuFlamegraphs.IDLE_WAITS_FILE).append("` recognize them, and the \"Left out as idle\"")
                .append(" column is their time.\n")
                .append("- The flame graphs from where threads entered Pulsar or BookKeeper code start each stack")
                .append(" at its")
                .append(" root-most frame matching `").append(OffCpuFlamegraphs.APPLICATION_ROOT).append("`, once the")
                .append(" frames that only dispatch work (`").append(OffCpuFlamegraphs.DISPATCH_HIDE_FILE)
                .append("`) are hidden, so that the same code reached from different thread pools joins into one")
                .append(" tree. Stacks without such a frame, such as the JVM's own threads, are left out of them and")
                .append(" counted in the last column; the jonoffcpu report ranks them by thread pool.\n")
                .append("- Each flame graph has a `.collapsed` file with full names and a `.json` summary beside")
                .append(" it.\n\n</details>\n");
    }

    private static void appendViews(StringBuilder report, Path directory, String base) {
        String views = base + JfrFlamegraphViews.OUTPUT_SUFFIX;
        if (!Files.isDirectory(directory.resolve(views))) {
            return;
        }
        StringBuilder rows = new StringBuilder();
        List<String> rendered = new ArrayList<>();
        List<String> shows = new ArrayList<>();
        for (JfrFlamegraphViews.View view : JfrFlamegraphViews.View.values()) {
            String label = view.label();
            if (!Files.isRegularFile(directory.resolve(views).resolve(label + ".html"))) {
                continue;
            }
            rendered.add(view.description());
            // What each view shows, and what its widths count: samples, or the bytes and time that --total weighs
            shows.add(switch (view) {
                case CPU -> "the CPU flame graph shows where they used the CPU (width: samples)";
                case WALL -> "the wall-clock flame graph shows where they spent their time, running or not (width:"
                        + " samples)";
                case ALLOC -> "the allocation flame graph shows where they allocated memory (width: bytes)";
                case LOCK -> "the lock flame graph shows where they waited to enter Java monitors or were parked,"
                        + " which includes threads waiting idle for work (width: time)";
            });
            rows.append("| ").append(label).append(" | ")
                    .append(link(directory, views, label + ".html", "flame graph")).append(" | ")
                    .append(link(directory, views, label + JfrFlamegraphViews.THREADS_SUFFIX + ".html",
                            "by thread")).append(" | ")
                    .append(link(directory, views, label + JfrFlamegraphViews.HEATMAP_SUFFIX + ".html",
                            "heatmap")).append(" | ")
                    .append(link(directory, views, label + ".collapsed", "collapsed")).append(" |\n");
        }
        if (!rows.isEmpty()) {
            // Unlike the off-CPU flame graphs, these come from the samples in the JFR recording
            report.append("\n### async-profiler flame graphs: ").append(inProse(rendered)).append("\n\n")
                    .append("async-profiler, which the jonoffcpu agent runs in the process, sampled the threads of ")
                    .append(processName(directory)).append(" into the JFR recording: ").append(inProse(shows))
                    .append(". Each view is also split by thread, and shown over time as a heatmap for bursts and")
                    .append(" pauses.\n\n")
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
