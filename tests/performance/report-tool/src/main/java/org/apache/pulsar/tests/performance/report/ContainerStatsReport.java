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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The run report's section on the host and the containers during the measurement: the host's CPU and disk use from
 * {@code host-io.csv}, and each container's CPU use and context switches from {@code container-stats.csv} and its
 * perf counts from {@code perf-stat.csv}: CPU migrations, page faults, the clock rate, instructions per cycle, and
 * cache and branch misses per thousand instructions. The same numbers go to {@code container-summary.json} for scripts
 * and agents.
 */
final class ContainerStatsReport {
    /** The perf columns of {@code perf-stat.csv} after the container's name, in order. */
    static final List<String> PERF_COLUMNS = List.of(RunReport.PERF_STAT_HEADER.split(",")).subList(2,
            RunReport.PERF_STAT_HEADER.split(",").length);

    /** A container's per-second averages within the measurement, or their sum over the containers. */
    static final class Row {
        boolean stats;
        double cpus;
        double voluntary;
        double involuntary;
        boolean perf;
        // Per-second averages of the perf columns; a column with an empty value in any row is missing
        final Map<String, Double> counts = new LinkedHashMap<>();
        final Set<String> missing = new HashSet<>();

        void add(Row other) {
            stats |= other.stats;
            cpus += other.cpus;
            voluntary += other.voluntary;
            involuntary += other.involuntary;
            perf |= other.perf;
            other.counts.forEach((column, value) -> counts.merge(column, value, Double::sum));
            missing.addAll(other.missing);
        }

        /** A perf count per second, or NaN when it isn't available. */
        double count(String column) {
            return perf && !missing.contains(column) ? counts.getOrDefault(column, Double.NaN) : Double.NaN;
        }

        double ghz() {
            return count("cycles") / (count("taskClockMillis") * 1e6);
        }

        double ipc() {
            return count("instructions") / count("cycles");
        }

        /** Events per thousand instructions. */
        double perKiloInstructions(String column) {
            return count(column) / count("instructions") * 1000;
        }

        double cacheMissPercent() {
            return count("cacheMisses") / count("cacheReferences") * 100;
        }
    }

    private ContainerStatsReport() {
    }

    /**
     * Each container's per-second averages within the measurement. A row covers the second before its time, so the
     * rows from one second after the measurement's start to its end are counted.
     */
    static Map<String, Row> rows(Path runDirectory, long measurementStart, long measurementEnd) throws IOException {
        Map<String, double[]> stats = new LinkedHashMap<>();
        Map<String, Integer> statsRows = new LinkedHashMap<>();
        for (String[] row : csvRows(runDirectory.resolve(RunReport.CONTAINER_STATS_FILE))) {
            if (row.length < 5 || !inMeasurement(row[0], measurementStart, measurementEnd)) {
                continue;
            }
            double[] sums = stats.computeIfAbsent(row[1], name -> new double[3]);
            for (int i = 0; i < 3; i++) {
                sums[i] += value(row[2 + i]);
            }
            statsRows.merge(row[1], 1, Integer::sum);
        }
        Map<String, Row> rows = new LinkedHashMap<>();
        stats.forEach((name, sums) -> {
            Row row = rows.computeIfAbsent(name, n -> new Row());
            int count = statsRows.get(name);
            row.stats = true;
            row.cpus = sums[0] / count;
            row.voluntary = sums[1] / count;
            row.involuntary = sums[2] / count;
        });
        Map<String, Integer> perfRows = new LinkedHashMap<>();
        for (String[] fields : csvRows(runDirectory.resolve(RunReport.PERF_STAT_FILE))) {
            if (fields.length < 2 + PERF_COLUMNS.size()
                    || !inMeasurement(fields[0], measurementStart, measurementEnd)) {
                continue;
            }
            Row row = rows.computeIfAbsent(fields[1], n -> new Row());
            row.perf = true;
            for (int i = 0; i < PERF_COLUMNS.size(); i++) {
                String text = fields[2 + i];
                if (text.isEmpty()) {
                    row.missing.add(PERF_COLUMNS.get(i));
                } else {
                    row.counts.merge(PERF_COLUMNS.get(i), Double.parseDouble(text), Double::sum);
                }
            }
            perfRows.merge(fields[1], 1, Integer::sum);
        }
        perfRows.forEach((name, count) -> rows.get(name).counts.replaceAll((column, sum) -> sum / count));
        return rows;
    }

    /**
     * Appends the section and writes {@code container-summary.json}, or does nothing when the run has none of the
     * files.
     */
    static void append(StringBuilder report, Path runDirectory, long measurementStart, long measurementEnd,
                       long measuredMessages) throws IOException {
        Map<String, Row> rows = rows(runDirectory, measurementStart, measurementEnd);
        Map<String, Double> host = hostAverages(runDirectory.resolve(RunReport.HOST_IO_FILE), measurementStart,
                measurementEnd);
        if (rows.isEmpty() && host.isEmpty()) {
            return;
        }
        double seconds = (measurementEnd - measurementStart) / 1000.0;
        Row all = new Row();
        rows.values().forEach(all::add);
        writeSummary(runDirectory, measurementStart, measurementEnd, measuredMessages, host, rows, all);
        report.append("\n## Containers\n\n");
        if (!host.isEmpty()) {
            report.append(hostLine(host)).append("\n\n");
        }
        if (rows.isEmpty()) {
            return;
        }
        boolean perf = all.perf;
        report.append("Each container's CPU use during the measurement, from its cgroup, and its threads' context")
                .append(" switches: voluntary when a thread blocked or waited for work, involuntary when the scheduler")
                .append(" preempted it, which grows when the host's CPUs are saturated. The [sampled container")
                .append(" stats](").append(RunReport.CONTAINER_STATS_FILE).append(") are a CSV file, and")
                .append(" [the summary](").append(RunReport.CONTAINER_SUMMARY_FILE).append(") holds these tables'")
                .append(" numbers as JSON.\n\n| Container | CPUs | CPU s per million messages | Voluntary switches/s |")
                .append(" Involuntary switches/s |").append(perf ? " CPU migrations/s | Page faults/s |" : "")
                .append("\n|---|---:|---:|---:|---:|").append(perf ? "---:|---:|" : "").append('\n');
        for (Map.Entry<String, Row> entry : rows.entrySet()) {
            appendUseRow(report, entry.getKey(), entry.getValue(), perf, seconds, measuredMessages);
        }
        if (rows.size() > 1) {
            appendUseRow(report, "**All containers**", all, perf, seconds, measuredMessages);
        }
        if (!perf) {
            return;
        }
        report.append("\nHow efficiently the containers' threads ran, from exact `perf stat` counts per container")
                .append(" cgroup ([perf counts](").append(RunReport.PERF_STAT_FILE).append(")): the clock rate, the")
                .append(" instructions per cycle (IPC), the misses of the last-level cache, the L1 data cache and the")
                .append(" branch predictor per thousand instructions (MPKI), and the share of last-level cache")
                .append(" references that missed. A lower IPC with more cache misses per instruction means more time")
                .append(" waiting for memory.\n\n")
                .append("| Container | GHz | IPC | LLC MPKI | LLC miss % | L1D MPKI | Branch MPKI |\n")
                .append("|---|---:|---:|---:|---:|---:|---:|\n");
        for (Map.Entry<String, Row> entry : rows.entrySet()) {
            appendEfficiencyRow(report, entry.getKey(), entry.getValue());
        }
        if (rows.size() > 1) {
            appendEfficiencyRow(report, "**All containers**", all);
        }
    }

    private static void appendUseRow(StringBuilder report, String label, Row row, boolean perf, double seconds,
                                     long measuredMessages) {
        report.append("| ").append(label).append(" | ");
        if (row.stats) {
            report.append(format("%.2f", row.cpus)).append(" | ")
                    .append(measuredMessages > 0 ? format("%,.1f", row.cpus * seconds / (measuredMessages / 1e6))
                            : "").append(" | ")
                    .append(format("%,.0f", row.voluntary)).append(" | ")
                    .append(format("%,.0f", row.involuntary)).append(" |");
        } else {
            report.append(" |  |  |  |");
        }
        if (perf) {
            report.append(' ').append(format("%,.0f", row.count("cpuMigrations"))).append(" | ")
                    .append(format("%,.0f", row.count("pageFaults"))).append(" |");
        }
        report.append('\n');
    }

    private static void appendEfficiencyRow(StringBuilder report, String label, Row row) {
        report.append("| ").append(label).append(" | ").append(format("%.2f", row.ghz())).append(" | ")
                .append(format("%.2f", row.ipc())).append(" | ")
                .append(format("%.2f", row.perKiloInstructions("cacheMisses"))).append(" | ")
                .append(format("%.1f", row.cacheMissPercent())).append(" | ")
                .append(format("%.2f", row.perKiloInstructions("l1dLoadMisses"))).append(" | ")
                .append(format("%.2f", row.perKiloInstructions("branchMisses"))).append(" |\n");
    }

    /** Writes the section's numbers as JSON: the measurement, the host, and a record per container and for all. */
    static void writeSummary(Path runDirectory, long measurementStart, long measurementEnd, long measuredMessages,
                             Map<String, Double> host, Map<String, Row> rows, Row all) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode summary = mapper.createObjectNode();
        ObjectNode measurement = summary.putObject("measurement");
        measurement.put("startEpochMillis", measurementStart).put("endEpochMillis", measurementEnd)
                .put("messages", measuredMessages);
        ObjectNode hostNode = summary.putObject("host");
        host.forEach((column, value) -> put(hostNode, column, value));
        ObjectNode containers = summary.putObject("containers");
        double seconds = (measurementEnd - measurementStart) / 1000.0;
        rows.forEach((name, row) -> containerNode(containers.putObject(name), row, seconds, measuredMessages));
        if (!rows.isEmpty()) {
            containerNode(summary.putObject("allContainers"), all, seconds, measuredMessages);
        }
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(runDirectory.resolve(RunReport.CONTAINER_SUMMARY_FILE).toFile(), summary);
    }

    private static void containerNode(ObjectNode node, Row row, double seconds, long measuredMessages) {
        if (row.stats) {
            put(node, "cpus", row.cpus);
            if (measuredMessages > 0) {
                put(node, "cpuSecondsPerMillionMessages", row.cpus * seconds / (measuredMessages / 1e6));
            }
            put(node, "voluntarySwitchesPerSecond", row.voluntary);
            put(node, "involuntarySwitchesPerSecond", row.involuntary);
        }
        if (row.perf) {
            for (String column : PERF_COLUMNS) {
                put(node, column + "PerSecond", row.count(column));
            }
            put(node, "ghz", row.ghz());
            put(node, "instructionsPerCycle", row.ipc());
            put(node, "llcMissesPerKiloInstructions", row.perKiloInstructions("cacheMisses"));
            put(node, "llcMissPercent", row.cacheMissPercent());
            put(node, "l1dMissesPerKiloInstructions", row.perKiloInstructions("l1dLoadMisses"));
            put(node, "branchMissesPerKiloInstructions", row.perKiloInstructions("branchMisses"));
        }
    }

    private static void put(ObjectNode node, String field, double value) {
        if (Double.isFinite(value)) {
            node.put(field, Math.round(value * 1000) / 1000.0);
        }
    }

    /**
     * The measurement's averages of {@code host-io.csv}'s columns by name, such as {@code cpuBusyPercent} and
     * {@code nvme0n1WriteMBps}; empty when the file is missing or has no rows within the measurement.
     */
    static Map<String, Double> hostAverages(Path csv, long measurementStart, long measurementEnd) throws IOException {
        Map<String, Double> averages = new LinkedHashMap<>();
        if (!Files.isRegularFile(csv)) {
            return averages;
        }
        List<String> lines = Files.readAllLines(csv);
        if (lines.size() < 2) {
            return averages;
        }
        String[] header = lines.get(0).split(",", -1);
        double[] sums = new double[header.length];
        int count = 0;
        for (String line : lines.subList(1, lines.size())) {
            String[] fields = line.split(",", -1);
            if (fields.length != header.length || !inMeasurement(fields[0], measurementStart, measurementEnd)) {
                continue;
            }
            for (int i = 1; i < fields.length; i++) {
                sums[i] += value(fields[i]);
            }
            count++;
        }
        for (int i = 1; count > 0 && i < header.length; i++) {
            averages.put(header[i], sums[i] / count);
        }
        return averages;
    }

    /** A sentence on the Docker engine host's CPU and disks during the measurement, a VM on macOS. */
    static String hostLine(Map<String, Double> host) {
        StringBuilder text = new StringBuilder(String.format(Locale.ROOT,
                "During the measurement, the Docker engine host's CPUs were %.1f %% busy and %.1f %% waiting for I/O "
                        + "on average",
                host.getOrDefault("cpuBusyPercent", Double.NaN), host.getOrDefault("cpuIowaitPercent", Double.NaN)));
        for (String column : host.keySet()) {
            if (column.endsWith("ReadMBps")) {
                String disk = column.substring(0, column.length() - "ReadMBps".length());
                text.append(String.format(Locale.ROOT, "; disk %s read %,.0f MB/s and wrote %,.0f MB/s, busy %.1f %%",
                        disk, host.get(column), host.getOrDefault(disk + "WriteMBps", Double.NaN),
                        host.getOrDefault(disk + "BusyPercent", Double.NaN)));
            }
        }
        return text.append(" ([sampled host I/O](").append(RunReport.HOST_IO_FILE).append(")).").toString();
    }

    private static boolean inMeasurement(String epochMillis, long measurementStart, long measurementEnd) {
        if (epochMillis.isEmpty()) {
            return false;
        }
        long time = Long.parseLong(epochMillis);
        return time >= measurementStart + 1000 && time <= measurementEnd;
    }

    private static List<String[]> csvRows(Path csv) throws IOException {
        if (!Files.isRegularFile(csv)) {
            return List.of();
        }
        return Files.readAllLines(csv).stream().skip(1).filter(line -> !line.isBlank())
                .map(line -> line.split(",", -1)).toList();
    }

    private static double value(String text) {
        return text.isEmpty() ? 0 : Double.parseDouble(text);
    }

    private static String format(String format, double value) {
        return Double.isFinite(value) ? String.format(Locale.ROOT, format, value) : "";
    }
}
