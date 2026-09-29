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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The run report's section on the containers: each container's CPU use, context switches, CPU migrations, clock rate
 * and instructions per cycle during the measurement, from {@code container-stats.csv} and {@code perf-stat.csv}.
 */
final class ContainerStatsReport {
    /** Sums of one container's rows within the measurement. */
    static final class Totals {
        int statsRows;
        double cpuCores;
        double voluntary;
        double involuntary;
        int perfRows;
        double taskClockMillis;
        double contextSwitches;
        double migrations;
        double cycles;
        double instructions;
        boolean cyclesMissing;
    }

    private ContainerStatsReport() {
    }

    /**
     * Sums the rows of both files within the measurement, per container. A row covers the second before its time, so
     * the rows from one second after the measurement's start to its end are counted.
     */
    static Map<String, Totals> totals(Path runDirectory, long measurementStart, long measurementEnd)
            throws IOException {
        Map<String, Totals> totals = new LinkedHashMap<>();
        for (String[] row : rows(runDirectory.resolve(RunReport.CONTAINER_STATS_FILE))) {
            long epochMillis = Long.parseLong(row[0]);
            if (epochMillis < measurementStart + 1000 || epochMillis > measurementEnd || row.length < 5) {
                continue;
            }
            Totals t = totals.computeIfAbsent(row[1], name -> new Totals());
            t.statsRows++;
            t.cpuCores += Double.parseDouble(row[2]);
            t.voluntary += Double.parseDouble(row[3]);
            t.involuntary += Double.parseDouble(row[4]);
        }
        for (String[] row : rows(runDirectory.resolve(RunReport.PERF_STAT_FILE))) {
            long epochMillis = Long.parseLong(row[0]);
            if (epochMillis < measurementStart + 1000 || epochMillis > measurementEnd || row.length < 7) {
                continue;
            }
            Totals t = totals.computeIfAbsent(row[1], name -> new Totals());
            t.perfRows++;
            t.taskClockMillis += value(row[2]);
            t.contextSwitches += value(row[3]);
            t.migrations += value(row[4]);
            t.cycles += value(row[5]);
            t.instructions += value(row[6]);
            t.cyclesMissing |= row[5].isEmpty() || row[6].isEmpty();
        }
        return totals;
    }

    /** Appends the section, or nothing when the run has neither file. */
    static void append(StringBuilder report, Path runDirectory, long measurementStart, long measurementEnd,
                       long measuredMessages) throws IOException {
        Map<String, Totals> totals = totals(runDirectory, measurementStart, measurementEnd);
        if (totals.isEmpty()) {
            return;
        }
        boolean perf = totals.values().stream().anyMatch(t -> t.perfRows > 0);
        double seconds = (measurementEnd - measurementStart) / 1000.0;
        report.append("\n## Containers\n\nEach container's CPU use during the measurement, from its cgroup, and its")
                .append(" threads' context switches: voluntary when a thread blocked or waited for work, involuntary")
                .append(" when the scheduler preempted it, which grows when the host's CPUs are saturated. The")
                .append(" [sampled container stats](").append(RunReport.CONTAINER_STATS_FILE)
                .append(") are a CSV file.");
        if (perf) {
            report.append(" The perf columns are exact counts of `perf stat` per container cgroup ([perf counts](")
                    .append(RunReport.PERF_STAT_FILE).append(")): the clock rate its threads ran at and their")
                    .append(" instructions per cycle (IPC).");
        }
        report.append("\n\n| Container | CPUs | CPU s per million messages | Voluntary switches/s | Involuntary")
                .append(" switches/s |");
        if (perf) {
            report.append(" CPU migrations/s | GHz | IPC |");
        }
        report.append("\n|---|---:|---:|---:|---:|").append(perf ? "---:|---:|---:|" : "").append('\n');
        for (Map.Entry<String, Totals> entry : totals.entrySet()) {
            Totals t = entry.getValue();
            report.append("| ").append(entry.getKey()).append(" | ");
            if (t.statsRows > 0) {
                double cores = t.cpuCores / t.statsRows;
                report.append(format("%.2f", cores)).append(" | ")
                        .append(measuredMessages > 0 ? format("%,.1f", cores * seconds / (measuredMessages / 1e6))
                                : "").append(" | ")
                        .append(format("%,.0f", t.voluntary / t.statsRows)).append(" | ")
                        .append(format("%,.0f", t.involuntary / t.statsRows)).append(" |");
            } else {
                report.append(" |  |  |  |");
            }
            if (perf) {
                if (t.perfRows > 0) {
                    report.append(' ').append(format("%,.0f", t.migrations / t.perfRows)).append(" | ")
                            .append(t.cyclesMissing || t.taskClockMillis <= 0 ? ""
                                    : format("%.2f", t.cycles / (t.taskClockMillis * 1e6))).append(" | ")
                            .append(t.cyclesMissing || t.cycles <= 0 ? "" : format("%.2f", t.instructions / t.cycles))
                            .append(" |");
                } else {
                    report.append("  |  |  |");
                }
            }
            report.append('\n');
        }
    }

    private static List<String[]> rows(Path csv) throws IOException {
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
        return String.format(Locale.ROOT, format, value);
    }
}
