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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The run report's Network conditions section: the network impairments that the launcher applied, as its
 * {@value RunReport#NETWORK_SUMMARY_FILE} describes them, and the impaired containers' TCP retransmissions. A run
 * without impairments has no such file and no section.
 */
final class NetworkReport {
    static final String DOCS_URL = RunReport.README_URL + "/docs/network-conditions.md";

    private NetworkReport() {
    }

    static void append(StringBuilder report, Path runDirectory, ObjectMapper mapper, long measurementStartEpochMs) {
        Path file = runDirectory.resolve(RunReport.NETWORK_SUMMARY_FILE);
        if (!Files.isRegularFile(file)) {
            return;
        }
        JsonNode summary;
        try {
            summary = mapper.readTree(file.toFile());
        } catch (IOException e) {
            return;
        }
        report.append("\n## Network conditions\n\nThe launcher impaired the network between the containers with Linux"
                + " traffic control, as [the network conditions guide](").append(DOCS_URL).append(") describes. The"
                + " times are from the measurement's start; the packets and drops are those of each impairment's"
                + " queues, on the receivers' side for a delay, a loss or a partition, and on the senders' side for a"
                + " rate. A removed impairment's queued packets are still delivered, within its delay and jitter.\n\n"
                + "| Impairment | Between | What | Applied | Removed | Packets | Dropped |\n"
                + "|---|---|---|---:|---:|---:|---:|\n");
        List<String> problems = new ArrayList<>();
        for (JsonNode impairment : summary.path("impairments")) {
            String name = impairment.path("name").asText();
            String status = impairment.path("status").asText();
            boolean reset = "reset".equals(impairment.path("kind").asText());
            String what = impairment.path("description").asText();
            if (impairment.has("resetConnections")) {
                what += String.format(Locale.ROOT, ", %,d connection(s)", impairment.path("resetConnections").asInt());
            }
            String applied = impairment.has("appliedEpochMs")
                    ? "whole run".equals(impairment.path("anchor").asText()) ? "with the run"
                    : relative(impairment.path("appliedEpochMs").asLong(), measurementStartEpochMs)
                    : "**" + status + "**";
            String removed = impairment.has("removedEpochMs")
                    ? relative(impairment.path("removedEpochMs").asLong(), measurementStartEpochMs) : reset ? "" : "—";
            long packets = 0;
            long drops = 0;
            for (JsonNode device : impairment.path("statistics")) {
                packets += device.path("packets").asLong();
                drops += device.path("drops").asLong();
            }
            report.append("| ").append(name).append(" | ").append(impairment.path("endpoints").asText())
                    .append(" | ").append(what).append(" | ").append(applied).append(" | ").append(removed)
                    .append(" | ").append(reset ? "" : String.format(Locale.ROOT, "%,d", packets))
                    .append(" | ").append(reset ? "" : dropped(drops, packets)).append(" |\n");
            if (impairment.has("note")) {
                problems.add(name + ": " + impairment.path("note").asText());
            }
        }
        if (!problems.isEmpty()) {
            report.append('\n');
            problems.forEach(problem -> report.append("- ").append(problem).append('\n'));
        }
        List<String> offloadsOff = new ArrayList<>();
        summary.path("offloadsOff").forEach(container -> offloadsOff.add(container.asText()));
        if (!offloadsOff.isEmpty()) {
            report.append("\nThe TCP and generic segmentation offloads were off on ")
                    .append(String.join(", ", offloadsOff)).append(" for the whole run, so that netem saw the"
                            + " packets as they go on a wire.\n");
        }
        JsonNode tcp = summary.path("tcp");
        if (!tcp.isEmpty()) {
            report.append("\nThe TCP segments that the impaired containers sent during the whole run:\n\n"
                    + "| Container | Segments sent | Retransmitted |\n|---|---:|---:|\n");
            for (JsonNode container : tcp) {
                long sent = container.path("outSegments").asLong();
                long retransmitted = container.path("retransmittedSegments").asLong();
                report.append("| ").append(container.path("container").asText()).append(" | ")
                        .append(String.format(Locale.ROOT, "%,d", sent)).append(" | ")
                        .append(String.format(Locale.ROOT, "%,d", retransmitted))
                        .append(sent > 0 ? String.format(Locale.ROOT, " (%.3f %%)", retransmitted * 100.0 / sent) : "")
                        .append(" |\n");
            }
        }
    }

    /** A time relative to the measurement's start, such as "+30.0 s" or "−12.5 s". */
    static String relative(long epochMs, long measurementStartEpochMs) {
        if (measurementStartEpochMs <= 0) {
            return "";
        }
        double seconds = (epochMs - measurementStartEpochMs) / 1000.0;
        return String.format(Locale.ROOT, "%s%.1f s", seconds < 0 ? "−" : "+", Math.abs(seconds));
    }

    private static String dropped(long drops, long packets) {
        if (drops == 0) {
            return "0";
        }
        return String.format(Locale.ROOT, "%,d (%.3f %%)", drops, drops * 100.0 / (packets + drops));
    }
}
