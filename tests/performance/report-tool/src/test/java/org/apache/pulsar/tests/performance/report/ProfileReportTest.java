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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProfileReportTest {
    private Path directory;

    @BeforeMethod
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("profile-report-test");
    }

    @AfterMethod(alwaysRun = true)
    public void deleteDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void linksExistingOutputsWithTheirTotals() throws IOException {
        Path offCpu = Files.createDirectories(directory.resolve("broker" + OffCpuFlamegraphs.OUTPUT_SUFFIX));
        Files.writeString(offCpu.resolve(OffCpuFlamegraphs.SUMMARY_FILE), "# digest\n");
        Files.writeString(offCpu.resolve(OffCpuFlamegraphs.IDLE_WAITS_FILE), "^idle$\n");
        writeSlice(offCpu, OffCpuFlamegraphs.ALL_SLICE, "{\"intervals\": \"1000\", \"totalNanos\": \"3843000000000\"}");
        writeSlice(offCpu, OffCpuFlamegraphs.NO_IDLE_SLICE, "{\"intervals\": \"207\", \"totalNanos\": \"4677199858\","
                + " \"filtered\": {\"intervals\": \"793\", \"totalNanos\": \"3838609192364\"}}");
        writeSlice(offCpu, OffCpuFlamegraphs.NO_IDLE_APP_ROOT_SLICE, "{\"intervals\": \"180\", \"totalNanos\":"
                + " \"2300000000\", \"filtered\": {\"intervals\": \"793\", \"totalNanos\": \"3838609192364\"},"
                + " \"rootAtUnmatchedHidden\": {\"intervals\": \"27\", \"totalNanos\": \"2377199858\"}}");
        Path views = Files.createDirectories(directory.resolve("broker" + JfrFlamegraphViews.OUTPUT_SUFFIX));
        for (String file : List.of("cpu.html", "cpu-threads.html", "cpu-heatmap.html", "cpu.collapsed",
                "alloc.html")) {
            Files.writeString(views.resolve(file), "");
        }
        // Retention removed the complete recording; the measurement recording and the capture stream remain
        Files.writeString(directory.resolve("broker.measurement.jfr"), "");
        Files.writeString(directory.resolve("broker.jonoffcpu-capture.pb"), "");

        Path file = ProfileReport.write(directory, List.of(directory.resolve("broker.jfr")),
                new ProfileReport.Run("scenario.yaml", "run-1", Instant.parse("2026-09-25T00:00:00Z"),
                        Instant.parse("2026-09-25T00:00:40Z"), 102328.7),
                new ObjectMapper(), directory);
        String report = Files.readString(file);

        assertTrue(report.contains("Scenario `scenario.yaml`, run `run-1`."), report);
        assertTrue(report.contains("(40.0 s); producer throughput 102,329 msg/s."), report);
        // The files are a table with descriptive link texts; the recording's generated name is not shown
        assertTrue(report.contains("| File | Contents |\n|---|---|\n"
                + "| [Digest (off-CPU summary)](broker-offcpu/jonoffcpu-summary.md) | Start here:"), report);
        assertTrue(report.contains("| [JFR recording for the measurement period](broker.measurement.jfr) |"), report);
        assertTrue(report.contains("| [Off-CPU capture stream](broker.jonoffcpu-capture.pb) |"), report);
        assertTrue(report.contains("| [Idle-wait patterns](broker-offcpu/offcpu-idle-waits.txt) |"), report);
        assertFalse(report.contains("(broker.jfr)"), report);
        assertFalse(report.contains("## broker"), report);
        assertTrue(report.contains("| [All off-CPU time](broker-offcpu/offcpu.html) | 3,843.0 | 1,000 |  |"),
                report);
        assertTrue(report.contains(
                "| [Without idle waits](broker-offcpu/offcpu-no-idle.html) | 4.7 | 207 | 3,838.6 |  |"), report);
        // Stacks without an application frame are left out of the app-root flame graphs and counted apart
        assertTrue(report.contains("| [Without idle waits, from the application's first frame]"
                + "(broker-offcpu/offcpu-no-idle-app-root.html) | 2.3 | 180 | 3,838.6 | 2.4 |"), report);
        assertFalse(report.contains("[no application frame]"), report);
        // Slices that were not rendered are left out rather than linked.
        assertFalse(report.contains(OffCpuFlamegraphs.APP_ROOT_SLICE + ".html"), report);
        // Only the rendered views are named; this recording has no lock or wall-clock view.
        assertTrue(report.contains("### CPU and allocation views\n"), report);
        assertTrue(report.contains("| cpu | [flame graph](broker-flamegraphs/cpu.html) | "
                + "[by thread](broker-flamegraphs/cpu-threads.html) | "
                + "[heatmap](broker-flamegraphs/cpu-heatmap.html) | "
                + "[collapsed](broker-flamegraphs/cpu.collapsed) |"), report);
        assertTrue(report.contains("| alloc | [flame graph](broker-flamegraphs/alloc.html) |  |  |  |"), report);

        // The HTML pages link the digest's page rather than its Markdown.
        String page = Files.readString(directory.resolve("profile-report.html"));
        assertTrue(page.contains("<a href=\"broker-offcpu/jonoffcpu-summary.html\">"), page);
        assertTrue(Files.isRegularFile(offCpu.resolve("jonoffcpu-summary.html")));
    }

    @Test
    public void reportsTheRunWithoutProfilerOutputs() throws IOException {
        Path file = ProfileReport.write(directory, List.of(directory.resolve("producer-1.jfr"),
                        directory.resolve("producer-2.jfr")),
                new ProfileReport.Run("scenario.yaml", "run-2", Instant.parse("2026-09-25T00:00:00Z"),
                        Instant.parse("2026-09-25T00:00:01Z"), 0),
                new ObjectMapper(), directory);
        String report = Files.readString(file);

        // Several recordings in one directory are numbered rather than named
        assertTrue(report.contains("## Recording 1\n"), report);
        assertTrue(report.contains("## Recording 2\n"), report);
        assertFalse(report.contains("producer throughput"), report);
        assertFalse(report.contains("### "), report);
        assertFalse(report.contains("| File |"), report);
    }

    @Test
    public void reportsViewsWithoutOffCpuCapture() throws IOException {
        Path views = Files.createDirectories(directory.resolve("broker" + JfrFlamegraphViews.OUTPUT_SUFFIX));
        for (String file : List.of("cpu.html", "alloc.html", "lock.html")) {
            Files.writeString(views.resolve(file), "");
        }

        Path file = ProfileReport.write(directory, List.of(directory.resolve("broker.jfr")),
                new ProfileReport.Run("scenario.yaml", "run-3", Instant.parse("2026-09-25T00:00:00Z"),
                        Instant.parse("2026-09-25T00:00:10Z"), 0),
                new ObjectMapper(), directory);
        String report = Files.readString(file);

        assertFalse(report.contains("Off-CPU"), report);
        assertTrue(report.contains("### CPU, allocation and lock views\n"), report);
        assertTrue(report.contains("| lock | [flame graph](broker-flamegraphs/lock.html) |"), report);
    }

    @Test
    public void namesViewsInProse() {
        assertEquals(ProfileReport.inProse(List.of("CPU")), "CPU");
        assertEquals(ProfileReport.inProse(List.of("CPU", "allocation")), "CPU and allocation");
        assertEquals(ProfileReport.inProse(List.of("CPU", "wall-clock", "allocation", "lock")),
                "CPU, wall-clock, allocation and lock");
    }

    private static void writeSlice(Path offCpu, String slice, String json) throws IOException {
        Files.writeString(offCpu.resolve(slice + ".json"), json);
        Files.writeString(offCpu.resolve(slice + ".html"), "");
    }
}
