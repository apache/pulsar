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

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RunReportTest {
    private static final String CLUSTER = "{\"brokers\": {\"replicas\": 1}, \"bookies\": {\"replicas\": 3}}";
    // A run's information with the given git state, and no git user or host details
    private static RunInfo runInfo(ZonedDateTime started, String branch, boolean detached, String commit,
                                   boolean dirty, String version) {
        return new RunInfo(started, "host", HostDetails.UNKNOWN, null, "user", "", "", Path.of("/p"), branch,
                detached, commit, dirty, version, null);
    }

    private static final long START = 1_790_000_000_000L;
    private final ObjectMapper mapper = new ObjectMapper();
    private Path run;

    @BeforeMethod
    public void createRun() throws IOException {
        run = Files.createTempDirectory("run-report-test").toRealPath();
    }

    @AfterMethod(alwaysRun = true)
    public void deleteRun() throws IOException {
        try (Stream<Path> paths = Files.walk(run)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void reportsTheCatchUpOfTheApplicationsThatJoinedLater() throws Exception {
        JsonNode workload = mapper.readTree("{\"applications\": {\"subscriptionPrefix\": \"app-\"}}");
        long start = 1_000_000;
        long end = start + 120_000;
        List<JsonNode> consumers = List.of(
                mapper.readTree("{\"applicationIndex\": 0, \"joinEpochMs\": 0}"),
                // caught up while the gateways published
                late(1, start + 20_000, start + 30_000, 1_500_000, start + 120_000),
                // caught up with the gateways' last messages, within the threshold after they finished
                late(2, start + 40_000, start + 120_400, 4_000_000, start + 140_000),
                // never caught up, with no sample before it joined
                late(3, start + 500, 0, 4_600_000, start + 150_000),
                // joined but never received a measured message
                late(4, start + 60_000, 0, 0, 0),
                // caught up in the millisecond that it joined
                late(5, start + 70_000, start + 70_000, 10, start + 80_000));
        long[] epochs = {start + 1_000, start + 19_000, start + 39_000};
        RunReport.Samples samples = new RunReport.Samples(epochs, new double[3], Map.of(),
                Map.of("app-1", new double[] {0, 570_000, 0}, "app-2", new double[] {0, 0, 1_170_000},
                        "app-3", new double[] {0, 0, 0}));
        StringBuilder report = new StringBuilder();
        RunReport.appendCatchUp(report, workload, consumers, samples, start, end);

        assertThat(report.toString())
                .contains("## Catch-up")
                // the threshold that the application wrote
                .contains("within 500 ms of its publishing")
                .contains("| `app-1` | 20.0 s | 570,000 | 10.0 s | 150,000 msg/s | 100.0 s |")
                .contains("| `app-2` | 40.0 s | 1,170,000 | 80.4 s, after the gateways finished | 49,751 msg/s"
                        + " | 100.0 s |")
                .contains("| `app-3` | 0.5 s | – | not caught up | 30,769 msg/s until its last message | 149.5 s |")
                .contains("| `app-4` | 60.0 s | – | not caught up | – | – |")
                .contains("| `app-5` | 70.0 s | – | 0.0 s | – | 10.0 s |")
                .doesNotContain("`app-0`");
        // without topic stats
        StringBuilder withoutSamples = new StringBuilder();
        RunReport.appendCatchUp(withoutSamples, workload, consumers.subList(0, 2), null, start, end);
        assertThat(withoutSamples.toString()).contains("| `app-1` | 20.0 s | – | 10.0 s |");
        // without late applications, there's no section
        StringBuilder none = new StringBuilder();
        RunReport.appendCatchUp(none, workload, consumers.subList(0, 1), samples, start, end);
        assertThat(none.toString()).isEmpty();
    }

    private JsonNode late(int index, long joined, long caughtUp, long messages, long lastReceived) throws Exception {
        return mapper.readTree("{\"applicationIndex\": " + index + ", \"joinEpochMs\": " + joined
                + ", \"caughtUpLatencyMillis\": 500, \"caughtUpEpochMs\": " + caughtUp
                + ", \"messagesWhenCaughtUp\": " + messages + ", \"uniqueMessages\": 4600000"
                + ", \"lastMeasurementMessageReceivedEpochMs\": " + lastReceived + "}");
    }

    @Test
    public void reportsCorrectnessThroughputLatencyAndSampledStats() throws IOException {
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"measurementMessages\": 400000,"
                + " \"measurementElapsedSeconds\": 4.0, \"messagesPerSecond\": 100000.0,"
                + " \"measurementStartEpochMs\": " + START + ", \"measurementEndEpochMs\": " + (START + 4000) + "}");
        writeHistogram(run.resolve("gateways/gateways-latency.hdr"), 900_000, 1_000);
        for (int application = 0; application < 2; application++) {
            Path consumer = Files.createDirectories(run.resolve("applications/sub-" + application));
            Files.writeString(consumer.resolve("application-summary.json"), "{\"applicationIndex\": " + application
                    + ", \"uniqueMessages\": 500000, \"duplicates\": " + application + ", \"orderingViolations\": 0,"
                    + " \"invalidMessages\": 0, \"lastMeasurementMessageReceivedEpochMs\": "
                    + (START + 5000 + application * 1000) + "}");
            writeHistogram(consumer.resolve("application-latency.hdr"), 1_200_000, 1_000);
        }
        // One topic, two subscriptions: sub-1 stalls for a second in the middle of the measurement.
        StringBuilder csv = new StringBuilder(RunReport.TOPIC_STATS_HEADER).append('\n');
        long[] in = {0, 100_000, 200_000, 300_000, 400_000, 500_000, 500_000, 500_000};
        long[] out0 = {0, 100_000, 200_000, 300_000, 400_000, 500_000, 500_000, 500_000};
        long[] out1 = {0, 100_000, 200_000, 200_000, 350_000, 450_000, 500_000, 500_000};
        for (int round = 0; round < in.length; round++) {
            long epoch = START - 1000 + round * 1000L;
            // sub-0 carries a warmup backlog before the measurement, which its maximum leaves out
            csv.append(epoch).append(",persistent://public/default/t-0,sub-0,")
                    .append(round == 0 ? 900_000 : in[round] - out0[round])
                    .append(',').append(in[round]).append(',').append(out0[round]).append('\n');
            csv.append(epoch).append(",persistent://public/default/t-0,sub-1,").append(in[round] - out1[round])
                    .append(',').append(in[round]).append(',').append(out1[round]).append('\n');
        }
        Files.writeString(run.resolve(RunReport.TOPIC_STATS_FILE), csv);
        Files.writeString(run.resolve("scenario.yaml"), "extends: base.yaml\n");
        Files.writeString(run.resolve("applications").resolve(RunReport.CONTAINER_LOG), "log\n");
        Files.writeString(run.resolve(RunReport.RESOLVED_CONFIG), "cluster: {}\n");
        // The broker was profiled with off-CPU capture, the gateways with async-profiler only.
        Path offCpu = Files.createDirectories(run.resolve("broker-profile/broker" + OffCpuFlamegraphs.OUTPUT_SUFFIX));
        Files.writeString(offCpu.resolve(OffCpuFlamegraphs.NO_IDLE_SLICE + ".json"),
                "{\"totalNanos\": \"4677199858\"}");
        Files.writeString(run.resolve("broker-profile/" + ProfileReport.FILE_NAME), "");
        Files.writeString(run.resolve("broker-profile/broker.jfr"), "");
        Files.writeString(run.resolve("broker-profile/broker.measurement.jfr"), "");
        Files.createDirectories(run.resolve("gateways/profile-gateways" + JfrFlamegraphViews.OUTPUT_SUFFIX));
        Files.writeString(run.resolve("gateways/" + ProfileReport.FILE_NAME), "");
        Files.writeString(run.resolve("applications/" + ProfileReport.FILE_NAME), "");

        Path file = RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1", "image:tag",
                json("{\"brokers\": {\"replicas\": 1, \"env\": {\"managedLedgerDefaultEnsembleSize\": \"1\","
                        + " \"managedLedgerDefaultWriteQuorum\": \"1\", \"managedLedgerDefaultAckQuorum\": \"1\"}},"
                        + " \"bookies\": {\"replicas\": 3}}"),
                json("{\"gateways\": {\"count\": 500, \"producer\": {\"batchingEnabled\": false}},"
                        + " \"topics\": {\"count\": 1}, \"applications\": {\"count\": 2,"
                        + " \"subscriptionPrefix\": \"sub-\", \"podsPerApplication\": 20},"
                        + " \"measurement\": {\"messages\": 400000}, \"warmup\": {\"messages\": 100000, \"rounds\": 1},"
                        + " \"payload\": {\"size\": 128}, \"rate\": 0}"),
                new RunInfo(ZonedDateTime.parse("2026-09-25T06:42:59+03:00"), "perf-host",
                        new HostDetails("Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz", 1, 8, 16, 33_256_595_456L,
                                "Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)"),
                        new DockerEngine("28.4.0", 16, 33_256_595_456L, "Pop!_OS 24.04 LTS", "7.1.5-76070105-generic",
                                "x86_64"),
                        "lari", "Lari Hotari", "lari@example.com", Path.of("/work/pulsar/.claude/worktrees/w1"),
                        "lh-branch", false, "0123456789abcdef0123456789abcdef01234567", true, "5.0.0-SNAPSHOT", null),
                ZonedDateTime.parse("2026-09-25T06:46:41+03:00"), List.of()),
                mapper);
        String report = Files.readString(file);

        // The title names the start, the branch, the commit and the scenario, in the report and in its HTML page,
        // and the first paragraph links to the guide
        assertThat(report).startsWith("# Pulsar performance test run 2026-09-25 06:42:59 lh-branch"
                + " 0123456789ab-dirty scenario\n\nRun `run-1`, image `image:tag`. The [Pulsar performance testing"
                + " README](" + RunReport.README_URL + ") describes the tests and how to read this report.\n");
        assertThat(Files.readString(run.resolve("index.html"))).contains("<title>Pulsar performance test run"
                + " 2026-09-25 06:42:59 lh-branch 0123456789ab-dirty scenario</title>");
        // The run's other files are linked, so that they can be found when the run is browsed over HTTP, and a
        // footer below a horizontal line repeats the title, the run ID and the link to the guide
        assertThat(report).endsWith("\n<details><summary>Files</summary>\n\n"
                + "- [gateways/gateways-summary.json](gateways/gateways-summary.json)\n"
                + "- [applications/container.log.txt](applications/container.log.txt)\n"
                + "- [applications/sub-0/application-summary.json](applications/sub-0/application-summary.json)\n"
                + "- [applications/sub-1/application-summary.json](applications/sub-1/application-summary.json)\n\n"
                + "</details>\n"
                + "\n------------\n\nPulsar performance test run 2026-09-25 06:42:59 lh-branch 0123456789ab-dirty"
                + " scenario · run `run-1` · [Pulsar performance testing README](" + RunReport.README_URL + ")\n");
        String footerPage = Files.readString(run.resolve("index.html"));
        assertThat(footerPage).contains("<hr />");
        assertThat(footerPage.substring(footerPage.lastIndexOf("<hr />"))).contains("<p>Pulsar performance test"
                + " run 2026-09-25 06:42:59 lh-branch 0123456789ab-dirty scenario · run <code>run-1</code> ·"
                + " <a href=\"" + RunReport.README_URL + "\">Pulsar performance testing README</a></p>");
        assertThat(report).contains("<details><summary>HDR histogram logs and percentile distributions</summary>\n\n"
                + "- [gateways/gateways-latency.hdr](gateways/gateways-latency.hdr) ·"
                + " [gateways/gateways-latency.hgrm](gateways/gateways-latency.hgrm)\n"
                + "- [applications/sub-0/application-latency.hdr](applications/sub-0/application-latency.hdr) ·"
                + " [applications/sub-0/application-latency.hgrm](applications/sub-0/application-latency.hgrm)\n"
                + "- [applications/sub-1/application-latency.hdr](applications/sub-1/application-latency.hdr) ·"
                + " [applications/sub-1/application-latency.hgrm](applications/sub-1/application-latency.hgrm)\n\n"
                + "</details>\n");
        assertThat(run.resolve("applications/sub-1/application-latency.hgrm")).isRegularFile();
        assertThat(report).contains("The [sampled topic stats](topic-stats.csv) are a CSV file.");
        assertThat(report).contains("| Scenario | [scenario](scenario.yaml) |\n");
        assertThat(report).contains("| Cluster | 1 broker(s), 3 bookies, [configuration](resolved-config.yaml) |\n");
        assertThat(report).contains("| Started | 2026-09-25T06:42:59+03:00 |");
        // The host's hardware and operating system, and the Docker engine, whose CPUs and memory are those of
        // Docker Desktop's virtual machine on macOS
        assertThat(report).contains("| Host | perf-host: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz, 8 cores,"
                + " 16 hardware threads, 31 GiB, Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic) |\n");
        assertThat(report).contains("| Docker engine | Docker 28.4.0, 16 CPUs, 31 GiB, Pop!_OS 24.04 LTS"
                + " (kernel 7.1.5-76070105-generic, x86_64) |\n");
        assertThat(report).contains("| User | lari (git: Lari Hotari <lari@example.com>) |");
        assertThat(report).contains("| Project directory | `/work/pulsar/.claude/worktrees/w1` |");
        assertThat(report).contains("| Git branch | `lh-branch` |");
        assertThat(report).contains("| Git commit | `0123456789abcdef0123456789abcdef01234567`, with uncommitted"
                + " changes |");
        assertThat(report).contains("| Pulsar version | 5.0.0-SNAPSHOT |");

        assertThat(report).contains("| Broker | [profile report](broker-profile/README.md) | 4.7 s |"
                + " [complete](broker-profile/broker.jfr) ·"
                + " [measurement period](broker-profile/broker.measurement.jfr) |\n");
        assertThat(report).contains("| Gateways | [profile report](gateways/README.md) | not captured |");
        assertThat(report).contains("| Applications | [profile report](applications/README.md) | not captured |");
        // The profiles follow the run's settings
        assertThat(report.indexOf("## Profiles")).isGreaterThan(report.indexOf("| Setting |"));
        assertThat(report.indexOf("## Profiles")).isLessThan(report.indexOf("## Correctness"));
        assertThat(report).contains("| Ledger replication | E=1, W=1, A=1 |");
        assertThat(report).contains("| sub-1 | 500,000 | 1 | 0 | 0 |");
        assertThat(report).contains("**Duplicates, ordering violations or invalid messages were received.**");
        assertThat(report).contains("| Gateways' throughput | 100,000 msg/s |");
        // 400,000 measured messages until the slower application finished 6 s after the start
        assertThat(report).contains("| 66,667 msg/s |");
        assertThat(report).contains("| Applications still receiving after the gateways finished | 2.0 s |");
        assertThat(report).contains("| Latency (ms) | Count | Min | p50 | p90 | p99 | p99.9 | Max |\n");
        // Every observation is the same value: the minimum is the lowest value of its HDR bucket, the percentiles
        // and the maximum its highest
        assertThat(report).contains("| Publish (send to acknowledgment) | 1,000 | 899.6 | 900.1 | 900.1 | 900.1 |"
                + " 900.1 | 900.1 |");
        // Each application's end-to-end latency is its own row; none is merged across applications
        assertThat(report).contains("| sub-0 (publish to consume) | 1,000 | 1,199.1 | 1,200.1 |");
        assertThat(report).contains("| sub-1 (publish to consume) | 1,000 | 1,199.1 | 1,200.1 |");
        assertThat(report).doesNotContain("End to end");
        assertThat(report).doesNotContain("Delivery after the publish");
        assertThat(report).contains("| Published msg/s | 100,000 | 100,000 |");
        // Seconds 1–2 and 2–3 are the measurement without its first and last second. sub-1 dispatches nothing in
        // the first and catches up in the second, so the total's minimum drops to 100,000.
        assertThat(report).contains("| Dispatched msg/s, all subscriptions | 250,000 | 100,000 |");
        assertThat(report).contains("| `sub-1` | 100,000 | 2 s | 50,000 |");
        assertThat(report).contains("| `sub-0` | 0 | 0 s | 0 |");
        // The report shows the SVG charts; each also has a PNG beside it
        for (String chart : new String[] {"latency-percentiles.svg", "latency-timeline.svg", "throughput.svg",
                "backlog.svg"}) {
            assertThat(report).contains("](" + chart + ")");
            assertThat(run.resolve(chart)).as(chart).isRegularFile();
            assertThat(run.resolve(chart.replace(".svg", ".png"))).as(chart).isRegularFile();
        }
        // Every SVG chart says which run it shows
        for (String chart : new String[] {"throughput", "backlog"}) {
            assertThat(Files.readString(run.resolve(chart + ".svg"))
                    ).as(chart).contains(">lh-branch@01234567-dirty 2026-09-25 06:42:59-06:46:41</text>");
        }
        String page = Files.readString(run.resolve("index.html"));
        assertThat(page).contains("<img src=\"throughput.svg\"");
        assertThat(page).contains("<img src=\"latency-percentiles.svg\"");
    }

    @Test
    public void reportsTheHostsThermalState() throws IOException {
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"measurementMessages\": 400000,"
                + " \"measurementElapsedSeconds\": 4.0, \"messagesPerSecond\": 100000.0,"
                + " \"measurementStartEpochMs\": " + START + ", \"measurementEndEpochMs\": " + (START + 4000) + "}");
        // A sample before the measurement, four within it and one after; the counters grow within it, the
        // host has no fan sensor and the second sample has no frequency
        Files.writeString(run.resolve(RunReport.HOST_STATS_FILE), RunReport.HOST_STATS_HEADER + "\n"
                + (START - 1000) + ",52.0,54.0,3500,3000,100,1000,\n"
                + START + ",70.0,73.0,,,100,1000,\n"
                + (START + 1000) + ",80.0,84.0,3100,2900,105,1000,\n"
                + (START + 2000) + ",90.0,95.0,2800,2400,112,1003,\n"
                + (START + 4000) + ",82.0,85.0,3000,2700,112,1003,\n"
                + (START + 6000) + ",60.0,61.0,3600,3200,150,1010,\n");

        Path file = RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1", "image:tag",
                json(CLUSTER), json("{\"applications\": {\"count\": 1}}"), null, null,
                List.of(new RunReport.Cooldown(RunReport.Cooldown.BEFORE_RUN, 50.0, 78.0, 49.5, 95.0, true,
                                START - 200_000, START - 105_000),
                        new RunReport.Cooldown(RunReport.Cooldown.BEFORE_MEASUREMENT, 50.0, 71.0, 58.0, 600.0,
                                false, START - 600_500, START - 500))), mapper);
        String report = Files.readString(file);

        assertThat(report).contains("| Host CPU | 52 °C at the start, at most 90 °C and 2,967 MHz on average during"
                + " the measurement, **thermal throttling** |");
        assertThat(report).contains("Before the run, the launcher waited 95 s for the CPU package to cool down from"
                + " 78 °C to 50 °C.");
        assertThat(report).contains("After the warmup, before the measurement, the launcher waited 600 s for the CPU"
                + " package to cool down to 50 °C, and went on at 58 °C when the wait timed out.");
        // Growth from the last sample before the measurement to the last one within it
        assertThat(report).contains("**The CPU throttled during the measurement:** 12 core and 3 package thermal"
                + " throttle events");
        assertThat(report).contains("| CPU package temperature (°C) | 52 | 81 | 70 | 90 |");
        assertThat(report).contains("| Hottest core temperature (°C) | 54 | 84 | 73 | 95 |");
        assertThat(report).contains("| Lowest core frequency (MHz) | 3,000 | 2,667 | 2,400 | 2,900 |");
        // No fan sensor, no fan row
        assertThat(report).doesNotContain("Fastest fan");
        assertThat(report).contains("The [sampled host stats](host-stats.csv) are a CSV file.");
        // The charts are collapsed below the table
        assertThat(report).contains("<details><summary>CPU temperature and frequency over time</summary>\n\n"
                + "![CPU temperature over time](host-temperature.svg)\n\n"
                + "![CPU frequency over time](host-frequency.svg)\n\n</details>\n");
        for (String chart : new String[] {"host-temperature", "host-frequency"}) {
            assertThat(run.resolve(chart + ".svg")).isRegularFile();
        }
    }

    @Test
    public void rendersNoHostChartsWithoutTemperaturesOrFrequencies() throws IOException {
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"measurementStartEpochMs\": " + START
                + ", \"measurementEndEpochMs\": " + (START + 2000) + "}");
        // Only throttle counters and a fan
        Files.writeString(run.resolve(RunReport.HOST_STATS_FILE), RunReport.HOST_STATS_HEADER + "\n"
                + START + ",,,,,5,5,4000\n" + (START + 1000) + ",,,,,5,5,4100\n" + (START + 2000) + ",,,,,5,5,4200\n");

        String report = Files.readString(RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1",
                "image:tag", json(CLUSTER), json("{\"applications\": {\"count\": 1}}"), null,
                null, List.of()), mapper));

        assertThat(report).contains("No thermal throttling during the measurement.");
        assertThat(report).doesNotContain("over time</summary>");
        assertThat(run.resolve("host-temperature.svg")).doesNotExist();
        assertThat(run.resolve("host-frequency.svg")).doesNotExist();
    }

    @Test
    public void reportsAHostThatDidNotThrottle() throws IOException {
        Files.writeString(run.resolve(RunReport.HOST_STATS_FILE), RunReport.HOST_STATS_HEADER + "\n"
                + START + ",60.0,,3100,3100,5,5,4000\n" + (START + 1000) + ",62.0,,3100,3100,5,5,4100\n");

        RunReport.HostSamples samples = RunReport.readHostSamples(run.resolve(RunReport.HOST_STATS_FILE));
        RunReport.HostSummary summary = RunReport.summarize(samples, START, START + 1000);

        assertThat(summary.throttled()).isFalse();
        assertThat(summary.fanRpm().max()).isEqualTo(4100.0);
        assertThat(summary.coreCelsius().mean()).isNaN();
        assertThat(RunReport.hostSummaryLine(summary)).isEqualTo("60 °C at the start, at most 62 °C and 3,100 MHz"
                + " on average during the measurement, no thermal throttling");
    }

    @Test
    public void reportsThrottlingAsUnknownWithoutThrottleCounters() throws IOException {
        // Temperatures and frequencies, but no throttle counters, as on hosts without the thermal_throttle files
        Files.writeString(run.resolve(RunReport.HOST_STATS_FILE), RunReport.HOST_STATS_HEADER + "\n"
                + START + ",60.0,,3100,3100,,,\n" + (START + 1000) + ",62.0,,3100,3100,,,\n");

        RunReport.HostSamples samples = RunReport.readHostSamples(run.resolve(RunReport.HOST_STATS_FILE));
        RunReport.HostSummary summary = RunReport.summarize(samples, START, START + 1000);

        assertThat(summary.throttled()).isFalse();
        assertThat(summary.throttlingKnown()).isFalse();
        assertThat(RunReport.hostSummaryLine(summary)).isEqualTo("60 °C at the start, at most 62 °C and 3,100 MHz"
                + " on average during the measurement, thermal throttling unknown");
    }

    @Test
    public void cutsALongCoolDownBeforeTheMeasurementOutOfTheCharts() {
        // Warmup samples at -103 and -102 s, a cool-down from -101.5 to -1.5 s sampled every second, then the
        // measurement
        long[] epochs = new long[106];
        for (int round = 0; round < epochs.length; round++) {
            epochs[round] = START - 103_000 + round * 1000L;
        }
        RunReport.Cooldown coolDown = new RunReport.Cooldown(RunReport.Cooldown.BEFORE_MEASUREMENT, 50, 70, 50, 100,
                true, START - 101_500, START - 1_500);

        RunReport.ChartTimeline timeline = RunReport.chartTimeline(epochs, START, List.of(coolDown));

        // The warmup samples moved up by the 100 s of the cool-down, a break at its end, then the samples from -1 s
        assertThat(timeline.seconds()).containsExactly(-3.0, -2.0, -1.5, -1.0, 0.0, 1.0, 2.0);
        assertThat(timeline.rounds()).containsExactly(0, 1, -1, 102, 103, 104, 105);
        // Left of the cut the axis shows the real time
        assertThat(timeline.cut().gapSeconds()).isEqualTo(100.0);
        assertThat(timeline.cut().realSeconds(-3.0)).isEqualTo(-103.0);
        assertThat(timeline.cut().realSeconds(1.0)).isEqualTo(1.0);
        assertThat(timeline.select(new double[106])[2]).isNaN();
        // A short cool-down, or none, leaves the time axis as it is
        RunReport.Cooldown shortCoolDown = new RunReport.Cooldown(RunReport.Cooldown.BEFORE_MEASUREMENT, 50, 55, 50,
                5, true, START - 6_500, START - 1_500);
        assertThat(RunReport.chartTimeline(epochs, START, List.of(shortCoolDown)).cut()).isNull();
        assertThat(RunReport.chartTimeline(epochs, START, List.of()).seconds()).hasSize(106);
    }

    @Test
    public void writesAShortChartFooter() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        RunInfo clean = runInfo(started, "lh-branch", false, "1ebd73f2652103b30483ba6ddd7ab587a605912a", false,
                "5.0.0-SNAPSHOT");
        RunInfo noGit = runInfo(started, "", false, "", false, "");

        // Past midnight the end is still only a time, in the start's zone
        assertThat(RunReport.chartFooter(clean, ZonedDateTime.parse("2026-09-25T21:02:05Z")))
                .isEqualTo("lh-branch@1ebd73f2 2026-09-25 23:58:30-00:02:05");
        assertThat(RunReport.chartFooter(noGit, null)).isEqualTo("2026-09-25 23:58:30");
        assertThat(RunReport.chartFooter(null, null)).isEmpty();
    }

    @Test
    public void leadsWithTheClustersReleaseAndNamesTheClientsRevision() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        String commit = "1ebd73f2652103b30483ba6ddd7ab587a605912a";
        RunInfo info = runInfo(started, "lh-branch", false, commit, false, "5.0.0-SNAPSHOT")
                .withCluster(new RunInfo.Cluster("apachepulsar/pulsar:latest", "4.1.1"));
        // A cluster whose brokers didn't report their version is named by its image
        RunInfo unknownVersion = runInfo(started, "", false, "", false, "")
                .withCluster(new RunInfo.Cluster("apachepulsar/pulsar:4.0.13", ""));

        assertThat(RunReport.title("iot.yaml", info)).isEqualTo("Pulsar 4.1.1 performance test run"
                + " 2026-09-25 23:58:30 iot, clients lh-branch 1ebd73f26521");
        assertThat(RunReport.title("iot.yaml", unknownVersion)).isEqualTo("Pulsar apachepulsar/pulsar:4.0.13"
                + " performance test run 2026-09-25 23:58:30 iot");
        assertThat(RunReport.chartFooter(info, null))
                .isEqualTo("Pulsar 4.1.1, clients lh-branch@1ebd73f2 2026-09-25 23:58:30");
        assertThat(RunReport.chartFooter(unknownVersion, null))
                .isEqualTo("Pulsar apachepulsar/pulsar:4.0.13 2026-09-25 23:58:30");
        assertThat(RunReport.clusterNote(info)).isEqualTo("ZooKeeper, the bookies and the brokers ran Pulsar"
                + " `4.1.1`, from the image `apachepulsar/pulsar:latest`. The gateways and the applications ran"
                + " `lh-branch 1ebd73f26521`, and with it its Pulsar client, so the results also depend on the revision"
                + " when the clients are the bottleneck.\n\n");
        assertThat(RunReport.clusterNote(runInfo(started, "lh-branch", false, commit, false, ""))).isEmpty();
        StringBuilder rows = new StringBuilder();
        RunReport.appendRunInfo(rows, info);
        assertThat(rows.toString()).contains("| Git commit | `" + commit + "` |\n")
                .contains("| Pulsar version of the clients | 5.0.0-SNAPSHOT |\n")
                .contains("| Cluster's Pulsar image | `apachepulsar/pulsar:latest` |\n")
                .contains("| Cluster's Pulsar version | 4.1.1 |\n");
    }

    @Test
    public void saysWhenTheHeadWasDetached() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        String commit = "1ebd73f2652103b30483ba6ddd7ab587a605912a";
        StringBuilder onBranch = new StringBuilder();
        RunReport.appendRunInfo(onBranch, runInfo(started, "lh-branch", false, commit, false, "5.0.0-SNAPSHOT"));
        StringBuilder atBranchCommit = new StringBuilder();
        RunReport.appendRunInfo(atBranchCommit, runInfo(started, "lh-branch", true, commit, false, "5.0.0-SNAPSHOT"));
        StringBuilder atNoBranch = new StringBuilder();
        RunReport.appendRunInfo(atNoBranch,
                runInfo(started, RunInfo.DETACHED_HEAD, true, commit, false, "5.0.0-SNAPSHOT"));

        assertThat(onBranch.toString()).contains("| Git branch | `lh-branch` |\n");
        // Without host details, the host is only named, and an unknown Docker engine has no row
        assertThat(onBranch.toString()).contains("| Host | host |\n").doesNotContain("Docker engine");
        assertThat(atBranchCommit.toString())
                .contains("| Git branch | `lh-branch`, a detached HEAD at a commit of this branch |\n");
        // A commit that no branch contains is named as git names it
        assertThat(atNoBranch.toString()).contains("| Git branch | `HEAD` |\n");
    }

    @Test
    public void linksAProfilesReportsDirectly() throws IOException {
        Path profile = Files.createDirectories(run.resolve("broker-profile"));
        Path offCpu = Files.createDirectories(profile.resolve("recording-offcpu"));
        Files.writeString(offCpu.resolve("jonoffcpu-summary.md"), "# Digest");
        Files.writeString(offCpu.resolve("offcpu-no-idle.html"), "");
        Path flameGraphs = Files.createDirectories(profile.resolve("recording-flamegraphs"));
        Files.writeString(flameGraphs.resolve("cpu.html"), "");

        assertThat(RunReport.reportLinks("broker-profile", profile)).isEqualTo(
                // The jonoffcpu report comes first, in bold, as the place to start
                "**[jonoffcpu report (off-CPU summary)](broker-profile/recording-offcpu/jonoffcpu-summary.md)**"
                        + " · [profile report](broker-profile/README.md)"
                        + " · [blocked time flame graph](broker-profile/recording-offcpu/offcpu-no-idle.html)"
                        // A profile without allocation sampling has no allocation flame graph
                        + " · [CPU flame graph](broker-profile/recording-flamegraphs/cpu.html)");
    }

    @Test
    public void linksTheRunsMetricsInGrafana() throws IOException {
        Files.writeString(run.resolve(RunReport.METRICS_FILE), """
                {"cluster": "2026-09-27/master/iot-telemetry/09-27-12-00-00", "intervalSeconds": 5,
                 "grafanaDashboard": "http://127.0.0.1:3000/d/EetmjdhnA/pulsar-messaging?var-cluster=x"}
                """);
        StringBuilder report = new StringBuilder();

        RunReport.appendMetrics(report, run, mapper);

        assertThat(report.toString())
                .contains("## Metrics")
                .contains("every 5 s, with the cluster label `2026-09-27/master/iot-telemetry/09-27-12-00-00`")
                .contains("[the run on the Pulsar / Messaging dashboard]"
                        + "(http://127.0.0.1:3000/d/EetmjdhnA/pulsar-messaging?var-cluster=x)");
        // A run without metrics has no section
        StringBuilder none = new StringBuilder();
        RunReport.appendMetrics(none, Files.createTempDirectory("no-metrics"), mapper);
        assertThat(none.toString()).isEmpty();
    }

    @Test
    public void showsTheRenderedPanelsThatExistAndSurvivesAnUnreadableMetricsFile() throws IOException {
        Files.createDirectories(run.resolve("grafana-panels"));
        Files.write(run.resolve("grafana-panels/publish-rate.png"), new byte[] {1});
        Files.writeString(run.resolve(RunReport.METRICS_FILE), """
                {"cluster": "run", "intervalSeconds": 5, "grafanaDashboard": "http://grafana/",
                 "dashboards": [{"title": "Pulsar / Messaging", "url": "http://grafana/d/messaging"}],
                 "panels": [{"title": "Publish rate", "file": "grafana-panels/publish-rate.png",
                             "url": "http://grafana/d/messaging?viewPanel=16", "dashboard": "Pulsar / Messaging",
                             "dashboardUrl": "http://grafana/d/messaging"},
                            {"title": "Backlog", "file": "grafana-panels/backlog.png"}]}
                """);
        StringBuilder report = new StringBuilder();

        RunReport.appendMetrics(report, run, mapper);

        assertThat(report.toString())
                .contains("over the run: [Pulsar / Messaging](http://grafana/d/messaging).")
                .contains("[![Publish rate](grafana-panels/publish-rate.png)](http://grafana/d/messaging?viewPanel=16)")
                // Below the panel, links to it and to the dashboard that it is on
                .contains("\n[Publish rate](http://grafana/d/messaging?viewPanel=16)"
                        + " · [Pulsar / Messaging dashboard](http://grafana/d/messaging)\n")
                // Its image is missing
                .doesNotContain("Backlog");
        Files.writeString(run.resolve(RunReport.METRICS_FILE), "{not json");
        StringBuilder unreadable = new StringBuilder();
        RunReport.appendMetrics(unreadable, run, mapper);
        assertThat(unreadable.toString()).isEmpty();
    }

    @Test
    public void listsEachHeapDumpOnceAsItsLastRowDescribesIt() throws IOException {
        Path dumps = Files.createDirectories(run.resolve("heap-dumps/broker"));
        Files.write(dumps.resolve("broker-0-peak.hprof"), new byte[3 << 20]);
        Files.write(dumps.resolve("broker-0-at-30s.hprof"), new byte[1 << 20]);
        // A peak dump replaced by a later one under the same name, and a dump whose file was removed since
        Files.writeString(run.resolve("heap-dumps/heap-dumps.csv"), """
                epochMillis,target,trigger,file,usedBytes,maxBytes,dumpMillis
                1000,broker-0,peak,broker/broker-0-peak.hprof,2147483648,4294967296,0
                2000,broker-0,at-30s,broker/broker-0-at-30s.hprof,1073741824,4294967296,850
                3000,broker-0,peak,broker/broker-0-peak.hprof,3221225472,4294967296,0
                4000,broker-0,end,broker/broker-0-end.hprof,1073741824,4294967296,900
                """);
        StringBuilder report = new StringBuilder();

        RunReport.appendHeapDumps(report, run);

        assertThat(report.toString()).contains("## Heap dumps\n")
                .contains("| [broker-0, at-30s](heap-dumps/broker/broker-0-at-30s.hprof) | 1,024 MB of 4,096 MB |"
                        + " 1 MB |\n| [broker-0, peak](heap-dumps/broker/broker-0-peak.hprof) | 3,072 MB of 4,096 MB |"
                        + " 3 MB |\n")
                .doesNotContain("end.hprof");
        // Without dumps, no section
        StringBuilder empty = new StringBuilder();
        RunReport.appendHeapDumps(empty, Files.createDirectories(run.resolve("other")));
        assertThat(empty.toString()).isEmpty();
    }

    @Test
    public void namesTheStartTheBranchTheCommitAndTheScenarioInTheTitle() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        String commit = "1ebd73f2652103b30483ba6ddd7ab587a605912a";

        assertThat(RunReport.title("iot.yaml", runInfo(started, "lh-branch", true, commit, false, "")))
                .isEqualTo("Pulsar performance test run 2026-09-25 23:58:30 lh-branch 1ebd73f26521 iot");
        // A commit that no branch contains, with uncommitted changes
        assertThat(RunReport.title("iot.yaml", runInfo(started, RunInfo.DETACHED_HEAD, true, commit, true, "")))
                .isEqualTo("Pulsar performance test run 2026-09-25 23:58:30 1ebd73f26521-dirty iot");
        assertThat(RunReport.title("iot.yml", runInfo(started, "", false, "", false, "")))
                .isEqualTo("Pulsar performance test run 2026-09-25 23:58:30 iot");
        assertThat(RunReport.title("iot.yaml", null)).isEqualTo("Pulsar performance test run iot");
    }

    @Test
    public void readsPerRoundRatesFromTheCounters() throws IOException {
        Files.writeString(run.resolve(RunReport.TOPIC_STATS_FILE), RunReport.TOPIC_STATS_HEADER + "\n"
                + "1000,t,s,0,0,0\n2000,t,s,50,100,50\n3000,t,s,20,180,160\n");

        RunReport.Samples samples = RunReport.readSamples(run.resolve(RunReport.TOPIC_STATS_FILE));

        assertThat(samples.epochMillis()).isEqualTo(new long[] {1000, 2000, 3000});
        assertThat(samples.published()[0]).isNaN();
        assertThat(samples.published()[1]).isEqualTo(100.0);
        assertThat(samples.published()[2]).isEqualTo(80.0);
        assertThat(samples.dispatched().get("s")[2]).isEqualTo(110.0);
        assertThat(samples.backlog().get("s")[1]).isEqualTo(50.0);
    }

    @Test
    public void sumsEachSubscriptionOverItsTopics() throws IOException {
        // Two topics consumed by the same two subscriptions; t-1 has no sample in the second round
        Files.writeString(run.resolve(RunReport.TOPIC_STATS_FILE), RunReport.TOPIC_STATS_HEADER + "\n"
                + "1000,t-0,a,10,0,0\n1000,t-0,b,1,0,0\n1000,t-1,a,20,0,0\n1000,t-1,b,2,0,0\n"
                + "2000,t-0,a,30,100,70\n2000,t-0,b,3,100,97\n"
                + "3000,t-0,a,40,200,160\n3000,t-0,b,4,200,196\n3000,t-1,a,50,300,250\n3000,t-1,b,5,300,295\n"
                + "4000,t-0,a,0,300,300\n4000,t-0,b,0,300,300\n4000,t-1,a,0,400,400\n4000,t-1,b,0,400,400\n");

        RunReport.Samples samples = RunReport.readSamples(run.resolve(RunReport.TOPIC_STATS_FILE));

        assertThat(samples.backlog()).containsOnlyKeys("a", "b");
        assertThat(samples.dispatched()).containsOnlyKeys("a", "b");
        assertThat(samples.backlog().get("a")).containsExactly(30.0, 30.0, 90.0, 0.0);
        assertThat(samples.backlog().get("b")).containsExactly(3.0, 3.0, 9.0, 0.0);
        // Round 2 has t-0 only, and round 3 has t-1 without a previous sample: the sums would be partial
        assertThat(samples.dispatched().get("a")[1]).isEqualTo(70.0);
        assertThat(samples.dispatched().get("a")[2]).isNaN();
        assertThat(samples.dispatched().get("a")[3]).isEqualTo(140.0 + 150.0);
    }

    @Test
    public void reportsTheGatewaysMessageCounts() throws IOException {
        // A scenario limited by duration and rate leaves the configured counts at 0
        Files.createDirectories(run.resolve("gateways"));
        Files.writeString(run.resolve("gateways/gateways-summary.json"), "{\"measurementMessages\": 120000,"
                + " \"warmupMessages\": 20000, \"measurementStartEpochMs\": " + START
                + ", \"measurementEndEpochMs\": " + (START + 120_000) + "}");

        Path file = RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1", "image:tag",
                json(CLUSTER),
                json("{\"applications\": {\"count\": 1}, \"measurement\": {\"messages\": 0},"
                        + " \"warmup\": {\"messages\": 0},"
                        + " \"rate\": 1000}"), null, null, List.of()), mapper);

        assertThat(Files.readString(file)).contains("| Messages | 120,000 measured, 20,000 warmup |");
    }

    private JsonNode json(String text) throws IOException {
        return mapper.readTree(text);
    }

    private static void writeHistogram(Path file, long micros, int count) throws IOException {
        Histogram histogram = new Histogram(3);
        histogram.recordValueWithCount(micros, count);
        histogram.setStartTimeStamp(START);
        histogram.setEndTimeStamp(START + 4000);
        try (PrintStream out = new PrintStream(Files.newOutputStream(file))) {
            HistogramLogWriter writer = new HistogramLogWriter(out);
            writer.outputLogFormatVersion();
            writer.outputLegend();
            writer.outputIntervalHistogram(histogram);
        }
    }
}
