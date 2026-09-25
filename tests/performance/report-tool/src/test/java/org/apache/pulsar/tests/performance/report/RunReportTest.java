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
import java.util.stream.Stream;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RunReportTest {
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
    public void reportsCorrectnessThroughputLatencyAndSampledStats() throws IOException {
        Files.createDirectories(run.resolve("producer"));
        Files.writeString(run.resolve("producer/producer-summary.json"), "{\"measurementMessages\": 400000,"
                + " \"measurementElapsedSeconds\": 4.0, \"messagesPerSecond\": 100000.0,"
                + " \"measurementStartEpochMs\": " + START + ", \"measurementEndEpochMs\": " + (START + 4000) + "}");
        writeHistogram(run.resolve("producer/produce-latency.hdr"), 900_000, 1_000);
        for (int application = 0; application < 2; application++) {
            Path consumer = Files.createDirectories(run.resolve("sub-" + application));
            Files.writeString(consumer.resolve("consumer-summary.json"), "{\"applicationIndex\": " + application
                    + ", \"uniqueMessages\": 500000, \"duplicates\": " + application + ", \"orderingViolations\": 0,"
                    + " \"invalidMessages\": 0, \"lastMeasurementMessageReceivedEpochMs\": "
                    + (START + 5000 + application * 1000) + "}");
            writeHistogram(consumer.resolve("consume-latency.hdr"), 1_200_000, 1_000);
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
        Files.writeString(run.resolve("sub-0").resolve(RunReport.CONTAINER_LOG), "log\n");
        Files.writeString(run.resolve(RunReport.RESOLVED_CONFIG), "cluster: {}\n");
        // The broker was profiled with off-CPU capture, the producer with async-profiler only.
        Path offCpu = Files.createDirectories(run.resolve("broker-profile/broker" + OffCpuFlamegraphs.OUTPUT_SUFFIX));
        Files.writeString(offCpu.resolve(OffCpuFlamegraphs.NO_IDLE_SLICE + ".json"),
                "{\"totalNanos\": \"4677199858\"}");
        Files.writeString(run.resolve("broker-profile/" + ProfileReport.FILE_NAME), "");
        Files.writeString(run.resolve("broker-profile/broker.jfr"), "");
        Files.writeString(run.resolve("broker-profile/broker.measurement.jfr"), "");
        Files.createDirectories(run.resolve("producer-profile/producer" + JfrFlamegraphViews.OUTPUT_SUFFIX));
        Files.writeString(run.resolve("producer-profile/" + ProfileReport.FILE_NAME), "");

        Path file = RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1", "image:tag",
                json("{\"brokers\": 1, \"bookies\": 3, \"brokerEnvs\": {\"managedLedgerDefaultEnsembleSize\": \"1\","
                        + " \"managedLedgerDefaultWriteQuorum\": \"1\", \"managedLedgerDefaultAckQuorum\": \"1\"}}"),
                json("{\"gatewayCount\": 500, \"topicCount\": 1, \"applicationCount\": 2,"
                        + " \"subscriptionPrefix\": \"sub-\", \"clientsPerApplication\": 20,"
                        + " \"numberOfMessages\": 400000, \"warmupMessages\": 100000,"
                        + " \"warmupRounds\": 1, \"payloadBytes\": 128, \"batchingEnabled\": false, \"rate\": 0}"),
                new RunInfo(ZonedDateTime.parse("2026-09-25T06:42:59+03:00"), "perf-host", "lari", "Lari Hotari",
                        "lari@example.com", Path.of("/work/pulsar/.claude/worktrees/w1"), "lh-branch",
                        "0123456789abcdef0123456789abcdef01234567", true, "5.0.0-SNAPSHOT"),
                ZonedDateTime.parse("2026-09-25T06:46:41+03:00"), List.of()),
                mapper);
        String report = Files.readString(file);

        // The run's other files are linked, so that they can be found when the run is browsed over HTTP
        assertThat(report).endsWith("\n<details><summary>Files</summary>\n\n"
                + "- [producer/producer-summary.json](producer/producer-summary.json)\n"
                + "- [sub-0/consumer-summary.json](sub-0/consumer-summary.json)\n"
                + "- [sub-0/container.log.txt](sub-0/container.log.txt)\n"
                + "- [sub-1/consumer-summary.json](sub-1/consumer-summary.json)\n\n</details>\n");
        assertThat(report).contains("<details><summary>HDR histogram logs and percentile distributions</summary>\n\n"
                + "- [producer/produce-latency.hdr](producer/produce-latency.hdr) ·"
                + " [producer/produce-latency.hgrm](producer/produce-latency.hgrm)\n"
                + "- [sub-0/consume-latency.hdr](sub-0/consume-latency.hdr) ·"
                + " [sub-0/consume-latency.hgrm](sub-0/consume-latency.hgrm)\n"
                + "- [sub-1/consume-latency.hdr](sub-1/consume-latency.hdr) ·"
                + " [sub-1/consume-latency.hgrm](sub-1/consume-latency.hgrm)\n\n</details>\n");
        assertThat(run.resolve("sub-1/consume-latency.hgrm")).isRegularFile();
        assertThat(report).contains("The [sampled topic stats](topic-stats.csv) are a CSV file.");
        assertThat(report).contains("| Scenario | [scenario](scenario.yaml) |\n");
        assertThat(report).contains("| Cluster | 1 broker(s), 3 bookies, [configuration](resolved-config.yaml) |\n");
        assertThat(report).contains("| Started | 2026-09-25T06:42:59+03:00 |");
        assertThat(report).contains("| Host | perf-host |");
        assertThat(report).contains("| User | lari (git: Lari Hotari <lari@example.com>) |");
        assertThat(report).contains("| Project directory | `/work/pulsar/.claude/worktrees/w1` |");
        assertThat(report).contains("| Git branch | `lh-branch` |");
        assertThat(report).contains("| Git commit | `0123456789abcdef0123456789abcdef01234567`, with uncommitted"
                + " changes |");
        assertThat(report).contains("| Pulsar version | 5.0.0-SNAPSHOT |");

        assertThat(report).contains("| [Broker](broker-profile/profile-report.md) | 4.7 s |"
                + " [complete](broker-profile/broker.jfr) ·"
                + " [measurement period](broker-profile/broker.measurement.jfr) |\n");
        assertThat(report).contains("| [Producer](producer-profile/profile-report.md) | not captured |");
        // The profiles follow the run's settings
        assertThat(report.indexOf("## Profiles")).isGreaterThan(report.indexOf("| Setting |"));
        assertThat(report.indexOf("## Profiles")).isLessThan(report.indexOf("## Correctness"));
        assertThat(report).contains("| Ledger replication | E=1, W=1, A=1 |");
        assertThat(report).contains("| sub-1 | 500,000 | 1 | 0 | 0 |");
        assertThat(report).contains("**Duplicates, ordering violations or invalid messages were received.**");
        assertThat(report).contains("| Producer throughput | 100,000 msg/s |");
        // 400,000 measured messages until the slower application finished 6 s after the start
        assertThat(report).contains("| 66,667 msg/s |");
        assertThat(report).contains("| Consumers still draining after the producers finished | 2.0 s |");
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
        // The latency charts are PNG only; throughput and backlog also have SVG
        for (String chart : new String[] {"latency-percentiles.png", "latency-timeline.png", "throughput.svg",
                "backlog.svg"}) {
            assertThat(report).contains("](" + chart + ")");
            assertThat(run.resolve(chart)).as(chart).isRegularFile();
        }
        // Every SVG chart says which run it shows
        for (String chart : new String[] {"throughput", "backlog"}) {
            assertThat(Files.readString(run.resolve(chart + ".svg"))
                    ).as(chart).contains(">lh-branch@01234567-dirty 2026-09-25 06:42:59-06:46:41</text>");
        }
        String page = Files.readString(run.resolve("run-report.html"));
        assertThat(page).contains("<img src=\"throughput.svg\"");
        assertThat(page).contains("<img src=\"latency-percentiles.png\"");
    }

    @Test
    public void reportsTheHostsThermalState() throws IOException {
        Files.createDirectories(run.resolve("producer"));
        Files.writeString(run.resolve("producer/producer-summary.json"), "{\"measurementMessages\": 400000,"
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
                json("{\"brokers\": 1, \"bookies\": 3}"), json("{\"applicationCount\": 1}"), null, null,
                List.of(new RunReport.Cooldown(RunReport.Cooldown.BEFORE_RUN, 50.0, 78.0, 49.5, 95.0, true),
                        new RunReport.Cooldown(RunReport.Cooldown.BEFORE_MEASUREMENT, 50.0, 71.0, 58.0, 600.0,
                                false))), mapper);
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
        for (String chart : new String[] {"host-temperature", "host-frequency"}) {
            assertThat(report).contains("](" + chart + ".svg)");
            assertThat(run.resolve(chart + ".svg")).isRegularFile();
        }
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
    public void writesAShortChartFooter() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        RunInfo clean = new RunInfo(started, "host", "user", "", "", Path.of("/p"), "lh-branch",
                "1ebd73f2652103b30483ba6ddd7ab587a605912a", false, "5.0.0-SNAPSHOT");
        RunInfo noGit = new RunInfo(started, "host", "user", "", "", Path.of("/p"), "", "", false, "");

        // Past midnight the end is still only a time, in the start's zone
        assertThat(RunReport.chartFooter(clean, ZonedDateTime.parse("2026-09-25T21:02:05Z")))
                .isEqualTo("lh-branch@1ebd73f2 2026-09-25 23:58:30-00:02:05");
        assertThat(RunReport.chartFooter(noGit, null)).isEqualTo("2026-09-25 23:58:30");
        assertThat(RunReport.chartFooter(null, null)).isEmpty();
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
