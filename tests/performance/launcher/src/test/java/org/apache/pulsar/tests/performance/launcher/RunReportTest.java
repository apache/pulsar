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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Comparator;
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
            Path consumer = Files.createDirectories(run.resolve("consumer-" + application));
            Files.writeString(consumer.resolve("consumer-summary.json"), "{\"applicationIndex\": " + application
                    + ", \"uniqueMessages\": 500000, \"duplicates\": " + application + ", \"orderingViolations\": 0,"
                    + " \"invalidMessages\": 0, \"lastMeasurementMessageReceivedEpochMs\": "
                    + (START + 5000 + application * 1000) + "}");
            writeHistogram(consumer.resolve("consume-latency.hdr"), 1_200_000, 1_000);
        }
        // One topic, two subscriptions: sub-1 stalls for a second in the middle of the measurement.
        StringBuilder csv = new StringBuilder(TopicStatsSampler.HEADER).append('\n');
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
        Files.writeString(run.resolve(TopicStatsSampler.FILE_NAME), csv);
        Files.writeString(run.resolve("scenario.yaml"), "extends: base.yaml\n");
        Files.writeString(run.resolve(RunReport.RESOLVED_CONFIG), "cluster: {}\n");
        // The broker was profiled with off-CPU capture, the producer with async-profiler only.
        Path offCpu = Files.createDirectories(run.resolve("broker-profile/broker" + OffCpuFlamegraphs.OUTPUT_SUFFIX));
        Files.writeString(offCpu.resolve(OffCpuFlamegraphs.NO_IDLE_SLICE + ".json"),
                "{\"totalNanos\": \"4677199858\"}");
        Files.writeString(run.resolve("broker-profile/" + ProfileReport.FILE_NAME), "");
        Files.createDirectories(run.resolve("producer-profile/producer" + JfrFlamegraphViews.OUTPUT_SUFFIX));
        Files.writeString(run.resolve("producer-profile/" + ProfileReport.FILE_NAME), "");

        Path file = RunReport.write(run, new RunReport.Run("scenario.yaml", "run-1", "image:tag",
                json("{\"brokers\": 1, \"bookies\": 3, \"brokerEnvs\": {\"managedLedgerDefaultEnsembleSize\": \"1\","
                        + " \"managedLedgerDefaultWriteQuorum\": \"1\", \"managedLedgerDefaultAckQuorum\": \"1\"}}"),
                json("{\"gatewayCount\": 500, \"topicCount\": 1, \"applicationCount\": 2,"
                        + " \"clientsPerApplication\": 20, \"numberOfMessages\": 400000, \"warmupMessages\": 100000,"
                        + " \"warmupRounds\": 1, \"payloadBytes\": 128, \"batchingEnabled\": false, \"rate\": 0}"),
                new RunInfo(ZonedDateTime.parse("2026-09-25T06:42:59+03:00"), "perf-host", "lari", "Lari Hotari",
                        "lari@example.com", Path.of("/work/pulsar/.claude/worktrees/w1"), "lh-branch",
                        "0123456789abcdef0123456789abcdef01234567", true, "5.0.0-SNAPSHOT"),
                ZonedDateTime.parse("2026-09-25T06:46:41+03:00")),
                mapper);
        String report = Files.readString(file);

        // The run's other files are linked, so that they can be found when the run is browsed over HTTP
        assertTrue(report.contains("\nFiles: [producer/producer-summary.json](producer/producer-summary.json) · "
                + "[consumer-0/consumer-summary.json](consumer-0/consumer-summary.json) · "
                + "[consumer-1/consumer-summary.json](consumer-1/consumer-summary.json)\n"), report);
        assertTrue(report.contains("<details><summary>HDR histogram logs</summary>\n\n"
                + "- [producer/produce-latency.hdr](producer/produce-latency.hdr)\n"
                + "- [consumer-0/consume-latency.hdr](consumer-0/consume-latency.hdr)\n"
                + "- [consumer-1/consume-latency.hdr](consumer-1/consume-latency.hdr)\n\n</details>\n"), report);
        assertTrue(report.contains("The samples are in [topic-stats.csv](topic-stats.csv)."), report);
        assertTrue(report.contains("| Scenario | [scenario](scenario.yaml) |\n"), report);
        assertTrue(report.contains("| Cluster | 1 broker(s), 3 bookies, [configuration](resolved-config.yaml) |\n"),
                report);
        assertTrue(report.contains("| Started | 2026-09-25T06:42:59+03:00 |"), report);
        assertTrue(report.contains("| Host | perf-host |"), report);
        assertTrue(report.contains("| User | lari (git: Lari Hotari <lari@example.com>) |"), report);
        assertTrue(report.contains("| Project directory | `/work/pulsar/.claude/worktrees/w1` |"), report);
        assertTrue(report.contains("| Git branch | `lh-branch` |"), report);
        assertTrue(report.contains("| Git commit | `0123456789abcdef0123456789abcdef01234567`, with uncommitted"
                + " changes |"), report);
        assertTrue(report.contains("| Pulsar version | 5.0.0-SNAPSHOT |"), report);

        assertTrue(report.contains("| [broker-profile](broker-profile/profile-report.md) | 4.7 s |"), report);
        assertTrue(report.contains("| [producer-profile](producer-profile/profile-report.md) | not captured |"),
                report);
        // The profiles follow the run's settings
        assertTrue(report.indexOf("## Profiles") > report.indexOf("| Setting |"), report);
        assertTrue(report.indexOf("## Profiles") < report.indexOf("## Correctness"), report);
        assertTrue(report.contains("| Ledger replication | E=1, W=1, A=1 |"), report);
        assertTrue(report.contains("| 1 | 500,000 | 1 | 0 | 0 |"), report);
        assertTrue(report.contains("**Duplicates, ordering violations or invalid messages were received.**"), report);
        assertTrue(report.contains("| Producer throughput | 100,000 msg/s |"), report);
        // 400,000 measured messages until the slower application finished 6 s after the start
        assertTrue(report.contains("| 66,667 msg/s |"), report);
        assertTrue(report.contains("| Consumers still draining after the producers finished | 2.0 s |"), report);
        assertTrue(report.contains("| Latency (ms) | Count | Min | p50 | p90 | p99 | p99.9 | Max |\n"), report);
        // Every observation is the same value: the minimum is the lowest value of its HDR bucket, the percentiles
        // and the maximum its highest
        assertTrue(report.contains("| Publish (send to acknowledgment) | 1,000 | 899.6 | 900.1 | 900.1 | 900.1 |"
                + " 900.1 | 900.1 |"), report);
        assertTrue(report.contains("| End to end, consumer-1 | 1,000 | 1,199.1 | 1,200.1 |"), report);
        assertTrue(report.contains("Delivery after the publish is acknowledged: about 300."), report);
        assertTrue(report.contains("| Published msg/s | 100,000 | 100,000 |"), report);
        // Seconds 1–2 and 2–3 are the measurement without its first and last second. sub-1 dispatches nothing in
        // the first and catches up in the second, so the total's minimum drops to 100,000.
        assertTrue(report.contains("| Dispatched msg/s, all subscriptions | 250,000 | 100,000 |"), report);
        assertTrue(report.contains("| `sub-1` | 100,000 | 2 s | 50,000 |"), report);
        assertTrue(report.contains("| `sub-0` | 0 | 0 s | 0 |"), report);
        for (String chart : new String[] {"latency-histograms", "throughput", "backlog"}) {
            assertTrue(report.contains("](" + chart + ".svg)"), report);
            assertTrue(Files.isRegularFile(run.resolve(chart + ".svg")), chart);
            assertTrue(Files.isRegularFile(run.resolve(chart + ".png")), chart);
        }
        // Every chart says which run it shows; the latency chart's title no longer names the scenario
        for (String chart : new String[] {"latency-histograms", "throughput", "backlog"}) {
            assertTrue(Files.readString(run.resolve(chart + ".svg"))
                    .contains(">lh-branch@01234567-dirty 2026-09-25 06:42:59-06:46:41</text>"), chart);
        }
        assertTrue(Files.readString(run.resolve("latency-histograms.svg")).contains("font-weight=\"bold\">Latency<"));
        String page = Files.readString(run.resolve("run-report.html"));
        assertTrue(page.contains("<img src=\"throughput.svg\""), page);
    }

    @Test
    public void writesAShortChartFooter() {
        ZonedDateTime started = ZonedDateTime.parse("2026-09-25T23:58:30+03:00");
        RunInfo clean = new RunInfo(started, "host", "user", "", "", Path.of("/p"), "lh-branch",
                "1ebd73f2652103b30483ba6ddd7ab587a605912a", false, "5.0.0-SNAPSHOT");
        RunInfo noGit = new RunInfo(started, "host", "user", "", "", Path.of("/p"), "", "", false, "");

        // Past midnight the end is still only a time, in the start's zone
        assertEquals(RunReport.chartFooter(clean, ZonedDateTime.parse("2026-09-25T21:02:05Z")),
                "lh-branch@1ebd73f2 2026-09-25 23:58:30-00:02:05");
        assertEquals(RunReport.chartFooter(noGit, null), "2026-09-25 23:58:30");
        assertEquals(RunReport.chartFooter(null, null), "");
    }

    @Test
    public void readsPerRoundRatesFromTheCounters() throws IOException {
        Files.writeString(run.resolve(TopicStatsSampler.FILE_NAME), TopicStatsSampler.HEADER + "\n"
                + "1000,t,s,0,0,0\n2000,t,s,50,100,50\n3000,t,s,20,180,160\n");

        RunReport.Samples samples = RunReport.readSamples(run.resolve(TopicStatsSampler.FILE_NAME));

        assertEquals(samples.epochMillis(), new long[] {1000, 2000, 3000});
        assertTrue(Double.isNaN(samples.published()[0]));
        assertEquals(samples.published()[1], 100.0);
        assertEquals(samples.published()[2], 80.0);
        assertEquals(samples.dispatched().get("s")[2], 110.0);
        assertEquals(samples.backlog().get("s")[1], 50.0);
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
