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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NetworkReportTest {
    private static final long START = 1_790_000_000_000L;
    private final ObjectMapper mapper = new ObjectMapper();
    private Path run;

    @BeforeMethod
    public void createRun() throws IOException {
        run = Files.createTempDirectory("network-report-test");
    }

    @AfterMethod(alwaysRun = true)
    public void deleteRun() throws IOException {
        Files.deleteIfExists(run.resolve(RunReport.NETWORK_SUMMARY_FILE));
        Files.deleteIfExists(run);
    }

    @Test
    public void listsTheImpairmentsWithTheirTimesAndCounters() throws IOException {
        Files.writeString(run.resolve(RunReport.NETWORK_SUMMARY_FILE), """
                {
                  "offloads": "auto",
                  "offloadsOff": ["gateways"],
                  "impairments": [
                    {"name": "wan", "kind": "condition", "description": "20 ms ± 5 ms delay, 1 %% loss",
                     "endpoints": "gateways ↔ brokers", "anchor": "whole run", "status": "active",
                     "appliedEpochMs": %d,
                     "statistics": {"veth1": {"packets": 9900, "drops": 100}, "eth0": {"packets": 5000, "drops": 0}}},
                    {"name": "cut", "kind": "partition", "description": "partition",
                     "endpoints": "clients ↔ broker-1", "anchor": "measurement", "status": "removed",
                     "appliedEpochMs": %d, "removedEpochMs": %d,
                     "statistics": {"veth2": {"packets": 0, "drops": 400}}},
                    {"name": "reset", "kind": "reset", "description": "connection reset",
                     "endpoints": "gateways → brokers", "anchor": "measurement", "status": "removed",
                     "appliedEpochMs": %d, "resetConnections": 0, "note": "matched no TCP connection"},
                    {"name": "late", "kind": "partition", "description": "partition",
                     "endpoints": "gateways ↔ brokers", "anchor": "measurement", "status": "missed"}
                  ],
                  "tcp": [{"container": "gateways", "outSegments": 10000, "retransmittedSegments": 120}]
                }
                """.formatted(START - 20_000, START + 30_000, START + 50_250, START + 90_000));
        StringBuilder report = new StringBuilder();

        NetworkReport.append(report, run, mapper, START);

        assertThat(report.toString())
                .contains("## Network conditions")
                .contains("| wan | gateways ↔ brokers | 20 ms ± 5 ms delay, 1 % loss | with the run | — | 14,900 |"
                        + " 100 (0.667 %) |")
                .contains("| cut | clients ↔ broker-1 | partition | +30.0 s | +50.3 s | 0 | 400 (100.000 %) |")
                .contains("| reset | gateways → brokers | connection reset, 0 connection(s) | +90.0 s |  |  |  |")
                .contains("| late | gateways ↔ brokers | partition | **missed** | — | 0 | 0 |")
                .contains("- reset: matched no TCP connection")
                .contains("offloads were off on gateways")
                .contains("| gateways | 10,000 | 120 (1.200 %) |");
    }

    @Test
    public void hasNoSectionWithoutImpairments() {
        StringBuilder report = new StringBuilder();
        NetworkReport.append(report, run, mapper, START);
        assertThat(report).isEmpty();
    }

    @Test
    public void showsTimesBeforeTheMeasurementAsNegative() {
        assertThat(NetworkReport.relative(START - 12_500, START)).isEqualTo("−12.5 s");
        assertThat(NetworkReport.relative(START, 0)).isEmpty();
    }
}
