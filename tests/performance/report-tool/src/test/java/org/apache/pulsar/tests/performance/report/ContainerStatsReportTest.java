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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerStatsReportTest {
    private Path run;

    @BeforeMethod
    public void createRun() throws IOException {
        run = Files.createTempDirectory("container-stats-report");
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
    public void summarizesTheMeasurementPerContainer() throws IOException {
        // The measurement runs from 10,000 to 12,000 ms; the rows at 11,000 and 12,000 cover it
        Files.writeString(run.resolve(RunReport.CONTAINER_STATS_FILE), RunReport.CONTAINER_STATS_HEADER + "\n"
                + "10000,broker-0,9.000,1.0,1.0\n"
                + "11000,broker-0,2.000,1000.0,100.0\n"
                + "12000,broker-0,4.000,3000.0,300.0\n"
                + "11000,bookie-0,1.000,500.0,10.0\n"
                + "12000,bookie-0,1.000,500.0,10.0\n"
                + "13000,broker-0,9.000,1.0,1.0\n");
        // broker-0: 3 CPUs of task-clock per second, 6 G cycles and 10 G instructions per second on average
        Files.writeString(run.resolve(RunReport.PERF_STAT_FILE), RunReport.PERF_STAT_HEADER + "\n"
                + "11000,broker-0,2000,1500,10,5,4000000000,8000000000,20000000,5000000,40000000,10000000\n"
                + "12000,broker-0,4000,2500,30,15,8000000000,12000000000,20000000,5000000,40000000,10000000\n"
                + "11000,bookie-0,1000,100,2,1,,,,,,\n"
                + "12000,bookie-0,1000,100,2,1,,,,,,\n");
        Files.writeString(run.resolve(RunReport.HOST_IO_FILE),
                "epochMs,cpuBusyPercent,cpuIowaitPercent,nvme0n1ReadMBps,nvme0n1WriteMBps,nvme0n1BusyPercent\n"
                        + "11000,80.0,1.0,0.0,400.0,40.0\n"
                        + "12000,90.0,3.0,2.0,600.0,60.0\n");
        StringBuilder report = new StringBuilder();

        ContainerStatsReport.append(report, run, 10_000, 12_000, 1_000_000);

        assertThat(report.toString())
                .contains("## Containers")
                .contains("the Docker engine host's CPUs were 85.0 % busy and 2.0 % waiting for I/O on average; "
                        + "disk nvme0n1 read 1 MB/s and wrote 500 MB/s, busy 50.0 %")
                // 3 CPUs on average for 2 s, over one million messages
                .contains("| broker-0 | 3.00 | 6.0 | 2,000 | 200 | 20 | 10 |")
                .contains("| **All containers** | 4.00 | 8.0 | 2,500 | 210 | 22 | 11 |")
                // 6 G cycles in 3 s of CPU per second; 10 G instructions; 5 M LLC misses of 20 M references
                .contains("| broker-0 | 2.00 | 1.67 | 0.50 | 25.0 | 4.00 | 1.00 |")
                // bookie-0's host has no CPU counters, so its efficiency columns are empty
                .contains("| bookie-0 |  |  |  |  |  |  |");
        JsonNode summary = new ObjectMapper().readTree(run.resolve(RunReport.CONTAINER_SUMMARY_FILE).toFile());
        assertThat(summary.path("host").path("cpuBusyPercent").asDouble()).isEqualTo(85.0);
        assertThat(summary.path("containers").path("broker-0").path("cpus").asDouble()).isEqualTo(3.0);
        assertThat(summary.path("containers").path("broker-0").path("instructionsPerCycle").asDouble())
                .isEqualTo(1.667);
        assertThat(summary.path("containers").path("bookie-0").has("instructionsPerCycle")).isFalse();
        assertThat(summary.path("allContainers").path("cpuSecondsPerMillionMessages").asDouble()).isEqualTo(8.0);
    }

    @Test
    public void appendsNothingWithoutTheFiles() throws IOException {
        StringBuilder report = new StringBuilder();
        ContainerStatsReport.append(report, run, 0, 1_000, 1);
        assertThat(report).isEmpty();
        assertThat(run.resolve(RunReport.CONTAINER_SUMMARY_FILE)).doesNotExist();
    }
}
