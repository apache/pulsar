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
                + "13000,broker-0,9.000,1.0,1.0\n");
        Files.writeString(run.resolve(RunReport.PERF_STAT_FILE), RunReport.PERF_STAT_HEADER + "\n"
                + "11000,broker-0,2000,1500,10,4000000000,8000000000\n"
                + "12000,broker-0,4000,2500,30,8000000000,12000000000\n");
        StringBuilder report = new StringBuilder();

        ContainerStatsReport.append(report, run, 10_000, 12_000, 1_000_000);

        assertThat(report.toString())
                .contains("## Containers")
                .contains("| Container | CPUs | CPU s per million messages | Voluntary switches/s | Involuntary"
                        + " switches/s | CPU migrations/s | GHz | IPC |")
                // 3 CPUs on average for 2 s, over one million messages; 12 G cycles in 6 s of CPU; 20 G instructions
                .contains("| broker-0 | 3.00 | 6.0 | 2,000 | 200 | 20 | 2.00 | 1.67 |");
    }

    @Test
    public void appendsNothingWithoutTheFiles() throws IOException {
        StringBuilder report = new StringBuilder();
        ContainerStatsReport.append(report, run, 0, 1_000, 1);
        assertThat(report).isEmpty();
    }
}
