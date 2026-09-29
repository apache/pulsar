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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class PerfStatSidecarTest {
    @Test
    public void convertsPerfIntervalsIntoRowsPerContainer() {
        Map<String, String> cgroups = new LinkedHashMap<>();
        cgroups.put("broker-0", "system.slice/docker-a.scope");
        cgroups.put("bookie-0", "system.slice/docker-b.scope");
        String a = ",system.slice/docker-a.scope,1001357454,100.00,,";
        String b = ",system.slice/docker-b.scope,12500000,100.00,,";
        // perf stat -x, -I 1000 --for-each-cgroup output, as perf 6.6 writes it; bookie-0's host has no CPU counters
        List<String> perf = List.of(
                "# started on Wed Sep 30 10:00:00 2026",
                "",
                "1.000753600,1001.36,msec,task-clock" + a,
                "1.000753600,9,,context-switches" + a,
                "1.000753600,0,,cpu-migrations" + a,
                "1.000753600,3,,page-faults" + a,
                "1.000753600,4603060289,,cycles" + a,
                "1.000753600,10026701643,,instructions" + a,
                "1.000753600,292526,,cache-references" + a,
                "1.000753600,25437,,cache-misses" + a,
                "1.000753600,65756,,L1-dcache-load-misses" + a,
                "1.000753600,3171465,,branch-misses" + a,
                "1.000753600,12.5,msec,task-clock" + b,
                "1.000753600,30,,context-switches" + b,
                "1.000753600,2,,cpu-migrations" + b,
                "1.000753600,0,,page-faults" + b,
                "1.000753600,<not supported>,,cycles" + b,
                "1.000753600,<not supported>,,instructions" + b,
                "1.000753600,<not supported>,,cache-references" + b,
                "1.000753600,<not supported>,,cache-misses" + b,
                "1.000753600,<not supported>,,L1-dcache-load-misses" + b,
                "1.000753600,<not supported>,,branch-misses" + b,
                "1.000753600,1,,context-switches,system.slice/other.scope,1,100.00,1,/sec");

        assertThat(PerfStatSidecar.rows(1_000_000, perf, cgroups)).containsExactly(
                "1001001,broker-0,1001.36,9,0,3,4603060289,10026701643,292526,25437,65756,3171465",
                "1001001,bookie-0,12.5,30,2,0,,,,,,");
    }

    @Test
    public void parsesTheContainersCounters() {
        Map<String, ContainerStatsSampler.Snapshot> snapshots = PerfStatSidecar.parseSnapshots(List.of(
                "U,broker-0,4000000",
                "T,broker-0,10,140,7",
                "T,broker-0,12,20,0",
                "U,bookie-0,",
                "T,bookie-0,20,,"));

        assertThat(snapshots).containsOnlyKeys("broker-0", "bookie-0");
        assertThat(snapshots.get("broker-0").usageMicros()).isEqualTo(4_000_000);
        assertThat(snapshots.get("broker-0").threadSwitches()).containsOnlyKeys("10", "12");
        assertThat(snapshots.get("broker-0").threadSwitches().get("10")).containsExactly(140, 7);
        assertThat(snapshots.get("bookie-0").usageMicros()).isZero();
        assertThat(snapshots.get("bookie-0").threadSwitches()).isEmpty();
    }

    @Test
    public void splitsTheEngineHostsFiles() {
        HostIoSampler.HostFiles files = PerfStatSidecar.parseHostFiles(List.of(
                "cpu  100 0 50 800 20 0 5 0 0 0",
                "---",
                " 259       0 nvme0n1 10 0 800 5 20 0 1600 10 0 30 15 0 0 0 0 0 0"));

        assertThat(files.stat()).containsExactly("cpu  100 0 50 800 20 0 5 0 0 0");
        assertThat(files.diskstats())
                .containsExactly(" 259       0 nvme0n1 10 0 800 5 20 0 1600 10 0 30 15 0 0 0 0 0 0");
    }
}
