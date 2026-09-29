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
import java.util.List;
import org.testng.annotations.Test;

public class PerfStatSidecarTest {
    @Test
    public void convertsPerfIntervalsIntoRowsPerContainer() {
        List<PerfStatSidecar.Target> targets = List.of(
                new PerfStatSidecar.Target("broker-0", "system.slice/docker-a.scope"),
                new PerfStatSidecar.Target("bookie-0", "system.slice/docker-b.scope"));
        // perf stat -x, -I 1000 --for-each-cgroup output, as perf 6.6 writes it
        List<String> perf = List.of(
                "# started on Wed Sep 30 10:00:00 2026",
                "",
                "1.000753600,1001.36,msec,task-clock,system.slice/docker-a.scope,1001357454,100.00,1.001,CPUs utilized",
                "1.000753600,9,,context-switches,system.slice/docker-a.scope,1001357454,100.00,8.988,/sec",
                "1.000753600,0,,cpu-migrations,system.slice/docker-a.scope,1001357454,100.00,0.000,/sec",
                "1.000753600,4603060289,,cycles,system.slice/docker-a.scope,1001357454,100.00,4.597,GHz",
                "1.000753600,10026701643,,instructions,system.slice/docker-a.scope,1001357454,100.00,2.18,"
                        + "insn per cycle",
                "1.000753600,12.5,msec,task-clock,system.slice/docker-b.scope,12500000,100.00,0.012,CPUs utilized",
                "1.000753600,30,,context-switches,system.slice/docker-b.scope,12500000,100.00,2400,/sec",
                "1.000753600,2,,cpu-migrations,system.slice/docker-b.scope,12500000,100.00,160,/sec",
                "1.000753600,<not supported>,,cycles,system.slice/docker-b.scope,0,100.00,,",
                "1.000753600,<not supported>,,instructions,system.slice/docker-b.scope,0,100.00,,",
                "1.000753600,1,,context-switches,system.slice/other.scope,1,100.00,1,/sec");

        assertThat(PerfStatSidecar.rows(1_000_000, perf, targets)).containsExactly(
                "1001001,broker-0,1001.36,9,0,4603060289,10026701643",
                "1001001,bookie-0,12.5,30,2,,");
    }
}
