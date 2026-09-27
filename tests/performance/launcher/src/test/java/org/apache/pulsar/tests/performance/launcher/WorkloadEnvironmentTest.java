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

import static org.apache.pulsar.tests.performance.launcher.PerformanceLauncher.JAVA_TOOL_OPTIONS;
import static org.apache.pulsar.tests.performance.launcher.PerformanceLauncher.PULSAR_MEM;
import static org.apache.pulsar.tests.performance.launcher.PerformanceLauncher.WORKLOAD_MEMORY;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import org.testng.annotations.Test;

public class WorkloadEnvironmentTest {
    private static final String AGENT_OPTIONS = "-javaagent:/opt/jonoffcpu/jonoffcpu-agent.jar";

    @Test
    public void givesTheWorkloadItsHeapWithoutConfiguredVariables() {
        assertThat(PerformanceLauncher.workloadEnvironment("", null))
                .containsExactly(Map.entry(PULSAR_MEM, WORKLOAD_MEMORY));
    }

    @Test
    public void keepsAConfiguredHeap() {
        assertThat(PerformanceLauncher.workloadEnvironment("", Map.of(PULSAR_MEM, "-Xms1g -Xmx1g")))
                .containsExactly(Map.entry(PULSAR_MEM, "-Xms1g -Xmx1g"));
    }

    @Test
    public void passesTheConfiguredVariables() {
        String tunables = "glibc.malloc.hugetlb=1:glibc.malloc.arena_max=4";
        assertThat(PerformanceLauncher.workloadEnvironment(AGENT_OPTIONS, Map.of("GLIBC_TUNABLES", tunables)))
                .containsEntry("GLIBC_TUNABLES", tunables)
                .containsEntry(JAVA_TOOL_OPTIONS, AGENT_OPTIONS);
    }

    @Test
    public void appendsConfiguredJavaToolOptionsToTheLauncherOptions() {
        assertThat(PerformanceLauncher.workloadEnvironment(AGENT_OPTIONS,
                Map.of(JAVA_TOOL_OPTIONS, "-XX:+UseCompactObjectHeaders")))
                .containsEntry(JAVA_TOOL_OPTIONS, AGENT_OPTIONS + " -XX:+UseCompactObjectHeaders");
        assertThat(PerformanceLauncher.workloadEnvironment("",
                Map.of(JAVA_TOOL_OPTIONS, "-XX:+UseCompactObjectHeaders")))
                .containsEntry(JAVA_TOOL_OPTIONS, "-XX:+UseCompactObjectHeaders");
    }
}
