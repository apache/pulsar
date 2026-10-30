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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerStatsSamplerTest {
    private Path root;

    @BeforeMethod
    public void createRoot() throws IOException {
        root = Files.createTempDirectory("container-stats");
    }

    @AfterMethod(alwaysRun = true)
    public void deleteRoot() throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void findsTheUnifiedHierarchyCgroup() throws IOException {
        Path cgroup = Files.createDirectories(root.resolve("cgroup/system.slice/docker-abc.scope"));
        Files.createDirectories(root.resolve("proc/42"));
        Files.writeString(root.resolve("proc/42/cgroup"), "0::/system.slice/docker-abc.scope\n");

        assertThat(ContainerStatsSampler.cgroupOf(root.resolve("proc"), 42, root.resolve("cgroup"))).isEqualTo(cgroup);
        assertThat(ContainerStatsSampler.cgroupOf(root.resolve("proc"), 43, root.resolve("cgroup"))).isNull();
    }

    @Test
    public void rowIsTheRateSinceThePreviousReading() throws IOException {
        Path cgroup = Files.createDirectories(root.resolve("cgroup/broker"));
        Path proc = Files.createDirectories(root.resolve("proc"));
        thread(proc, "10", 100, 5);
        thread(proc, "11", 50, 1);
        Files.writeString(cgroup.resolve("cgroup.threads"), "10\n11\n");
        Files.writeString(cgroup.resolve("cpu.stat"), "usage_usec 1000000\nuser_usec 800000\n");
        ContainerStatsSampler.LocalSource source = new ContainerStatsSampler.LocalSource(proc,
                Map.of("broker-0", cgroup));
        ContainerStatsSampler sampler = ContainerStatsSampler.open(source, root);
        try {
            // The first reading only starts the counts
            assertThat(sampler.row("broker-0", source.read().get("broker-0"), 1_000)).isNull();
            // Two seconds later: 3 s of CPU; thread 10 switched 40 and 2 times, 11 exited, 12 started and switched 20
            thread(proc, "10", 140, 7);
            thread(proc, "12", 20, 0);
            Files.delete(proc.resolve("11/status"));
            Files.writeString(cgroup.resolve("cgroup.threads"), "10\n11\n12\n");
            Files.writeString(cgroup.resolve("cpu.stat"), "usage_usec 4000000\n");

            assertThat(sampler.row("broker-0", source.read().get("broker-0"), 3_000))
                    .isEqualTo("3000,broker-0,1.500,30.0,1.0");
        } finally {
            sampler.close();
        }
    }

    private static void thread(Path proc, String tid, long voluntary, long involuntary) throws IOException {
        Files.createDirectories(proc.resolve(tid));
        Files.writeString(proc.resolve(tid).resolve("status"), "Name:\tjava\nvoluntary_ctxt_switches:\t" + voluntary
                + "\nnonvoluntary_ctxt_switches:\t" + involuntary + "\n");
    }
}
