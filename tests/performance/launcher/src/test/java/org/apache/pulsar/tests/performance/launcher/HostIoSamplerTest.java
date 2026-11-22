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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HostIoSamplerTest {
    private Path root;

    @BeforeMethod
    public void createRoot() throws IOException {
        root = Files.createTempDirectory("host-io");
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
    public void physicalDisksAreBlockDevicesWithADevice() throws IOException {
        Files.createDirectories(root.resolve("sys/block/nvme0n1/device"));
        Files.createDirectories(root.resolve("sys/block/sda/device"));
        Files.createDirectories(root.resolve("sys/block/dm-0"));
        Files.createDirectories(root.resolve("sys/block/loop0"));

        assertThat(HostIoSampler.physicalDisks(root.resolve("sys"))).containsExactly("nvme0n1", "sda");
    }

    @Test
    public void rowIsTheRateBetweenTwoReadings() {
        List<String> disks = List.of("nvme0n1");
        HostIoSampler.Counters before = new HostIoSampler.Counters(1_000, 1_000, 600, 100,
                Map.of("nvme0n1", new long[] {0, 0, 0}));
        // One second later: 100 more jiffies, of which 20 idle and 10 waiting for I/O; 2,000,000 sectors read
        // (1,024 MB) and 4,000,000 written (2,048 MB); the disk busy for 500 of the 1,000 ms
        HostIoSampler.Counters after = new HostIoSampler.Counters(2_000, 1_100, 620, 110,
                Map.of("nvme0n1", new long[] {2_000_000, 4_000_000, 500}));

        assertThat(HostIoSampler.row(null, before, disks)).isNull();
        assertThat(HostIoSampler.row(before, after, disks)).isEqualTo("2000,70.0,10.0,1024.0,2048.0,50.0");
        assertThat(HostIoSampler.header(disks))
                .isEqualTo("epochMs,cpuBusyPercent,cpuIowaitPercent,nvme0n1ReadMBps,nvme0n1WriteMBps,"
                        + "nvme0n1BusyPercent");
    }

    @Test
    public void readsProcCounters() throws IOException {
        Path proc = Files.createDirectories(root.resolve("proc"));
        Files.writeString(proc.resolve("stat"), "cpu  100 5 50 800 20 3 2 0 0 0\ncpu0 1 2 3 4 5 6 7 8 0 0\n");
        Files.writeString(proc.resolve("diskstats"),
                "259 0 nvme0n1 10 0 2048 5 20 0 4096 7 0 30 12\n259 1 nvme0n1p1 1 0 8 1 1 0 8 1 0 1 1\n");
        Files.createDirectories(root.resolve("sys/block/nvme0n1/device"));

        HostIoSampler.Counters counters;
        HostIoSampler.Source source = new HostIoSampler.LocalSource(proc, root.resolve("sys"));
        try (HostIoSampler sampler = HostIoSampler.start(source, root)) {
            assertThat(sampler).isNotNull();
            counters = sampler.read(5_000);
        }
        assertThat(counters.cpuTotal()).isEqualTo(980);
        assertThat(counters.cpuIdle()).isEqualTo(800);
        assertThat(counters.cpuIowait()).isEqualTo(20);
        assertThat(counters.disks().get("nvme0n1")).containsExactly(2048, 4096, 30);
        assertThat(Files.readAllLines(root.resolve(HostIoSampler.FILE_NAME)).get(0))
                .startsWith("epochMs,cpuBusyPercent,cpuIowaitPercent,nvme0n1ReadMBps");
    }
}
