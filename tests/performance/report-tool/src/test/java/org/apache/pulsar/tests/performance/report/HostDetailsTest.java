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
import org.testng.annotations.Test;

public class HostDetailsTest {
    @Test
    public void collectsTheHostFromJfr() {
        HostDetails details = HostDetails.collect();

        assertThat(details.cpu()).isNotEmpty();
        assertThat(details.cores()).isPositive();
        assertThat(details.hardwareThreads()).isGreaterThanOrEqualTo(details.cores());
        assertThat(details.memoryBytes()).isPositive();
        assertThat(details.os()).contains(System.getProperty("os.name"));
    }

    @Test
    public void readsTheCpuModelFromTheDescription() {
        // HotSpot's description on x86, whose first line has the brand
        assertThat(HostDetails.cpuModel("Brand: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz, Vendor: GenuineIntel\n"
                + "Family: <unknown> (0x6), Model: <unknown> (0x9e), Stepping: 0xd\n", "Intel (null) (HT) SSE"))
                .isEqualTo("Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz");
        assertThat(HostDetails.cpuModel("Brand: AMD Ryzen 9 7950X 16-Core Processor    ", "AMD"))
                .isEqualTo("AMD Ryzen 9 7950X 16-Core Processor");
        // Without a brand, such as on AArch64, the short description
        assertThat(HostDetails.cpuModel("0x61:0x0:0x0:0, fp, asimd", "AArch64  0x61:0x0"))
                .isEqualTo("AArch64 0x61:0x0");
        assertThat(HostDetails.cpuModel(null, null)).isEmpty();
    }

    @Test
    public void namesTheLinuxDistributionWithTheKernel() {
        String lsbRelease = "DISTRIB_ID=Pop\nDISTRIB_RELEASE=24.04\nDISTRIB_DESCRIPTION=\"Pop!_OS 24.04 LTS\"\n"
                + "uname: Linux 7.1.5-76070105-generic #202607241434 SMP x86_64\nlibc: glibc 2.39 NPTL 2.39\n";
        assertThat(HostDetails.operatingSystem(lsbRelease, "Linux", "7.1.5-76070105-generic"))
                .isEqualTo("Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)");
        String osRelease = "NAME=\"Fedora Linux\"\nPRETTY_NAME=\"Fedora Linux 42 (Workstation Edition)\"\n";
        assertThat(HostDetails.operatingSystem(osRelease, "Linux", "6.15.4"))
                .isEqualTo("Fedora Linux 42 (Workstation Edition) (Linux 6.15.4)");
        // Other operating systems are named as Java names them
        assertThat(HostDetails.operatingSystem("uname: Darwin 24.1.0", "Mac OS X", "15.1")).isEqualTo("Mac OS X 15.1");
        assertThat(HostDetails.operatingSystem(null, "", "")).isEmpty();
    }

    @Test
    public void summarizesTheHostInOneLine() {
        assertThat(new HostDetails("Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz", 1, 8, 16, 33_256_595_456L,
                "Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)").summary())
                .isEqualTo("Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz, 8 cores, 16 hardware threads, 31 GiB,"
                        + " Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)");
        assertThat(new HostDetails("Xeon", 2, 64, 128, 8L << 40, "Linux 6.8").summary())
                .isEqualTo("Xeon, 2 sockets, 64 cores, 128 hardware threads, 8192 GiB, Linux 6.8");
        assertThat(new HostDetails("", 0, 0, 0, 3L << 29, "").summary()).isEqualTo("1.5 GiB");
        assertThat(HostDetails.UNKNOWN.summary()).isEmpty();
    }

    @Test
    public void summarizesTheDockerEngineInOneLine() {
        assertThat(new DockerEngine("28.4.0", 4, 8_589_934_592L, "Docker Desktop", "6.10.14-linuxkit", "aarch64")
                .summary())
                .isEqualTo("Docker 28.4.0, 4 CPUs, 8.0 GiB, Docker Desktop (kernel 6.10.14-linuxkit, aarch64)");
        assertThat(new DockerEngine("", 1, 0, "", "", "").summary()).isEqualTo("1 CPU");
    }
}
