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
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HostStatsSamplerTest {
    private Path sysfs;

    @BeforeMethod
    public void createSysfs() throws IOException {
        sysfs = Files.createTempDirectory("host-stats-sysfs");
    }

    @AfterMethod(alwaysRun = true)
    public void deleteSysfs() throws IOException {
        try (Stream<Path> paths = Files.walk(sysfs)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void readsIntelTemperaturesFrequenciesThrottlesAndFans() throws IOException {
        write("class/hwmon/hwmon0/name", "coretemp");
        write("class/hwmon/hwmon0/temp1_label", "Package id 0");
        write("class/hwmon/hwmon0/temp1_input", "68000");
        write("class/hwmon/hwmon0/temp2_label", "Core 0");
        write("class/hwmon/hwmon0/temp2_input", "70000");
        write("class/hwmon/hwmon0/temp3_label", "Core 1");
        write("class/hwmon/hwmon0/temp3_input", "66000");
        write("class/hwmon/hwmon1/name", "dell_smm");
        write("class/hwmon/hwmon1/fan1_input", "5000");
        write("class/hwmon/hwmon1/fan2_input", "5100");
        // An hwmon device that isn't the CPU, such as the NVMe drive, is not a CPU temperature
        write("class/hwmon/hwmon2/name", "nvme");
        write("class/hwmon/hwmon2/temp1_input", "90000");
        write("devices/system/cpu/cpu0/cpufreq/scaling_cur_freq", "3100000");
        writeCpu(0, 0, 0, "5", "10");
        write("devices/system/cpu/cpu1/cpufreq/scaling_cur_freq", "2900000");
        writeCpu(1, 0, 1, "7", "10");
        // Not a CPU directory
        write("devices/system/cpu/cpufreq/boost", "1");

        HostStatsSampler.Sensors sensors = HostStatsSampler.discover(sysfs);

        assertThat(sensors.available()).isTrue();
        assertThat(sensors.packageCelsius()).hasValue(68.0);
        // The core counters are summed; both CPUs are in the same package, whose counter is read once
        assertThat(sensors.row(1000)).isEqualTo("1000,68.0,70.0,3000,2900,12,10,5100");
        assertThat(RunReport.HOST_STATS_HEADER.split(",")).hasSize(sensors.row(1000).split(",", -1).length);
    }

    @Test
    public void readsEachCoresAndPackagesThrottleCounterOnce() throws IOException {
        // Two cores with two hyperthreads each, which show their core's counter, in two packages
        writeCpu(0, 0, 0, "100", "20");
        writeCpu(1, 0, 0, "100", "20");
        writeCpu(2, 1, 0, "5", "3");
        writeCpu(3, 1, 0, "5", "3");

        assertThat(HostStatsSampler.discover(sysfs).row(1000)).isEqualTo("1000,,,,,105,23,");
    }

    @Test
    public void readsTheAmdPackageTemperatureAndLeavesMissingValuesEmpty() throws IOException {
        write("class/hwmon/hwmon0/name", "k10temp");
        write("class/hwmon/hwmon0/temp1_label", "Tctl");
        write("class/hwmon/hwmon0/temp1_input", "61250");
        write("class/hwmon/hwmon0/temp3_label", "Tccd1");
        write("class/hwmon/hwmon0/temp3_input", "70000");

        HostStatsSampler.Sensors sensors = HostStatsSampler.discover(sysfs);

        assertThat(sensors.row(2000)).isEqualTo("2000,61.3,,,,,,");
    }

    @Test
    public void fallsBackToThePackageThermalZone() throws IOException {
        write("class/thermal/thermal_zone0/type", "acpitz");
        write("class/thermal/thermal_zone0/temp", "40000");
        write("class/thermal/thermal_zone1/type", "x86_pkg_temp");
        write("class/thermal/thermal_zone1/temp", "72000");

        assertThat(HostStatsSampler.discover(sysfs).packageCelsius()).hasValue(72.0);
    }

    @Test
    public void doesNotSampleAHostWithoutSensors() throws IOException {
        HostStatsSampler.Sensors sensors = HostStatsSampler.discover(sysfs.resolve("missing"));

        assertThat(sensors.available()).isFalse();
        assertThat(sensors.packageCelsius()).isEmpty();
        Path runDirectory = Files.createDirectories(sysfs.resolve("run"));
        assertThat(HostStatsSampler.start(sensors, runDirectory)).isNull();
        assertThat(runDirectory.resolve(RunReport.HOST_STATS_FILE)).doesNotExist();
    }

    @Test
    public void writesOneRowPerSample() throws Exception {
        write("devices/system/cpu/cpu0/cpufreq/scaling_cur_freq", "3000000");
        Path runDirectory = Files.createDirectories(sysfs.resolve("run"));

        Path hostStats = runDirectory.resolve(RunReport.HOST_STATS_FILE);
        try (HostStatsSampler sampler = HostStatsSampler.start(HostStatsSampler.discover(sysfs), runDirectory)) {
            assertThat(sampler).isNotNull();
            // The first sample is taken at once, after the header
            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(Files.readAllLines(hostStats)).hasSizeGreaterThanOrEqualTo(2));
        }

        assertThat(Files.readAllLines(hostStats)).first().isEqualTo(RunReport.HOST_STATS_HEADER);
    }

    private void write(String path, String content) throws IOException {
        Path file = sysfs.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content + "\n");
    }

    private void writeCpu(int cpu, int packageId, int coreId, String coreThrottles, String packageThrottles)
            throws IOException {
        String directory = "devices/system/cpu/cpu" + cpu + "/";
        write(directory + "topology/physical_package_id", Integer.toString(packageId));
        write(directory + "topology/core_id", Integer.toString(coreId));
        write(directory + "thermal_throttle/core_throttle_count", coreThrottles);
        write(directory + "thermal_throttle/package_throttle_count", packageThrottles);
    }
}
