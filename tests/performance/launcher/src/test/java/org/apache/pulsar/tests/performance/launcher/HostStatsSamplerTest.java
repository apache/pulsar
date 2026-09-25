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
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.RunReport;
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
        write("devices/system/cpu/cpu0/thermal_throttle/core_throttle_count", "5");
        write("devices/system/cpu/cpu0/thermal_throttle/package_throttle_count", "10");
        write("devices/system/cpu/cpu1/cpufreq/scaling_cur_freq", "2900000");
        write("devices/system/cpu/cpu1/thermal_throttle/core_throttle_count", "7");
        write("devices/system/cpu/cpu1/thermal_throttle/package_throttle_count", "10");
        // Not a CPU directory
        write("devices/system/cpu/cpufreq/boost", "1");

        HostStatsSampler.Sensors sensors = HostStatsSampler.discover(sysfs);

        assertThat(sensors.available()).isTrue();
        assertThat(sensors.packageCelsius()).hasValue(68.0);
        assertThat(sensors.row(1000)).isEqualTo("1000,68.0,70.0,3000,2900,7,10,5100");
        assertThat(RunReport.HOST_STATS_HEADER.split(",")).hasSize(sensors.row(1000).split(",", -1).length);
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

        try (HostStatsSampler sampler = HostStatsSampler.start(HostStatsSampler.discover(sysfs), runDirectory)) {
            assertThat(sampler).isNotNull();
            // The first sample is taken at once
            Thread.sleep(200);
        }

        assertThat(Files.readAllLines(runDirectory.resolve(RunReport.HOST_STATS_FILE)))
                .hasSizeGreaterThanOrEqualTo(2)
                .first().isEqualTo(RunReport.HOST_STATS_HEADER);
    }

    private void write(String path, String content) throws IOException {
        Path file = sysfs.resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content + "\n");
    }
}
