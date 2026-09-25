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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Samples the host's thermal state once per second into {@code host-stats.csv}: the CPU package temperature and the
 * hottest core's, the mean and lowest core frequency, the kernel's thermal throttle counters and the fastest fan. A
 * laptop or a small server that heats up during a run lowers its clock or throttles, which changes the results, so
 * the run report shows these next to the throughput and latency.
 *
 * <p>The values are read from Linux's sysfs files directly: {@code /sys/class/hwmon} for the temperatures ({@code
 * coretemp} on Intel, {@code k10temp} or {@code zenpower} on AMD) and fans, {@code /sys/class/thermal} for the
 * {@code x86_pkg_temp} zone when hwmon has no CPU sensor, and {@code /sys/devices/system/cpu} for the frequencies and
 * the throttle counters. A value the host doesn't provide is left empty; a host that provides none of them, such as
 * one that isn't Linux, is not sampled. Reading a few dozen small files once per second has no measurable cost.
 */
final class HostStatsSampler implements AutoCloseable {
    // The CSV the run report reads; its name and columns are the report tool's
    static final String FILE_NAME = RunReport.HOST_STATS_FILE;
    static final String HEADER = RunReport.HOST_STATS_HEADER;
    private static final long INTERVAL_MILLIS = 1000;
    private static final Pattern CPU_DIRECTORY = Pattern.compile("cpu\\d+");

    /** The sensor files of a host, found once. */
    record Sensors(List<Path> packageTemperatures, List<Path> coreTemperatures, List<Path> frequencies,
                   List<Path> coreThrottleCounters, List<Path> packageThrottleCounters, List<Path> fans) {

        boolean available() {
            return !packageTemperatures.isEmpty() || !coreTemperatures.isEmpty() || !frequencies.isEmpty()
                    || !coreThrottleCounters.isEmpty();
        }

        /** The CPU package temperature in °C: the hottest package, else the hottest core, if the host has them. */
        OptionalDouble packageCelsius() {
            OptionalDouble packages = maxCelsius(packageTemperatures);
            return packages.isPresent() ? packages : maxCelsius(coreTemperatures);
        }

        /** One CSV row: the columns of {@link RunReport#HOST_STATS_HEADER}. */
        String row(long epochMillis) {
            OptionalDouble packageCelsius = packageCelsius();
            OptionalDouble coreCelsius = maxCelsius(coreTemperatures);
            List<Long> kiloHertz = values(frequencies);
            OptionalDouble meanMegaHertz = kiloHertz.stream().mapToLong(Long::longValue).average();
            OptionalLong minKiloHertz = kiloHertz.stream().mapToLong(Long::longValue).min();
            // The counters count per logical CPU since boot; the most on any CPU tells whether and how often the
            // cores, or the package, throttled.
            OptionalLong coreThrottles = values(coreThrottleCounters).stream().mapToLong(Long::longValue).max();
            OptionalLong packageThrottles = values(packageThrottleCounters).stream().mapToLong(Long::longValue).max();
            OptionalLong fanRpm = values(fans).stream().mapToLong(Long::longValue).max();
            return epochMillis + "," + format(packageCelsius) + "," + format(coreCelsius) + ","
                    + (meanMegaHertz.isPresent() ? Math.round(meanMegaHertz.getAsDouble() / 1000) : "") + ","
                    + (minKiloHertz.isPresent() ? minKiloHertz.getAsLong() / 1000 : "") + ","
                    + format(coreThrottles) + "," + format(packageThrottles) + "," + format(fanRpm);
        }

        private static OptionalDouble maxCelsius(List<Path> millidegrees) {
            return values(millidegrees).stream().mapToDouble(value -> value / 1000.0).max();
        }

        private static String format(OptionalDouble value) {
            return value.isPresent() ? String.format(Locale.ROOT, "%.1f", value.getAsDouble()) : "";
        }

        private static String format(OptionalLong value) {
            return value.isPresent() ? Long.toString(value.getAsLong()) : "";
        }
    }

    private final Sensors sensors;
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;

    private HostStatsSampler(Sensors sensors, BufferedWriter writer) {
        this.sensors = sensors;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "host-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts sampling the host into {@code runDirectory}, or returns {@code null} when the host has none of the
     * sensor files.
     */
    static HostStatsSampler start(Sensors sensors, Path runDirectory) throws IOException {
        if (!sensors.available()) {
            return null;
        }
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(HEADER);
        writer.newLine();
        HostStatsSampler sampler = new HostStatsSampler(sensors, writer);
        sampler.executor.scheduleAtFixedRate(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    /** Finds the sensor files under {@code sysfs}, normally {@code /sys}. */
    static Sensors discover(Path sysfs) {
        List<Path> packageTemperatures = new ArrayList<>();
        List<Path> coreTemperatures = new ArrayList<>();
        List<Path> fans = new ArrayList<>();
        for (Path hwmon : list(sysfs.resolve("class/hwmon"))) {
            String name = read(hwmon.resolve("name"));
            for (Path file : list(hwmon)) {
                String fileName = file.getFileName().toString();
                if (fileName.matches("fan\\d+_input")) {
                    fans.add(file);
                } else if (fileName.matches("temp\\d+_input") && name != null) {
                    String label = read(hwmon.resolve(fileName.replace("_input", "_label")));
                    if ("coretemp".equals(name)) {
                        // Intel: one "Package id N" and one "Core N" sensor for each package and core
                        if (label != null && label.startsWith("Package")) {
                            packageTemperatures.add(file);
                        } else if (label != null && label.startsWith("Core")) {
                            coreTemperatures.add(file);
                        }
                    } else if (name.equals("k10temp") || name.equals("zenpower")) {
                        // AMD: Tctl, or Tdie where it differs, stands for the package; the Tccd sensors are dies
                        if (label == null || label.equals("Tctl") || label.equals("Tdie")) {
                            packageTemperatures.add(file);
                        }
                    }
                }
            }
        }
        if (packageTemperatures.isEmpty() && coreTemperatures.isEmpty()) {
            for (Path zone : list(sysfs.resolve("class/thermal"))) {
                if ("x86_pkg_temp".equals(read(zone.resolve("type")))) {
                    packageTemperatures.add(zone.resolve("temp"));
                }
            }
        }
        List<Path> frequencies = new ArrayList<>();
        List<Path> coreThrottleCounters = new ArrayList<>();
        List<Path> packageThrottleCounters = new ArrayList<>();
        for (Path cpu : list(sysfs.resolve("devices/system/cpu"))) {
            if (!CPU_DIRECTORY.matcher(cpu.getFileName().toString()).matches()) {
                continue;
            }
            addIfReadable(frequencies, cpu.resolve("cpufreq/scaling_cur_freq"));
            addIfReadable(coreThrottleCounters, cpu.resolve("thermal_throttle/core_throttle_count"));
            addIfReadable(packageThrottleCounters, cpu.resolve("thermal_throttle/package_throttle_count"));
        }
        return new Sensors(packageTemperatures, coreTemperatures, frequencies, coreThrottleCounters,
                packageThrottleCounters, fans);
    }

    private void sample() {
        try {
            writer.write(sensors.row(System.currentTimeMillis()));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.out.println("Host stats sample failed: " + e);
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        writer.close();
    }

    private static List<Long> values(List<Path> files) {
        List<Long> values = new ArrayList<>(files.size());
        for (Path file : files) {
            String text = read(file);
            if (text != null) {
                try {
                    values.add(Long.parseLong(text));
                } catch (NumberFormatException e) {
                    // A sensor that reports something else is skipped
                }
            }
        }
        return values;
    }

    private static void addIfReadable(List<Path> files, Path file) {
        if (read(file) != null) {
            files.add(file);
        }
    }

    private static List<Path> list(Path directory) {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.sorted().toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file).trim();
        } catch (IOException e) {
            return null;
        }
    }
}
