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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

/**
 * The hardware and the operating system of the host that the launcher runs on: the CPU's model, its sockets, cores
 * and hardware threads, the physical memory, and the operating system. They come from the events that JDK Flight
 * Recorder writes when a recording starts, of a recording of the launcher's own JVM that is stopped right away, so
 * that they are the same on every operating system that the JDK supports, including macOS.
 *
 * @param cpu the CPU's model, such as {@code Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz}, or empty
 * @param os the operating system, such as {@code Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)}
 */
public record HostDetails(String cpu, int sockets, int cores, int hardwareThreads, long memoryBytes, String os) {
    /** The details when JFR is unavailable or the recording fails. */
    public static final HostDetails UNKNOWN = new HostDetails("", 0, 0, 0, 0, "");

    // The brand that the CPU reports, which HotSpot puts on the first line of the description on x86
    private static final Pattern BRAND = Pattern.compile("^Brand: (.+?)(?:, Vendor: .*)?$", Pattern.MULTILINE);
    // The distribution that Linux's /etc/lsb-release or /etc/os-release names, as HotSpot copies it
    private static final Pattern DISTRIBUTION =
            Pattern.compile("^(?:DISTRIB_DESCRIPTION|PRETTY_NAME)=\"?([^\"\\n]+)\"?$", Pattern.MULTILINE);

    /** Collects the details; nothing here fails, and details that can't be found are left out. */
    public static HostDetails collect() {
        String cpu = "";
        int sockets = 0;
        int cores = 0;
        int hardwareThreads = 0;
        long memory = 0;
        String osVersion = "";
        try {
            Path file = Files.createTempFile("host-details", ".jfr");
            try {
                try (Recording recording = new Recording()) {
                    recording.enable("jdk.CPUInformation");
                    recording.enable("jdk.PhysicalMemory");
                    recording.enable("jdk.OSInformation");
                    recording.start();
                    recording.stop();
                    recording.dump(file);
                }
                for (RecordedEvent event : RecordingFile.readAllEvents(file)) {
                    switch (event.getEventType().getName()) {
                        case "jdk.CPUInformation" -> {
                            cpu = cpuModel(event.getString("description"), event.getString("cpu"));
                            sockets = event.getInt("sockets");
                            cores = event.getInt("cores");
                            hardwareThreads = event.getInt("hwThreads");
                        }
                        case "jdk.PhysicalMemory" -> memory = event.getLong("totalSize");
                        case "jdk.OSInformation" -> osVersion = event.getString("osVersion");
                        default -> {
                        }
                    }
                }
            } finally {
                Files.deleteIfExists(file);
            }
        } catch (Exception | LinkageError e) {
            // No JFR, such as in a JRE without the jdk.jfr module, or no temporary directory: the details stay empty
        }
        return new HostDetails(cpu, sockets, cores, hardwareThreads, memory,
                operatingSystem(osVersion, System.getProperty("os.name", ""), System.getProperty("os.version", "")));
    }

    /**
     * The CPU's model: the brand from the first line of the description, which HotSpot has on x86, or else the
     * short description that JFR has on every platform.
     */
    static String cpuModel(String description, String cpu) {
        Matcher brand = BRAND.matcher(description == null ? "" : description);
        String model = brand.find() ? brand.group(1) : cpu == null ? "" : cpu;
        return model.trim().replaceAll("\\s+", " ");
    }

    /**
     * The operating system: the Linux distribution, when HotSpot found one, with the kernel, which is the name and
     * the version that Java reports for the operating system, such as {@code Mac OS X 15.1} on macOS.
     */
    static String operatingSystem(String osVersion, String osName, String osRelease) {
        String kernel = (osName + " " + osRelease).trim();
        Matcher distribution = DISTRIBUTION.matcher(osVersion == null ? "" : osVersion);
        if (distribution.find() && !kernel.isEmpty()) {
            return distribution.group(1).trim() + " (" + kernel + ")";
        }
        return kernel;
    }

    /**
     * The details in one line, such as "Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz, 8 cores, 16 hardware threads,
     * 31 GiB, Pop!_OS 24.04 LTS (Linux 7.1.5-76070105-generic)"; more than one socket is named too.
     */
    public String summary() {
        List<String> parts = new ArrayList<>();
        if (!cpu.isEmpty()) {
            parts.add(cpu);
        }
        if (sockets > 1) {
            parts.add(sockets + " sockets");
        }
        if (cores > 0) {
            parts.add(cores + (cores == 1 ? " core" : " cores"));
        }
        if (hardwareThreads > 0) {
            parts.add(hardwareThreads + (hardwareThreads == 1 ? " hardware thread" : " hardware threads"));
        }
        if (memoryBytes > 0) {
            parts.add(gibibytes(memoryBytes));
        }
        if (!os.isEmpty()) {
            parts.add(os);
        }
        return String.join(", ", parts);
    }

    /** A memory size in whole GiB, or with one decimal below 10 GiB. */
    static String gibibytes(long bytes) {
        double gibibytes = bytes / (1024.0 * 1024 * 1024);
        return gibibytes >= 10 ? String.format(Locale.ROOT, "%.0f GiB", gibibytes)
                : String.format(Locale.ROOT, "%.1f GiB", gibibytes);
    }
}
