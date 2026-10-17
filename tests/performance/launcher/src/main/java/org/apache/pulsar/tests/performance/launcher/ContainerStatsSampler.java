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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Samples each container of a run once per second into {@code container-stats.csv}: the CPUs it uses, from its
 * cgroup's {@code cpu.stat}, and its threads' voluntary and involuntary context switches per second, from
 * {@code /proc/<tid>/status}. A voluntary switch is a thread that blocked or waited for work; an involuntary one is a
 * thread that the scheduler preempted, which grows when the host's CPUs are saturated.
 *
 * <p>The files are read by a {@link Source}: {@link LocalSource} reads them directly when the launcher runs on the
 * Linux host of the containers, which needs no privileges; {@link PerfStatSidecar} reads them inside the Docker
 * engine's host, such as the Linux VM of Docker Desktop or OrbStack on macOS.
 */
final class ContainerStatsSampler implements AutoCloseable {
    static final String FILE_NAME = RunReport.CONTAINER_STATS_FILE;
    private static final long INTERVAL_MILLIS = 1000;

    /** One reading of a container: its cgroup's CPU time and each thread's voluntary and involuntary switches. */
    record Snapshot(long usageMicros, Map<String, long[]> threadSwitches) {
    }

    /** Reads the containers' snapshots, by container name. */
    interface Source {
        Map<String, Snapshot> read() throws IOException;
    }

    /** Reads the containers' cgroup and thread files on this host. */
    record LocalSource(Path procfs, Map<String, Path> cgroups) implements Source {
        @Override
        public Map<String, Snapshot> read() {
            Map<String, Snapshot> snapshots = new LinkedHashMap<>();
            cgroups.forEach((name, cgroup) -> {
                Map<String, long[]> threads = new HashMap<>();
                for (String tid : lines(cgroup.resolve("cgroup.threads"))) {
                    long[] switches = parseSwitches(lines(procfs.resolve(tid.trim()).resolve("status")));
                    if (switches != null) {
                        threads.put(tid.trim(), switches);
                    }
                }
                snapshots.put(name, new Snapshot(parseUsageMicros(lines(cgroup.resolve("cpu.stat"))), threads));
            });
            return snapshots;
        }
    }

    /** The previous reading of a container. */
    private static final class State {
        long epochMillis;
        Snapshot snapshot;
    }

    private final Source source;
    private final Map<String, State> states = new HashMap<>();
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;

    private ContainerStatsSampler(Source source, BufferedWriter writer) {
        this.source = source;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "container-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts sampling the source's containers into {@code runDirectory}. */
    static ContainerStatsSampler start(Source source, Path runDirectory) throws IOException {
        ContainerStatsSampler sampler = open(source, runDirectory);
        sampler.executor.scheduleAtFixedRate(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    /** Creates the CSV file with its header, without starting to sample. */
    static ContainerStatsSampler open(Source source, Path runDirectory) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(RunReport.CONTAINER_STATS_HEADER);
        writer.newLine();
        return new ContainerStatsSampler(source, writer);
    }

    /**
     * The cgroup directory of the process {@code pid}, from its {@code /proc/<pid>/cgroup} entry of the unified
     * (v2) hierarchy under {@code cgroupRoot}, or {@code null}, such as when the containers run in a VM.
     */
    static Path cgroupOf(Path procfs, long pid, Path cgroupRoot) {
        for (String line : lines(procfs.resolve(Long.toString(pid)).resolve("cgroup"))) {
            if (line.startsWith("0::/")) {
                Path directory = cgroupRoot.resolve(line.substring(4));
                return Files.isDirectory(directory) ? directory : null;
            }
        }
        return null;
    }

    /** One CSV row for a container, or {@code null} for its first reading. */
    String row(String name, Snapshot snapshot, long epochMillis) {
        State state = states.computeIfAbsent(name, n -> new State());
        String row = null;
        if (state.snapshot != null && epochMillis > state.epochMillis) {
            double seconds = (epochMillis - state.epochMillis) / 1000.0;
            long voluntary = 0;
            long involuntary = 0;
            for (Map.Entry<String, long[]> thread : snapshot.threadSwitches().entrySet()) {
                // A thread that started since the previous reading counts its switches from its start
                long[] previous = state.snapshot.threadSwitches().getOrDefault(thread.getKey(), new long[2]);
                voluntary += Math.max(0, thread.getValue()[0] - previous[0]);
                involuntary += Math.max(0, thread.getValue()[1] - previous[1]);
            }
            row = String.format(Locale.ROOT, "%d,%s,%.3f,%.1f,%.1f", epochMillis, name,
                    Math.max(0, snapshot.usageMicros() - state.snapshot.usageMicros()) / 1e6 / seconds,
                    voluntary / seconds, involuntary / seconds);
        }
        state.epochMillis = epochMillis;
        state.snapshot = snapshot;
        return row;
    }

    /** The {@code usage_usec} of a cgroup's {@code cpu.stat}, or 0. */
    static long parseUsageMicros(List<String> cpuStat) {
        for (String line : cpuStat) {
            if (line.startsWith("usage_usec ")) {
                return Long.parseLong(line.substring("usage_usec ".length()).trim());
            }
        }
        return 0;
    }

    /** A thread's voluntary and involuntary context switches from its {@code status}, or {@code null}. */
    static long[] parseSwitches(List<String> status) {
        long[] switches = new long[2];
        int found = 0;
        for (String line : status) {
            if (line.startsWith("voluntary_ctxt_switches:")) {
                switches[0] = Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
                found++;
            } else if (line.startsWith("nonvoluntary_ctxt_switches:")) {
                switches[1] = Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
                found++;
            }
        }
        return found == 2 ? switches : null;
    }

    private void sample() {
        try {
            long now = System.currentTimeMillis();
            for (Map.Entry<String, Snapshot> entry : source.read().entrySet()) {
                String row = row(entry.getKey(), entry.getValue(), now);
                if (row != null) {
                    writer.write(row);
                    writer.newLine();
                }
            }
            writer.flush();
        } catch (IOException | RuntimeException e) {
            System.out.println("Container stats sample failed: " + e);
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

    static List<String> lines(Path file) {
        try {
            return Files.readAllLines(file);
        } catch (IOException | RuntimeException e) {
            return new ArrayList<>();
        }
    }
}
