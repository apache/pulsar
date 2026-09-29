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
 * <p>These files are readable without privileges on Linux, where the containers' processes are the host's. A
 * container whose cgroup can't be found, such as on a host where Docker runs in a virtual machine, is left out.
 */
final class ContainerStatsSampler implements AutoCloseable {
    static final String FILE_NAME = RunReport.CONTAINER_STATS_FILE;
    private static final long INTERVAL_MILLIS = 1000;

    /** A container to sample: its name in the report and its cgroup directory. */
    record Target(String name, Path cgroup) {
    }

    /** The previous reading of a container. */
    private static final class State {
        long epochMillis;
        long usageMicros = -1;
        Map<String, long[]> switches = new HashMap<>();
    }

    private final Path procfs;
    private final List<Target> targets;
    private final Map<String, State> states = new HashMap<>();
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;

    private ContainerStatsSampler(Path procfs, List<Target> targets, BufferedWriter writer) {
        this.procfs = procfs;
        this.targets = targets;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "container-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts sampling the targets into {@code runDirectory}, or returns {@code null} when there are none. */
    static ContainerStatsSampler start(Path procfs, List<Target> targets, Path runDirectory) throws IOException {
        if (targets.isEmpty()) {
            return null;
        }
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(RunReport.CONTAINER_STATS_HEADER);
        writer.newLine();
        ContainerStatsSampler sampler = new ContainerStatsSampler(procfs, targets, writer);
        sampler.executor.scheduleAtFixedRate(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    /**
     * The cgroup directory of the process {@code pid}, from its {@code /proc/<pid>/cgroup} entry of the unified
     * (v2) hierarchy under {@code cgroupRoot}, or {@code null}.
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

    /** One CSV row for a target, or {@code null} for its first reading. */
    String row(Target target, long epochMillis) {
        State state = states.computeIfAbsent(target.name(), name -> new State());
        long usageMicros = cpuUsageMicros(target.cgroup());
        long voluntary = 0;
        long involuntary = 0;
        Map<String, long[]> switches = new HashMap<>();
        for (String tid : lines(target.cgroup().resolve("cgroup.threads"))) {
            long[] current = threadSwitches(tid.trim());
            if (current == null) {
                continue;
            }
            switches.put(tid.trim(), current);
            // A thread that started since the previous reading counts its switches from its start
            long[] previous = state.switches.getOrDefault(tid.trim(), new long[2]);
            voluntary += Math.max(0, current[0] - previous[0]);
            involuntary += Math.max(0, current[1] - previous[1]);
        }
        boolean first = state.usageMicros < 0;
        double seconds = (epochMillis - state.epochMillis) / 1000.0;
        String row = first || seconds <= 0 ? null : String.format(Locale.ROOT, "%d,%s,%.3f,%.1f,%.1f", epochMillis,
                target.name(), (usageMicros - state.usageMicros) / 1e6 / seconds, voluntary / seconds,
                involuntary / seconds);
        state.epochMillis = epochMillis;
        state.usageMicros = usageMicros;
        state.switches = switches;
        return row;
    }

    private long cpuUsageMicros(Path cgroup) {
        for (String line : lines(cgroup.resolve("cpu.stat"))) {
            if (line.startsWith("usage_usec ")) {
                return Long.parseLong(line.substring("usage_usec ".length()).trim());
            }
        }
        return 0;
    }

    /** The thread's voluntary and involuntary context switches, or {@code null} when it has exited. */
    private long[] threadSwitches(String tid) {
        long[] switches = new long[2];
        int found = 0;
        for (String line : lines(procfs.resolve(tid).resolve("status"))) {
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
            for (Target target : targets) {
                String row = row(target, now);
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

    private static List<String> lines(Path file) {
        try {
            return Files.readAllLines(file);
        } catch (IOException | RuntimeException e) {
            return new ArrayList<>();
        }
    }
}
