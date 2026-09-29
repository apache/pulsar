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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * A privileged sidecar container in the Docker engine's host, which counts each container's CPU time, context
 * switches, CPU migrations, page faults, cycles, instructions, and cache and branch misses with {@code perf stat} into
 * {@code perf-stat.csv}, once per second, and serves the containers' cgroup and thread counters to the
 * {@link ContainerStatsSampler} and the host's CPU and disk counters to the {@link HostIoSampler}. The perf counts are
 * exact, not sampled, and cost next to nothing, so they suit unprofiled runs. It starts idle with the run; perf starts
 * counting once the run's containers exist.
 *
 * <p>The sidecar runs in the engine host's PID and cgroup namespaces, so it works on a Linux host as well as in the
 * Linux VM of Docker Desktop or OrbStack, on x86-64 and arm64: it finds each container's cgroup from the PID that
 * Docker's container inspect reports. It is built once from Alpine's {@code perf} package. Its output stays in the
 * sidecar until the run ends, so that the run directory gets no root-owned files. The events are perf's generic ones,
 * which the kernel maps to the CPU's own; a count that the CPU or the VM doesn't provide is left empty, and when perf
 * can't count at all, the sidecar still serves the containers' counters.
 */
final class PerfStatSidecar implements AutoCloseable {
    static final String FILE_NAME = RunReport.PERF_STAT_FILE;
    // The software events, the cycles and instructions counters, and four more hardware counters: last-level cache
    // references and misses, L1 data cache load misses and branch misses. Common x86 and Arm cores have at least six
    // counters per hardware thread, so that these count without multiplexing
    static final String EVENTS = "task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,"
            + "cache-references,cache-misses,L1-dcache-load-misses,branch-misses";
    // GNU date from coreutils, for the start time in milliseconds, which BusyBox's date doesn't print
    private static final String IMAGE = "pulsar-performance-perf-stat:alpine-3.20-coreutils";
    private static final String TARGETS = "/tmp/targets";
    private static final String OUTPUT = "/tmp/perf-stat.raw";
    private static final String START = "/tmp/perf-stat.start";
    // Arguments: name=pid pairs. Resolves each container's cgroup, then starts perf stat detached from this docker exec
    private static final String COUNT_SCRIPT = ": > " + TARGETS + ".tmp\n"
            + "for pair in \"$@\"; do\n"
            + "  name=${pair%%=*}; pid=${pair#*=}\n"
            + "  cg=$(sed -n 's|^0::/||p' /proc/$pid/cgroup 2>/dev/null)\n"
            + "  if [ -n \"$cg\" ] && [ -d \"/sys/fs/cgroup/$cg\" ]; then echo \"$name,$cg\" >> " + TARGETS
            + ".tmp; fi\n"
            + "done\n"
            + "mv " + TARGETS + ".tmp " + TARGETS + "\n"
            + "[ -s " + TARGETS + " ] || exit 0\n"
            + "cgroups=$(cut -d, -f2 " + TARGETS + " | paste -sd, -)\n"
            + "date +%s%3N > /tmp/perf-stat.t0\n"
            + "setsid perf stat -a -x, -I 1000 -e " + EVENTS + " --for-each-cgroup \"$cgroups\" -o " + OUTPUT
            + " < /dev/null > /dev/null 2> /tmp/perf-stat.err &\n"
            + "sleep 2\n"
            + "if kill -0 $! 2>/dev/null; then mv /tmp/perf-stat.t0 " + START + "; fi\n";
    private static final String DISKS_SCRIPT =
            "for d in /sys/block/*; do if [ -e \"$d/device\" ]; then basename \"$d\"; fi; done";
    // Prints each container's cgroup CPU time and its threads' context switches:
    // U,<name>,<usage_usec> and T,<name>,<tid>,<voluntary>,<involuntary>
    private static final String SNAPSHOT_SCRIPT = "while IFS=, read -r name cg; do\n"
            + "  echo \"U,$name,$(sed -n 's/^usage_usec //p' /sys/fs/cgroup/$cg/cpu.stat)\"\n"
            + "  grep -H ctxt_switches $(sed 's|.*|/proc/&/status|' /sys/fs/cgroup/$cg/cgroup.threads) 2>/dev/null"
            + " | awk -F'[/:]' -v n=\"$name\" '{gsub(/[ \\t]/, \"\", $6); if ($5 == \"voluntary_ctxt_switches\")"
            + " v[$3] = $6; else nv[$3] = $6} END {for (t in v) print \"T,\" n \",\" t \",\" v[t] \",\" nv[t]}'\n"
            + "done < " + TARGETS + "\n";

    /** A container to count: its name in the report and its main process's PID in the Docker engine's host. */
    record Target(String name, long pid) {
    }

    private final GenericContainer<?> sidecar;
    private final Path runDirectory;
    private Map<String, String> cgroups = Map.of();
    private boolean counting;

    private PerfStatSidecar(GenericContainer<?> sidecar, Path runDirectory) {
        this.sidecar = sidecar;
        this.runDirectory = runDirectory;
    }

    /** Starts the idle sidecar in the Docker engine's host. */
    @SuppressWarnings("resource")
    static PerfStatSidecar start(Path runDirectory) {
        GenericContainer<?> sidecar = new GenericContainer<>(new ImageFromDockerfile(IMAGE, false)
                .withDockerfileFromBuilder(builder -> builder.from("alpine:3.20")
                        .run("apk add --no-cache perf coreutils").build()))
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withCgroupnsMode("host")
                        .withPidMode("host"))
                .withCommand("sleep", "infinity")
                .waitingFor(Wait.forSuccessfulCommand("true").withStartupTimeout(Duration.ofSeconds(60)));
        sidecar.start();
        return new PerfStatSidecar(sidecar, runDirectory);
    }

    /** The boot ID of the Docker engine host's kernel, which isn't namespaced: equal only on the same kernel. */
    String bootId() throws IOException {
        return exec("cat", "/proc/sys/kernel/random/boot_id").trim();
    }

    /**
     * Starts counting the targets' events, and serving their counters. Returns whether any of their cgroups was
     * found; perf may still be unable to count, see {@link #counting()}.
     */
    boolean count(List<Target> targets) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of("sh", "-c", COUNT_SCRIPT, "sh"));
        targets.forEach(target -> command.add(target.name() + "=" + target.pid()));
        sidecar.execInContainer(command.toArray(String[]::new));
        Map<String, String> found = new LinkedHashMap<>();
        for (String line : exec("cat", TARGETS).lines().toList()) {
            int comma = line.indexOf(',');
            if (comma > 0) {
                found.put(line.substring(0, comma), line.substring(comma + 1));
            }
        }
        cgroups = found;
        counting = !found.isEmpty() && sidecar.execInContainer("test", "-e", START).getExitCode() == 0;
        if (!found.isEmpty() && !counting) {
            System.out.println("perf stat can't count in this Docker engine: "
                    + exec("cat", "/tmp/perf-stat.err").trim());
        }
        return !found.isEmpty();
    }

    /** Whether perf stat is counting the containers' events. */
    boolean counting() {
        return counting;
    }

    /** The Docker engine host's CPU and disk counters, for the {@link HostIoSampler}. */
    HostIoSampler.Source hostSource() {
        return new HostIoSampler.Source() {
            @Override
            public List<String> disks() throws IOException {
                return exec("sh", "-c", DISKS_SCRIPT).lines().filter(line -> !line.isBlank()).toList();
            }

            @Override
            public HostIoSampler.HostFiles read() throws IOException {
                return parseHostFiles(exec("sh", "-c", "cat /proc/stat; echo ---; cat /proc/diskstats").lines()
                        .toList());
            }
        };
    }

    /** The counted containers' cgroup and thread counters, for the {@link ContainerStatsSampler}. */
    ContainerStatsSampler.Source containerSource() {
        return this::readContainers;
    }

    /** Splits the host files' exec output at its {@code ---} line. */
    static HostIoSampler.HostFiles parseHostFiles(List<String> lines) {
        int separator = lines.indexOf("---");
        return separator < 0 ? new HostIoSampler.HostFiles(lines, List.of())
                : new HostIoSampler.HostFiles(lines.subList(0, separator), lines.subList(separator + 1, lines.size()));
    }

    private String exec(String... command) throws IOException {
        try {
            return sidecar.execInContainer(command).getStdout();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /** The containers' cgroup and thread counters, read inside the Docker engine's host. */
    private Map<String, ContainerStatsSampler.Snapshot> readContainers() throws IOException {
        return parseSnapshots(exec("sh", "-c", SNAPSHOT_SCRIPT).lines().toList());
    }

    /** Parses the snapshot script's output into a snapshot per container. */
    static Map<String, ContainerStatsSampler.Snapshot> parseSnapshots(List<String> lines) {
        Map<String, Long> usage = new LinkedHashMap<>();
        Map<String, Map<String, long[]>> threads = new HashMap<>();
        for (String line : lines) {
            String[] fields = line.split(",", -1);
            if (fields[0].equals("U") && fields.length == 3) {
                usage.put(fields[1], fields[2].isEmpty() ? 0 : Long.parseLong(fields[2]));
            } else if (fields[0].equals("T") && fields.length == 5 && !fields[3].isEmpty() && !fields[4].isEmpty()) {
                threads.computeIfAbsent(fields[1], name -> new HashMap<>()).put(fields[2],
                        new long[] {Long.parseLong(fields[3]), Long.parseLong(fields[4])});
            }
        }
        Map<String, ContainerStatsSampler.Snapshot> snapshots = new LinkedHashMap<>();
        usage.forEach((name, micros) -> snapshots.put(name,
                new ContainerStatsSampler.Snapshot(micros, threads.getOrDefault(name, Map.of()))));
        return snapshots;
    }

    /** Converts perf's CSV intervals into one row per container and second, with epoch milliseconds. */
    static List<String> rows(long startEpochMillis, List<String> perfLines, Map<String, String> cgroups) {
        Map<String, String> names = new LinkedHashMap<>();
        cgroups.forEach((name, cgroup) -> names.put(cgroup, name));
        // interval,count,unit,event,cgroup,... grouped by interval and cgroup
        Map<String, Map<String, String>> byRow = new LinkedHashMap<>();
        for (String line : perfLines) {
            String[] fields = line.split(",", -1);
            if (fields.length < 5 || line.startsWith("#")) {
                continue;
            }
            String name = names.get(fields[4]);
            if (name == null) {
                continue;
            }
            long epochMillis = startEpochMillis + Math.round(Double.parseDouble(fields[0]) * 1000);
            String value = fields[1].startsWith("<") ? "" : fields[1];
            byRow.computeIfAbsent(epochMillis + "," + name, key -> new LinkedHashMap<>()).put(fields[3], value);
        }
        List<String> rows = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : byRow.entrySet()) {
            StringBuilder row = new StringBuilder(entry.getKey());
            for (String event : EVENTS.split(",")) {
                row.append(',').append(entry.getValue().getOrDefault(event, ""));
            }
            rows.add(row.toString());
        }
        return rows;
    }

    /** Writes {@code perf-stat.csv} from the sidecar's output, when perf counted, and stops the sidecar. */
    @Override
    public void close() throws IOException {
        try {
            if (counting) {
                Container.ExecResult start = sidecar.execInContainer("cat", START);
                Container.ExecResult output = sidecar.execInContainer("cat", OUTPUT);
                long startEpochMillis = Long.parseLong(start.getStdout().trim());
                if (startEpochMillis < 1_000_000_000_000L) {
                    throw new IOException("perf stat's start time isn't in milliseconds: " + startEpochMillis);
                }
                List<String> lines = new ArrayList<>();
                lines.add(RunReport.PERF_STAT_HEADER);
                lines.addAll(rows(startEpochMillis, output.getStdout().lines().toList(), cgroups));
                Files.write(runDirectory.resolve(FILE_NAME), lines);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sidecar.stop();
        }
    }
}
