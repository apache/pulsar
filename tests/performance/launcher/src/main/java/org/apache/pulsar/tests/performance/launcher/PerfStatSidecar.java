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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Counts each container's CPU time, context switches, CPU migrations, cycles and instructions with {@code perf stat}
 * into {@code perf-stat.csv}, once per second. The counts are exact, not sampled, and cost next to nothing, so they
 * suit unprofiled runs: CPUs utilized, context switches and migrations per second, the clock rate the containers'
 * threads ran at, and their instructions per cycle.
 *
 * <p>{@code perf stat} runs in a privileged sidecar container, built once from Alpine's {@code perf} package, in the
 * host's cgroup namespace, and counts every CPU's events per container cgroup ({@code --for-each-cgroup}). Its output
 * stays in the sidecar until the run ends, so that the run directory gets no root-owned files. On a host whose CPU
 * counters aren't available to containers, such as a virtual machine, perf reports them as not supported and their
 * columns are left empty.
 */
final class PerfStatSidecar implements AutoCloseable {
    static final String FILE_NAME = RunReport.PERF_STAT_FILE;
    static final String EVENTS = "task-clock,context-switches,cpu-migrations,cycles,instructions";
    // GNU date from coreutils, for the start time in milliseconds, which BusyBox's date doesn't print
    private static final String IMAGE = "pulsar-performance-perf-stat:alpine-3.20-coreutils";
    private static final String OUTPUT = "/tmp/perf-stat.raw";
    private static final String START = "/tmp/perf-stat.start";

    /** A container to count: its name in the report and its cgroup path relative to the cgroup root. */
    record Target(String name, String cgroup) {
    }

    private final GenericContainer<?> sidecar;
    private final List<Target> targets;
    private final Path runDirectory;

    private PerfStatSidecar(GenericContainer<?> sidecar, List<Target> targets, Path runDirectory) {
        this.sidecar = sidecar;
        this.targets = targets;
        this.runDirectory = runDirectory;
    }

    /** Starts counting the targets' events, or returns {@code null} when there are none. */
    @SuppressWarnings("resource")
    static PerfStatSidecar start(List<Target> targets, Path runDirectory) {
        if (targets.isEmpty()) {
            return null;
        }
        String cgroups = String.join(",", targets.stream().map(Target::cgroup).toList());
        String script = "date +%s%3N > " + START + " && exec perf stat -a -x, -I 1000 -e " + EVENTS
                + " --for-each-cgroup " + cgroups + " -o " + OUTPUT;
        GenericContainer<?> sidecar = new GenericContainer<>(new ImageFromDockerfile(IMAGE, false)
                .withDockerfileFromBuilder(builder -> builder.from("alpine:3.20")
                        .run("apk add --no-cache perf coreutils").build()))
                .withPrivilegedMode(true)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withCgroupnsMode("host"))
                .withCommand("sh", "-c", script)
                .waitingFor(Wait.forSuccessfulCommand("test -s " + START)
                        .withStartupTimeout(Duration.ofSeconds(60)));
        sidecar.start();
        return new PerfStatSidecar(sidecar, targets, runDirectory);
    }

    /** Converts perf's CSV intervals into one row per container and second, with epoch milliseconds. */
    static List<String> rows(long startEpochMillis, List<String> perfLines, List<Target> targets) {
        Map<String, String> names = new LinkedHashMap<>();
        for (Target target : targets) {
            names.put(target.cgroup(), target.name());
        }
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

    /** Writes {@code perf-stat.csv} from the sidecar's output and stops the sidecar. */
    @Override
    public void close() throws IOException {
        try {
            Container.ExecResult start = sidecar.execInContainer("cat", START);
            Container.ExecResult output = sidecar.execInContainer("cat", OUTPUT);
            long startEpochMillis = Long.parseLong(start.getStdout().trim());
            if (startEpochMillis < 1_000_000_000_000L) {
                throw new IOException("perf stat's start time isn't in milliseconds: " + startEpochMillis);
            }
            List<String> rows = rows(startEpochMillis, output.getStdout().lines().toList(), targets);
            List<String> lines = new ArrayList<>();
            lines.add(RunReport.PERF_STAT_HEADER);
            lines.addAll(rows);
            Files.write(runDirectory.resolve(FILE_NAME), lines);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sidecar.stop();
        }
    }
}
