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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.RunReport;

/**
 * Samples the host's CPU utilization and its disks' throughput once per second into {@code host-io.csv}: the share of
 * CPU time that is busy and that waits for I/O, and each physical disk's read and write megabytes per second and busy
 * share. All the containers of a run share the host, so these show whether a run is limited by the host's CPUs or by
 * its storage, which the containers' own metrics can't show when the bookies share one disk.
 *
 * <p>The values are deltas of Linux's cumulative counters in {@code /proc/stat} and {@code /proc/diskstats} between
 * samples. The disks are the block devices under {@code /sys/block} that have a {@code device} link, so partitions,
 * device-mapper volumes, loop and RAM devices are left out. A {@link Source} reads the files: {@link LocalSource}
 * on the Linux host of the containers, and {@link PerfStatSidecar} in the Docker engine's host otherwise, such as
 * the Linux VM of Docker Desktop or OrbStack on macOS, whose CPUs and virtual disks are then the ones sampled.
 */
final class HostIoSampler implements AutoCloseable {
    static final String FILE_NAME = RunReport.HOST_IO_FILE;
    private static final long INTERVAL_MILLIS = 1000;
    private static final int SECTOR_BYTES = 512;

    /** One reading of the cumulative counters. */
    record Counters(long epochMillis, long cpuTotal, long cpuIdle, long cpuIowait, Map<String, long[]> disks) {
    }

    /** The lines of {@code /proc/stat} and {@code /proc/diskstats}. */
    record HostFiles(List<String> stat, List<String> diskstats) {
    }

    /** Reads the host's files. */
    interface Source {
        /** The physical disks, by their block device names. */
        List<String> disks() throws IOException;

        HostFiles read() throws IOException;
    }

    /** Reads the files of this host. */
    record LocalSource(Path procfs, Path sysfs) implements Source {
        @Override
        public List<String> disks() {
            return physicalDisks(sysfs);
        }

        @Override
        public HostFiles read() {
            return new HostFiles(lines(procfs.resolve("stat")), lines(procfs.resolve("diskstats")));
        }
    }

    private final Source source;
    private final List<String> disks;
    private final BufferedWriter writer;
    private final ScheduledExecutorService executor;
    private Counters previous;

    private HostIoSampler(Source source, List<String> disks, BufferedWriter writer) {
        this.source = source;
        this.disks = disks;
        this.writer = writer;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "host-io-sampler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts sampling into {@code runDirectory}, or returns {@code null} when the host has no such counters. */
    static HostIoSampler start(Source source, Path runDirectory) throws IOException {
        List<String> disks = source.disks();
        if (parse(source.read(), System.currentTimeMillis(), disks) == null) {
            return null;
        }
        BufferedWriter writer = Files.newBufferedWriter(runDirectory.resolve(FILE_NAME));
        writer.write(header(disks));
        writer.newLine();
        HostIoSampler sampler = new HostIoSampler(source, disks, writer);
        sampler.executor.scheduleAtFixedRate(sampler::sample, 0, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return sampler;
    }

    static String header(List<String> disks) {
        StringBuilder header = new StringBuilder("epochMs,cpuBusyPercent,cpuIowaitPercent");
        for (String disk : disks) {
            header.append(',').append(disk).append("ReadMBps,").append(disk).append("WriteMBps,")
                    .append(disk).append("BusyPercent");
        }
        return header.toString();
    }

    /** The block devices that are disks: those with a {@code device} link under {@code /sys/block}. */
    static List<String> physicalDisks(Path sysfs) {
        Path block = sysfs.resolve("block");
        if (!Files.isDirectory(block)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(block)) {
            return entries.filter(entry -> Files.exists(entry.resolve("device")))
                    .map(entry -> entry.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    /** Reads the counters, or returns {@code null} when {@code /proc/stat} has no CPU line. */
    Counters read(long epochMillis) throws IOException {
        return parse(source.read(), epochMillis, disks);
    }

    /** The counters in the host's files, or {@code null} when {@code /proc/stat} has no CPU line. */
    static Counters parse(HostFiles files, long epochMillis, List<String> disks) {
        String cpu = files.stat().stream().filter(line -> line.startsWith("cpu ")).findFirst().orElse(null);
        if (cpu == null) {
            return null;
        }
        String[] fields = cpu.trim().split("\\s+");
        long total = 0;
        // user nice system idle iowait irq softirq steal; guest time is already included in user and nice
        for (int i = 1; i <= Math.min(8, fields.length - 1); i++) {
            total += Long.parseLong(fields[i]);
        }
        long idle = fields.length > 4 ? Long.parseLong(fields[4]) : 0;
        long iowait = fields.length > 5 ? Long.parseLong(fields[5]) : 0;
        Map<String, long[]> diskCounters = new LinkedHashMap<>();
        for (String line : files.diskstats()) {
            String[] d = line.trim().split("\\s+");
            // major minor name reads merged sectorsRead msRead writes merged sectorsWritten msWrite inFlight msIo ...
            if (d.length >= 13 && disks.contains(d[2])) {
                diskCounters.put(d[2], new long[] {Long.parseLong(d[5]), Long.parseLong(d[9]),
                        Long.parseLong(d[12])});
            }
        }
        return new Counters(epochMillis, total, idle, iowait, diskCounters);
    }

    /** One CSV row from two readings, or {@code null} for the first reading. */
    static String row(Counters previous, Counters current, List<String> disks) {
        if (previous == null) {
            return null;
        }
        double seconds = Math.max(1, current.epochMillis() - previous.epochMillis()) / 1000.0;
        long total = Math.max(1, current.cpuTotal() - previous.cpuTotal());
        long idle = current.cpuIdle() - previous.cpuIdle();
        long iowait = current.cpuIowait() - previous.cpuIowait();
        StringBuilder row = new StringBuilder().append(current.epochMillis()).append(',')
                .append(percent(total - idle - iowait, total)).append(',').append(percent(iowait, total));
        for (String disk : disks) {
            long[] before = previous.disks().get(disk);
            long[] after = current.disks().get(disk);
            if (before == null || after == null) {
                row.append(",,,");
                continue;
            }
            row.append(',').append(megabytes(after[0] - before[0], seconds))
                    .append(',').append(megabytes(after[1] - before[1], seconds))
                    .append(',').append(percent(after[2] - before[2], (long) (seconds * 1000)));
        }
        return row.toString();
    }

    private void sample() {
        try {
            Counters current = read(System.currentTimeMillis());
            String row = row(previous, current, disks);
            previous = current;
            if (row != null) {
                writer.write(row);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException | RuntimeException e) {
            System.out.println("Host I/O sample failed: " + e);
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

    private static String percent(long part, long whole) {
        return String.format(Locale.ROOT, "%.1f", Math.min(100.0, Math.max(0.0, 100.0 * part / Math.max(1, whole))));
    }

    private static String megabytes(long sectors, double seconds) {
        return String.format(Locale.ROOT, "%.1f", sectors * (double) SECTOR_BYTES / 1_000_000 / seconds);
    }

    private static List<String> lines(Path file) {
        try {
            return Files.readAllLines(file);
        } catch (IOException e) {
            return new ArrayList<>();
        }
    }
}
