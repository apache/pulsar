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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;

/**
 * Writes heap dumps of the JVMs in a run's containers, as the scenario's {@code heapDumps} section asks: when the
 * gateways start, at given seconds after that, periodically, at the highest heap usage, and after every application
 * has received every message. The dumps are written with {@code jcmd GC.heap_dump} into a directory that each
 * container has mounted from {@code heap-dumps/<component>} in the run directory, one at a time, and each is listed in
 * {@code heap-dumps/heap-dumps.csv}. A JVM that runs out of memory writes its own dump there, with the options of
 * {@link #outOfMemoryOptions(int)}. With a gzip level, the JVMs compress the dumps as they write them, into
 * {@code .hprof.gz} files.
 *
 * <p>A heap dump stops the JVM while it is written, and a dump of the live objects, which {@code GC.heap_dump} writes,
 * runs a full garbage collection first: a run with scheduled heap dumps is for finding what holds the memory, not for
 * measuring.
 */
final class HeapDumper implements AutoCloseable {
    /** The directory of the heap dumps in the run directory. */
    static final String DIRECTORY = "heap-dumps";
    /** Where each container has its component's heap dump directory mounted. */
    static final String MOUNT = "/heap-dumps";
    static final String INDEX_FILE = "heap-dumps.csv";
    static final String INDEX_HEADER = "epochMillis,target,trigger,file,usedBytes,maxBytes,dumpMillis";
    /** How often the heap usage is sampled for {@code atPeakUsage}. */
    static final Duration PEAK_SAMPLE_INTERVAL = Duration.ofSeconds(5);
    /** A peak dump is written only when the heap is at least this full, so that the start doesn't cause a series. */
    static final double PEAK_MINIMUM_FRACTION = 0.5;
    /** A new peak dump replaces the previous one only when the heap usage has grown at least this much. */
    static final double PEAK_GROWTH = 1.1;

    // GC.heap_info's first lines: "ZHeap used 2048M, capacity 2048M, max capacity 2048M" for ZGC, and
    // "garbage-first heap total reserved 524288K, committed 524288K, used 122160K ..." for G1
    private static final Pattern USED = Pattern.compile("\\bused (\\d+)([KMG])");
    private static final Pattern MAXIMUM = Pattern.compile("(?:max capacity|total reserved) (\\d+)([KMG])");

    /**
     * A JVM to dump.
     *
     * @param name the name of its dumps, such as {@code broker-0}
     * @param component the component, which names its directory
     */
    record Target(String name, String component, HeapDumpSettings.Component settings, GenericContainer<?> container) {
    }

    /** The heap usage that {@code GC.heap_info} reports; the maximum is 0 when it can't be read. */
    record HeapUsage(long usedBytes, long maxBytes) {
    }

    private record Jvm(String pid, String uid) {
    }

    // jcmd runs without the container's JAVA_TOOL_OPTIONS: in a profiled workload, they would start the profiling
    // agent in jcmd's own JVM, against the workload's capture files
    private static final Map<String, String> JCMD_ENVIRONMENT = Map.of("JAVA_TOOL_OPTIONS", "");

    private final Path directory;
    private final String image;
    private final int gzipLevel;
    private final String extension;
    private final ScheduledThreadPoolExecutor scheduler = newScheduler();
    private final Map<String, Long> peaks = new HashMap<>();
    private final List<Target> targets = new ArrayList<>();
    private long startNanos;

    /**
     * The dumps' single thread. Its shutdown cancels the dumps that are due later, such as at a time after the end of
     * the workload, and lets a dump that is being written finish.
     */
    static ScheduledThreadPoolExecutor newScheduler() {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "heap-dumps");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    /**
     * @param runDirectory the run directory, which {@link #prepare} prepared
     * @param image the image of the run's containers, which makes the dumps readable at the end
     * @param gzipLevel the gzip level of the dumps, from 1 to 9, or 0 to write them uncompressed
     */
    HeapDumper(Path runDirectory, String image, int gzipLevel) {
        this.directory = runDirectory.resolve(DIRECTORY);
        this.image = image;
        this.gzipLevel = gzipLevel;
        this.extension = gzipLevel > 0 ? ".hprof.gz" : ".hprof";
    }

    /**
     * Creates the heap dump directory of each component in the run directory. They are writable by every user, since
     * the JVMs in the containers run as other users than the launcher on a Linux host.
     *
     * @return the directory of each component, to mount at {@link #MOUNT}
     */
    static Map<String, Path> prepare(Path runDirectory) throws IOException {
        Map<String, Path> directories = new HashMap<>();
        for (String component : List.of(HeapDumpSettings.BROKER, HeapDumpSettings.GATEWAYS,
                HeapDumpSettings.APPLICATIONS)) {
            Path componentDirectory = Files.createDirectories(runDirectory.resolve(DIRECTORY).resolve(component));
            makeWritable(componentDirectory);
            directories.put(component, componentDirectory);
        }
        return directories;
    }

    private static void makeWritable(Path path) {
        try {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxrwxrwx"));
        } catch (UnsupportedOperationException | IOException e) {
            // A file system without POSIX permissions, such as on Windows, has no other users to let in
        }
    }

    /**
     * The JVM options that make a JVM write a heap dump into {@link #MOUNT} when it runs out of memory, named
     * {@code java_pid<pid>.hprof}, or {@code java_pid<pid>.hprof.gz} with a gzip level.
     *
     * @param gzipLevel the gzip level, from 1 to 9, or 0 to write the dump uncompressed
     */
    static String outOfMemoryOptions(int gzipLevel) {
        return "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + MOUNT
                + (gzipLevel > 0 ? " -XX:HeapDumpGzipLevel=" + gzipLevel : "");
    }

    /** Starts the dumps of the targets that the gateways' start begins, and the heap usage sampling. */
    synchronized void start(List<Target> started) {
        startNanos = System.nanoTime();
        for (Target target : started) {
            HeapDumpSettings.Component settings = target.settings();
            if (!settings.scheduled()) {
                continue;
            }
            targets.add(target);
            if (settings.atStart()) {
                scheduler.execute(() -> dump(target, "start"));
            }
            for (int seconds : settings.atSeconds()) {
                scheduler.schedule(() -> dump(target, "at-" + seconds + "s"), seconds, TimeUnit.SECONDS);
            }
            if (settings.everySeconds() > 0) {
                scheduler.scheduleAtFixedRate(() -> dump(target, "periodic-" + elapsedSeconds() + "s"),
                        settings.everySeconds(), settings.everySeconds(), TimeUnit.SECONDS);
            }
            if (settings.atPeakUsage()) {
                scheduler.scheduleWithFixedDelay(() -> samplePeak(target), PEAK_SAMPLE_INTERVAL.toMillis(),
                        PEAK_SAMPLE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Stops the scheduled dumps, lets a dump in progress finish, and writes the {@code atEnd} dumps, after every
     * application has received every message.
     */
    void end() throws InterruptedException {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(10, TimeUnit.MINUTES)) {
            System.out.println("Heap dumps: a dump was still being written after 10 minutes");
        }
        for (Target target : snapshotTargets()) {
            if (target.settings().atEnd()) {
                dump(target, "end");
            }
        }
    }

    private synchronized List<Target> snapshotTargets() {
        return List.copyOf(targets);
    }

    /**
     * Stops any dumps still scheduled, and makes every dump readable by the launcher's user: the JVMs write them
     * readable only by their own user, which in a container isn't the launcher's.
     */
    @Override
    public void close() {
        scheduler.shutdownNow();
        List<Path> dumps;
        try (Stream<Path> files = Files.walk(directory)) {
            dumps = files.filter(file -> isDump(file.getFileName().toString())).toList();
        } catch (IOException e) {
            return;
        }
        if (!dumps.isEmpty()) {
            // Also when the run failed, such as with an OutOfMemoryError, when there is no report to list them
            System.out.println("Heap dumps: " + dumps.size() + " in " + directory);
        }
        if (dumps.stream().allMatch(Files::isReadable)) {
            return;
        }
        // A one-off container of the run's image, as root, changes the permissions of the files the containers wrote
        try (GenericContainer<?> fixer = new GenericContainer<>(image)
                .withFileSystemBind(directory.toString(), MOUNT, BindMode.READ_WRITE)
                .withCreateContainerCmdModifier(command -> command.withUser("0").withEntrypoint("chmod"))
                .withCommand("-R", "a+rwX", MOUNT)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)))) {
            fixer.start();
        } catch (RuntimeException e) {
            System.out.println("Heap dumps: couldn't make the dumps in " + directory + " readable (" + e.getMessage()
                    + "); read them with sudo");
        }
    }

    /** Whether a file is a heap dump, compressed or not. */
    static boolean isDump(String fileName) {
        return fileName.endsWith(".hprof") || fileName.endsWith(".hprof.gz");
    }

    private long elapsedSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
    }

    private void samplePeak(Target target) {
        try {
            Jvm jvm = findJvm(target.container());
            if (jvm == null) {
                return;
            }
            HeapUsage usage = usage(target.container(), jvm);
            if (usage == null || !isNewPeak(usage, peaks.getOrDefault(target.name(), 0L))) {
                return;
            }
            String file = target.name() + "-peak" + extension;
            // Written beside the previous peak dump and moved over it, so that a failed dump keeps the previous one
            String pending = target.name() + "-peak-pending" + extension;
            long start = System.nanoTime();
            if (writeDump(target, jvm, pending)) {
                Path component = directory.resolve(target.component());
                Files.move(component.resolve(pending), component.resolve(file), StandardCopyOption.REPLACE_EXISTING);
                peaks.put(target.name(), usage.usedBytes());
                written(target, "peak", file, usage, start);
            }
        } catch (Exception e) {
            System.out.println("Heap dumps: sampling " + target.name() + " failed: " + e);
        }
    }

    /**
     * Whether a heap usage is a new peak to dump: at least {@link #PEAK_MINIMUM_FRACTION} of the maximum heap, when
     * the maximum is known, and at least {@link #PEAK_GROWTH} times the usage of the previous peak dump.
     */
    static boolean isNewPeak(HeapUsage usage, long previousPeakBytes) {
        if (usage.maxBytes() > 0 && usage.usedBytes() < PEAK_MINIMUM_FRACTION * usage.maxBytes()) {
            return false;
        }
        return usage.usedBytes() >= PEAK_GROWTH * previousPeakBytes;
    }

    private void dump(Target target, String trigger) {
        try {
            Jvm jvm = findJvm(target.container());
            if (jvm == null) {
                return;
            }
            HeapUsage usage = usage(target.container(), jvm);
            String file = target.name() + "-" + trigger + extension;
            long start = System.nanoTime();
            if (writeDump(target, jvm, file)) {
                written(target, trigger, file, usage, start);
            }
        } catch (Exception e) {
            System.out.println("Heap dumps: dumping " + target.name() + " (" + trigger + ") failed: " + e);
        }
    }

    private boolean writeDump(Target target, Jvm jvm, String file) throws IOException, InterruptedException {
        Container.ExecResult result = target.container().execInContainer(jcmd(jvm,
                dumpCommand(jvm.pid(), MOUNT + "/" + file, gzipLevel)));
        if (result.getExitCode() != 0 || !result.getStdout().contains("Heap dump file created")) {
            System.out.println("Heap dumps: jcmd GC.heap_dump of " + target.name() + " failed: "
                    + (result.getStdout() + result.getStderr()).trim());
            return false;
        }
        return true;
    }

    /**
     * The {@code jcmd} command that dumps a JVM's heap into a file, compressed with {@code -gz} at a gzip level: the
     * JVM writes the file under the name it is given, so a compressed dump's name ends in {@code .hprof.gz}.
     */
    static String[] dumpCommand(String pid, String file, int gzipLevel) {
        return gzipLevel > 0 ? new String[] {"jcmd", pid, "GC.heap_dump", "-gz=" + gzipLevel, file}
                : new String[] {"jcmd", pid, "GC.heap_dump", file};
    }

    // Reports a dump on the console and lists it in the index
    private void written(Target target, String trigger, String file, HeapUsage usage, long startNanos)
            throws IOException {
        long dumpMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        System.out.printf(Locale.ROOT, "Heap dump of %s (%s)%s in %.1f s: %s%n", target.name(), trigger,
                usage != null ? String.format(Locale.ROOT, " at %,d MB used", usage.usedBytes() >> 20) : "",
                dumpMillis / 1000.0, directory.resolve(target.component()).resolve(file));
        index(target, trigger, file, usage, dumpMillis);
    }

    private synchronized void index(Target target, String trigger, String file, HeapUsage usage, long dumpMillis)
            throws IOException {
        Path index = directory.resolve(INDEX_FILE);
        if (!Files.exists(index)) {
            Files.writeString(index, INDEX_HEADER + "\n", StandardCharsets.UTF_8);
        }
        Files.writeString(index, String.format(Locale.ROOT, "%d,%s,%s,%s,%d,%d,%d%n", System.currentTimeMillis(),
                target.name(), trigger, target.component() + "/" + file, usage != null ? usage.usedBytes() : -1,
                usage != null ? usage.maxBytes() : -1, dumpMillis), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    // The container's JVM, the oldest java process, with the user it runs as: jcmd has to run as that user
    private static Jvm findJvm(GenericContainer<?> container) throws IOException, InterruptedException {
        if (!container.isRunning()) {
            return null;
        }
        Container.ExecResult result = container.execInContainer("sh", "-c",
                "pid=$(pgrep -o -x java) && echo \"$pid $(stat -c %u /proc/$pid)\"");
        String[] fields = result.getStdout().trim().split("\\s+");
        return result.getExitCode() == 0 && fields.length == 2 ? new Jvm(fields[0], fields[1]) : null;
    }

    // jcmd has to run as the JVM's user
    private static ExecConfig jcmd(Jvm jvm, String[] command) {
        return ExecConfig.builder().user(jvm.uid()).envVars(JCMD_ENVIRONMENT).command(command).build();
    }

    private static HeapUsage usage(GenericContainer<?> container, Jvm jvm) throws IOException, InterruptedException {
        Container.ExecResult result = container.execInContainer(jcmd(jvm,
                new String[] {"jcmd", jvm.pid(), "GC.heap_info"}));
        return result.getExitCode() == 0 ? parseUsage(result.getStdout()) : null;
    }

    /** The heap usage in {@code jcmd GC.heap_info}'s output, or null when it has none. */
    static HeapUsage parseUsage(String heapInfo) {
        Matcher used = USED.matcher(heapInfo);
        if (!used.find()) {
            return null;
        }
        Matcher maximum = MAXIMUM.matcher(heapInfo);
        return new HeapUsage(bytes(used), maximum.find() ? bytes(maximum) : 0);
    }

    private static long bytes(Matcher matcher) {
        long value = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "K" -> value << 10;
            case "M" -> value << 20;
            default -> value << 30;
        };
    }
}
