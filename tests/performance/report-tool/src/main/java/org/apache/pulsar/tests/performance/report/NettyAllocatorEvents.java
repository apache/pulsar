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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Summarizes the JFR events of Netty's buffer allocators ({@code io.netty.AllocateBuffer}, {@code FreeBuffer},
 * {@code ReallocateBuffer}, {@code AllocateChunk}, {@code FreeChunk} and {@code ReturnChunk}) in a measurement
 * recording: how many buffers each allocator handed out and of which sizes, how many of them needed a chunk of their
 * own instead of pooled memory, how often buffers grew, the chunks allocated and freed, and the threads that
 * allocated. The summary is written as JSON beside the recording, and the profile report renders it.
 *
 * <p>Profiled components record the events with the JFR configuration {@code tests/performance/jfr/pulsar.jfc}, except
 * the events of every buffer allocation and free, which a component records when its {@code jfrConfigurations} list
 * {@code tests/performance/jfr/netty-allocations.jfc}.
 */
@Command(name = "netty-allocator-events", mixinStandardHelpOptions = true,
        description = "Summarize the Netty allocator events of a JFR recording into <recording>"
                + NettyAllocatorEvents.OUTPUT_SUFFIX + " and print the summary as Markdown")
public final class NettyAllocatorEvents implements Callable<Integer> {
    public static final String OUTPUT_SUFFIX = ".netty-allocator.json";
    static final String EVENT_PREFIX = "io.netty.";
    static final String ALLOCATE_BUFFER = "io.netty.AllocateBuffer";
    static final String REALLOCATE_BUFFER = "io.netty.ReallocateBuffer";
    static final String FREE_BUFFER = "io.netty.FreeBuffer";
    static final String ALLOCATE_CHUNK = "io.netty.AllocateChunk";
    static final String FREE_CHUNK = "io.netty.FreeChunk";
    // Size classes of the buffer size distribution, in bytes: 64 B up to 1 MiB by powers of four, then larger
    private static final long[] SIZE_CLASSES = {64, 256, 1024, 4096, 16384, 65536, 262144, 1048576};
    private static final int TOP_THREAD_POOLS = 15;
    // The numbers of a thread and of its pool at the end of its name, such as the -3-7 of pulsar-io-3-7
    private static final Pattern THREAD_NUMBERS = Pattern.compile("(-\\d+)+$");

    @Parameters(index = "0", description = "The JFR recording, such as a run's *.measurement.jfr")
    private Path recording;

    @Option(names = "--messages", description = "The messages measured in the recording's period, for the events per"
            + " message")
    private long messages;

    private NettyAllocatorEvents() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new NettyAllocatorEvents()).execute(args));
    }

    @Override
    public Integer call() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Path output = write(recording, null, messages, mapper);
        if (output == null) {
            System.out.println("No Netty allocator events in " + recording);
            return 1;
        }
        StringBuilder markdown = new StringBuilder();
        appendReport(markdown, mapper.readTree(output.toFile()), output.getFileName().toString());
        System.out.print(markdown);
        return 0;
    }

    /** Event counts and the bytes of their buffers or chunks. */
    static final class Totals {
        long count;
        long bytes;
        long oneOff;

        void add(long size, boolean oneOffChunk) {
            count++;
            bytes += size;
            if (oneOffChunk) {
                oneOff++;
            }
        }
    }

    /**
     * Summarizes {@code recording}'s Netty allocator events into {@code <recording>.netty-allocator.json} beside it.
     *
     * @param window the measurement period, or null to take it from the first and last allocator event
     * @param messages the messages measured in the period, or 0 when unknown
     * @return the summary file, or null when the recording has no Netty allocator events
     */
    public static Path write(Path recording, Duration window, long messages, ObjectMapper mapper) throws IOException {
        Map<String, Totals> events = new TreeMap<>();
        Map<String, Totals> allocations = new TreeMap<>();
        Map<String, Totals> frees = new TreeMap<>();
        Map<String, Totals> reallocations = new TreeMap<>();
        Map<String, Totals> chunkAllocations = new TreeMap<>();
        Map<String, Totals> chunkFrees = new TreeMap<>();
        Totals[] sizes = new Totals[SIZE_CLASSES.length + 1];
        for (int index = 0; index < sizes.length; index++) {
            sizes[index] = new Totals();
        }
        Map<String, Totals> threadPools = new TreeMap<>();
        Instant first = null;
        Instant last = null;
        try (RecordingFile file = new RecordingFile(recording)) {
            while (file.hasMoreEvents()) {
                RecordedEvent event = file.readEvent();
                String name = event.getEventType().getName();
                if (!name.startsWith(EVENT_PREFIX)) {
                    continue;
                }
                Instant time = event.getStartTime();
                first = first == null || time.isBefore(first) ? time : first;
                last = last == null || time.isAfter(last) ? time : last;
                String allocator = allocator(event);
                String memory = flag(event, "direct") ? "direct" : "heap";
                switch (name) {
                    case ALLOCATE_BUFFER -> {
                        long size = number(event, "size");
                        // A buffer whose chunk isn't pooled got memory of its own, which is allocated and freed with it
                        boolean oneOff = event.hasField("chunkPooled") && !event.getBoolean("chunkPooled");
                        events.computeIfAbsent(name, key -> new Totals()).add(size, oneOff);
                        allocations.computeIfAbsent(String.join("|", allocator, memory,
                                oneOff ? "one-off" : "pooled",
                                flag(event, "chunkThreadLocal") ? "thread-local" : "shared"),
                                key -> new Totals()).add(size, oneOff);
                        sizes[sizeClass(size)].add(size, oneOff);
                        threadPools.computeIfAbsent(threadPool(event.getThread()), key -> new Totals())
                                .add(size, oneOff);
                    }
                    case REALLOCATE_BUFFER -> {
                        long size = number(event, "newCapacity");
                        events.computeIfAbsent(name, key -> new Totals()).add(size, false);
                        reallocations.computeIfAbsent(String.join("|", allocator, memory), key -> new Totals())
                                .add(size, false);
                    }
                    case FREE_BUFFER -> {
                        long size = number(event, "size");
                        events.computeIfAbsent(name, key -> new Totals()).add(size, false);
                        frees.computeIfAbsent(String.join("|", allocator, memory), key -> new Totals())
                                .add(size, false);
                    }
                    case ALLOCATE_CHUNK -> {
                        long size = number(event, "capacity");
                        boolean oneOff = event.hasField("pooled") && !event.getBoolean("pooled");
                        events.computeIfAbsent(name, key -> new Totals()).add(size, oneOff);
                        chunkAllocations.computeIfAbsent(String.join("|", allocator, memory,
                                oneOff ? "one-off" : "pooled",
                                flag(event, "threadLocal") ? "thread-local" : "shared"),
                                key -> new Totals()).add(size, oneOff);
                    }
                    case FREE_CHUNK -> {
                        long size = number(event, "capacity");
                        boolean oneOff = event.hasField("pooled") && !event.getBoolean("pooled");
                        events.computeIfAbsent(name, key -> new Totals()).add(size, oneOff);
                        chunkFrees.computeIfAbsent(String.join("|", allocator, memory,
                                oneOff ? "one-off" : "pooled"), key -> new Totals()).add(size, oneOff);
                    }
                    // Events that a newer Netty adds, such as io.netty.ReturnChunk, are counted
                    default -> events.computeIfAbsent(name, key -> new Totals())
                            .add(event.hasField("capacity") ? number(event, "capacity") : 0, false);
                }
            }
        }
        if (events.isEmpty()) {
            return null;
        }
        double seconds = window != null ? window.toMillis() / 1000.0
                : Math.max(Duration.between(first, last).toMillis() / 1000.0, 0.001);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("recording", recording.getFileName().toString());
        summary.put("seconds", seconds);
        summary.put("messages", messages);
        summary.put("events", rows(events, List.of("event")));
        summary.put("bufferAllocations", rows(allocations, List.of("allocator", "memory", "chunk", "cache")));
        summary.put("bufferFrees", rows(frees, List.of("allocator", "memory")));
        summary.put("bufferReallocations", rows(reallocations, List.of("allocator", "memory")));
        List<Map<String, Object>> sizeRows = new ArrayList<>();
        for (int index = 0; index < sizes.length; index++) {
            if (sizes[index].count == 0) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("upToBytes", index < SIZE_CLASSES.length ? SIZE_CLASSES[index] : null);
            putTotals(row, sizes[index]);
            sizeRows.add(row);
        }
        summary.put("bufferSizes", sizeRows);
        summary.put("chunkAllocations", rows(chunkAllocations, List.of("allocator", "memory", "chunk", "cache")));
        summary.put("chunkFrees", rows(chunkFrees, List.of("allocator", "memory", "chunk")));
        List<Map<String, Object>> pools = rows(threadPools, List.of("threadPool"));
        pools.sort(Comparator.comparing((Map<String, Object> row) -> (Long) row.get("count")).reversed());
        summary.put("bufferAllocationsByThreadPool", pools);
        Path output = recording.resolveSibling(baseName(recording) + OUTPUT_SUFFIX);
        mapper.copy().enable(SerializationFeature.INDENT_OUTPUT).writeValue(output.toFile(), summary);
        return output;
    }

    private static List<Map<String, Object>> rows(Map<String, Totals> totals, List<String> keys) {
        List<Map<String, Object>> rows = new ArrayList<>();
        totals.forEach((key, value) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            String[] parts = key.split("\\|", -1);
            for (int index = 0; index < keys.size(); index++) {
                row.put(keys.get(index), parts[index]);
            }
            putTotals(row, value);
            rows.add(row);
        });
        return rows;
    }

    private static void putTotals(Map<String, Object> row, Totals totals) {
        row.put("count", totals.count);
        row.put("bytes", totals.bytes);
        row.put("oneOff", totals.oneOff);
    }

    static int sizeClass(long size) {
        for (int index = 0; index < SIZE_CLASSES.length; index++) {
            if (size <= SIZE_CLASSES[index]) {
                return index;
            }
        }
        return SIZE_CLASSES.length;
    }

    /** A thread's pool, its name without the numbers of the thread and its pool: pulsar-io-3-7 is pulsar-io. */
    static String threadPool(RecordedThread thread) {
        if (thread == null) {
            return "(unknown)";
        }
        String name = thread.getJavaName() != null ? thread.getJavaName() : thread.getOSName();
        if (name == null || name.isEmpty()) {
            return "(unknown)";
        }
        String pool = THREAD_NUMBERS.matcher(name).replaceFirst("");
        return pool.isEmpty() ? name : pool;
    }

    private static String allocator(RecordedEvent event) {
        if (!event.hasField("allocatorType")) {
            return "(unknown)";
        }
        RecordedClass type = event.getClass("allocatorType");
        if (type == null) {
            return "(unknown)";
        }
        String name = type.getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }

    private static boolean flag(RecordedEvent event, String field) {
        return event.hasField(field) && event.getBoolean(field);
    }

    private static long number(RecordedEvent event, String field) {
        return event.hasField(field) ? event.getLong(field) : 0;
    }

    private static String baseName(Path recording) {
        String name = recording.getFileName().toString();
        return name.endsWith(".jfr") ? name.substring(0, name.length() - ".jfr".length()) : name;
    }

    /** The summary file of {@code recording}'s measurement recording, which may not exist. */
    static Path summaryFile(Path directory, String recordingBase) {
        return directory.resolve(recordingBase + ".measurement" + OUTPUT_SUFFIX);
    }

    /**
     * Appends the summary as the profile report's section on Netty's allocators.
     *
     * @param file the summary's file name, which the section links to
     */
    static void appendReport(StringBuilder report, JsonNode summary, String file) {
        double seconds = summary.path("seconds").asDouble();
        long messages = summary.path("messages").asLong();
        boolean bufferEvents = false;
        for (JsonNode row : summary.path("events")) {
            bufferEvents |= ALLOCATE_BUFFER.equals(row.path("event").asText());
        }
        report.append("\n### Netty allocator events\n\n")
                .append("Netty's buffer allocators recorded an event for every chunk of memory they allocated and")
                .append(" freed to hand out buffers from, and for every buffer they grew")
                .append(bufferEvents ? ", allocated and freed" : "").append(", in the")
                .append(String.format(Locale.ROOT, " %.1f s measurement", seconds))
                .append(messages > 0 ? String.format(Locale.ROOT, " of %,d messages", messages) : "")
                .append(" ([the summary](").append(file).append(")). A one-off chunk is the memory of a single")
                .append(" buffer that didn't fit the pooled memory, allocated and freed with it, so such a buffer")
                .append(" costs much more than a pooled one; reallocations grow a buffer by copying it into a larger")
                .append(" one. Chunk allocations while the load is steady show the pools growing or churning.")
                .append(bufferEvents ? "" : " The events of every buffer allocation and free,"
                        + " `io.netty.AllocateBuffer` and `io.netty.FreeBuffer`, weren't recorded: there is one for"
                        + " every buffer, so they are recorded only when the component's `jfrConfigurations` list"
                        + " `netty-allocations.jfc`, such as with"
                        + " `--extends configs/profile-broker-netty-allocations`.")
                .append("\n\n");
        report.append("| Event | Events | Per second |").append(messages > 0 ? " Per message |" : "")
                .append(" MB | One-off |\n|---|---:|---:|").append(messages > 0 ? "---:|" : "")
                .append("---:|---:|\n");
        for (JsonNode row : summary.path("events")) {
            long count = row.path("count").asLong();
            report.append("| `").append(row.path("event").asText()).append("` | ")
                    .append(String.format(Locale.ROOT, "%,d | %,.0f |", count, count / seconds))
                    .append(messages > 0 ? String.format(Locale.ROOT, " %,.3f |", (double) count / messages) : "")
                    .append(String.format(Locale.ROOT, " %,.1f | %,d |%n", megabytes(row), row.path("oneOff")
                            .asLong()));
        }
        appendTable(report, "Buffer allocations", summary.path("bufferAllocations"),
                List.of("allocator", "memory", "chunk", "cache"),
                List.of("Allocator", "Memory", "Chunk", "Thread-local"), seconds, messages);
        appendSizes(report, summary.path("bufferSizes"));
        appendTable(report, "Buffer reallocations (bytes: the new capacities)", summary.path("bufferReallocations"),
                List.of("allocator", "memory"), List.of("Allocator", "Memory"), seconds, messages);
        appendTable(report, "Chunk allocations", summary.path("chunkAllocations"),
                List.of("allocator", "memory", "chunk", "cache"),
                List.of("Allocator", "Memory", "Chunk", "Thread-local"), seconds, messages);
        appendTable(report, "Chunk frees", summary.path("chunkFrees"), List.of("allocator", "memory", "chunk"),
                List.of("Allocator", "Memory", "Chunk"), seconds, messages);
        List<JsonNode> pools = new ArrayList<>();
        summary.path("bufferAllocationsByThreadPool").forEach(pools::add);
        if (!pools.isEmpty()) {
            JsonNode top = new ObjectMapper().valueToTree(pools.subList(0, Math.min(TOP_THREAD_POOLS, pools.size())));
            appendTable(report, "Buffer allocations by thread pool" + (pools.size() > TOP_THREAD_POOLS
                            ? ", the " + TOP_THREAD_POOLS + " with the most" : ""), top,
                    List.of("threadPool"), List.of("Thread pool"), seconds, messages);
        }
    }

    private static void appendTable(StringBuilder report, String title, JsonNode rows, List<String> keys,
                                    List<String> headers, double seconds, long messages) {
        if (!rows.isArray() || rows.isEmpty()) {
            return;
        }
        report.append("\n**").append(title).append("**\n\n| ").append(String.join(" | ", headers))
                .append(" | Count | Per second |").append(messages > 0 ? " Per message |" : "")
                .append(" MB | Mean bytes |\n|").append("---|".repeat(headers.size())).append("---:|---:|")
                .append(messages > 0 ? "---:|" : "").append("---:|---:|\n");
        for (JsonNode row : rows) {
            long count = row.path("count").asLong();
            report.append("| ");
            for (String key : keys) {
                report.append(row.path(key).asText()).append(" | ");
            }
            report.append(String.format(Locale.ROOT, "%,d | %,.0f |", count, count / seconds))
                    .append(messages > 0 ? String.format(Locale.ROOT, " %,.3f |", (double) count / messages) : "")
                    .append(String.format(Locale.ROOT, " %,.1f | %,.0f |%n", megabytes(row),
                            count > 0 ? row.path("bytes").asDouble() / count : 0));
        }
    }

    private static void appendSizes(StringBuilder report, JsonNode rows) {
        if (!rows.isArray() || rows.isEmpty()) {
            return;
        }
        long total = 0;
        for (JsonNode row : rows) {
            total += row.path("count").asLong();
        }
        report.append("\n**Buffer allocations by size**\n\n| Size | Allocations | Share | MB | One-off |\n")
                .append("|---|---:|---:|---:|---:|\n");
        for (JsonNode row : rows) {
            JsonNode upTo = row.path("upToBytes");
            String size = upTo.isNull() || upTo.isMissingNode() ? "over 1 MiB"
                    : "up to " + bytes(upTo.asLong());
            long count = row.path("count").asLong();
            report.append("| ").append(size).append(" | ")
                    .append(String.format(Locale.ROOT, "%,d | %.1f %% | %,.1f | %,d |%n", count,
                            total > 0 ? count * 100.0 / total : 0, megabytes(row), row.path("oneOff").asLong()));
        }
    }

    private static String bytes(long value) {
        if (value >= 1048576) {
            return value / 1048576 + " MiB";
        }
        return value >= 1024 ? value / 1024 + " KiB" : value + " B";
    }

    private static double megabytes(JsonNode row) {
        return row.path("bytes").asDouble() / 1e6;
    }
}
