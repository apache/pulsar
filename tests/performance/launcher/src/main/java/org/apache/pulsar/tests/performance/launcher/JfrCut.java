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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Creates a smaller JFR recording for a selected interval of a longer recording.
 *
 * <p>Performance recordings commonly start before clients, connections and application code have finished warming
 * up. Startup, class loading, JIT compilation and warmup traffic can then dominate a short profile even when the
 * experiment reports throughput for a later steady measurement interval. Cutting the JFR to the same interval makes
 * CPU, allocation and lock profiles correspond to the reported measurement and makes profiles from separate runs
 * easier to compare.
 *
 * <p>The cutter keeps the source JFR chunk headers, including the actual recording start, end and duration. It also
 * retains one-time JVM and host configuration events that normally occur before the selected interval. The result can
 * therefore still be interpreted in JDK Mission Control with the JVM flags, runtime configuration and machine details
 * that produced the measured events. Keeping the complete source recording alongside the cut recording remains useful
 * when investigating startup or shutdown behavior.
 *
 * <p>Each chunk of the recording is cut on its own, with the times its own header states, and the cut chunks are
 * joined in order. JDK 22+ readers of a whole recording instead convert every chunk with the first chunk's clock, so
 * a chunk whose clock has another origin, such as the one that async-profiler's {@code jfrsync} appends when its
 * clock isn't aligned with the JVM's, keeps its events in the cut but still reads wrongly in those readers.
 * {@link #clockMismatches(Path, Duration)} finds such chunks.
 *
 * <p>Boundaries can be supplied as absolute {@link Instant} values through {@link #cut(Path, Instant, Instant, Path)}.
 * {@link #cutFrom(Path, Instant, Path)} removes only the prefix before a boundary, which is appropriate when work
 * triggered during a measurement can finish later. {@link #cutUsingTimeExpressions(Path, String, String, Path)}
 * additionally accepts ISO-8601 timestamps, epoch milliseconds and offsets from the recording start such as
 * {@code 5s}. A missing boundary selects the corresponding beginning or end of the recording.
 * {@link #recordingInfo(Path)} reads the source timestamps directly from its JFR chunk headers.
 *
 * <p>Applications can call these methods directly. For one-off use, invoke this class through the {@code runJfrCut}
 * Gradle task.
 */
@Command(name = "jfr-cut", mixinStandardHelpOptions = true,
        description = "Write events overlapping a time interval to a new JFR recording")
public final class JfrCut implements Callable<Integer> {
    private static final int JFR_CHUNK_HEADER_SIZE = 68;
    private static final byte[] JFR_MAGIC = {'F', 'L', 'R', 0};
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final Set<String> JVM_CONTEXT_EVENTS = Set.of(
            "jdk.ActiveRecording",
            "jdk.ActiveSetting",
            "jdk.BooleanFlag",
            "jdk.CodeCacheConfiguration",
            "jdk.CompilerConfiguration",
            "jdk.ContainerConfiguration",
            "jdk.CPUInformation",
            "jdk.CPUTimeStampCounter",
            "jdk.DoubleFlag",
            "jdk.GCConfiguration",
            "jdk.GCHeapConfiguration",
            "jdk.GCSurvivorConfiguration",
            "jdk.GCTLABConfiguration",
            "jdk.InitialEnvironmentVariable",
            "jdk.InitialSecurityProperty",
            "jdk.InitialSystemProperty",
            "jdk.IntFlag",
            "jdk.JVMInformation",
            "jdk.LongFlag",
            "jdk.NativeAgent",
            "jdk.NativeLibrary",
            "jdk.OSInformation",
            "jdk.PhysicalMemory",
            "jdk.StringFlag",
            "jdk.SwapSpace",
            "jdk.SystemProcess",
            "jdk.UnsignedIntFlag",
            "jdk.UnsignedLongFlag",
            "jdk.VirtualizationInformation",
            "jdk.YoungGenerationConfiguration");

    @Option(names = "--input", required = true, description = "Source JFR recording")
    private Path input;

    @Option(names = "--output", description = "Output JFR; defaults to <input>.cut.jfr")
    private Path output;

    @Option(names = {"--from", "--start"},
            description = "Inclusive ISO-8601 instant, epoch milliseconds, or offset such as 5s")
    private String from;

    @Option(names = {"--to", "--end"},
            description = "Exclusive ISO-8601 instant, epoch milliseconds, or offset such as 5s")
    private String to;

    @Option(names = "--info", description = "Show the recording start, end and total duration")
    private boolean showInfo;

    private JfrCut() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new JfrCut()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        RecordingInfo info = null;
        if (showInfo) {
            info = recordingInfo(input);
            System.out.println("input=" + input.toAbsolutePath().normalize());
            System.out.println("start=" + info.start());
            System.out.println("end=" + info.end());
            System.out.println("duration=" + info.duration());
            System.out.println("durationMillis=" + info.duration().toMillis());
        }
        if (from == null && to == null && !showInfo) {
            throw new IllegalArgumentException("Specify at least one JFR cut boundary");
        }
        if (from != null || to != null) {
            cutUsingTimeExpressions(input, from, to, output != null ? output : defaultOutput(input), info);
        }
        return 0;
    }

    /**
     * Resolves command-line time expressions and cuts a recording without invoking {@link #main(String[])}.
     * Relative values such as {@code 5s} are measured from the recording start. A {@code null} start
     * or end selects the corresponding recording boundary.
     */
    public static void cutUsingTimeExpressions(Path input, String from, String to, Path output) throws IOException {
        cutUsingTimeExpressions(input, from, to, output, null);
    }

    private static void cutUsingTimeExpressions(Path input, String from, String to, Path output,
                                                RecordingInfo existingInfo) throws IOException {
        RecordingInfo info = existingInfo != null ? existingInfo : recordingInfo(input);
        Instant resolvedFrom = from != null ? parseTimeExpression(from, info.start()) : info.start();
        Instant resolvedTo = to != null ? parseTimeExpression(to, info.start()) : info.exclusiveEnd();
        cut(input, resolvedFrom, resolvedTo, output);
    }

    /**
     * Writes events that overlap the half-open interval {@code [from, to)}. One-time JVM, host, recording setting,
     * and runtime configuration events are retained even when they precede the interval so that tools such as JDK
     * Mission Control can describe the source JVM.
     *
     * @param input source JFR recording
     * @param from inclusive start of the selected interval
     * @param to exclusive end of the selected interval
     * @param output destination JFR recording, which must differ from {@code input}
     */
    public static void cut(Path input, Instant from, Instant to, Path output) throws IOException {
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("JFR cut start must be before its end");
        }
        cut(input, output, event -> isJvmContextEvent(event)
                || event.getStartTime().isBefore(to)
                && (event.getEndTime().isAfter(from) || event.getStartTime().equals(from)));
    }

    /**
     * Removes duration events ending at or before {@code from}, retaining instantaneous events at the boundary.
     * One-time JVM, host, recording setting, and runtime configuration events are retained even when they precede
     * the boundary.
     * This is useful for removing benchmark startup and warmup without discarding asynchronous work that finishes
     * after the measured producer activity has ended.
     *
     * @param input source JFR recording
     * @param from inclusive start boundary
     * @param output destination JFR recording, which must differ from {@code input}
     */
    public static void cutFrom(Path input, Instant from, Path output) throws IOException {
        cut(input, output, event -> isJvmContextEvent(event)
                || event.getEndTime().isAfter(from) || event.getStartTime().equals(from));
    }

    private static void cut(Path input, Path output, Predicate<RecordedEvent> filter) throws IOException {
        Path normalizedInput = input.toAbsolutePath().normalize();
        Path normalizedOutput = output.toAbsolutePath().normalize();
        if (normalizedInput.equals(normalizedOutput)) {
            throw new IllegalArgumentException("JFR cut output must differ from its input");
        }

        Path temporary = normalizedOutput.resolveSibling(normalizedOutput.getFileName() + ".tmp");
        Files.deleteIfExists(temporary);
        List<Chunk> chunks = chunks(normalizedInput);
        if (chunks.size() == 1) {
            try (RecordingFile recording = new RecordingFile(normalizedInput)) {
                write(recording, temporary, filter);
            }
        } else {
            cutChunks(normalizedInput, chunks, temporary, filter);
        }
        try {
            Files.move(temporary, normalizedOutput, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, normalizedOutput, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    // JDK 22+ readers convert the events of every chunk with the first chunk's clock, which mistimes the events of a
    // chunk whose clock has another origin, such as the one that async-profiler's jfrsync appends when its clock isn't
    // aligned with the JVM's. Each chunk is complete in itself, so each is cut on its own, timed by its own header.
    private static void cutChunks(Path input, List<Chunk> chunks, Path output, Predicate<RecordedEvent> filter)
            throws IOException {
        Path chunkInput = output.resolveSibling(output.getFileName() + ".chunk");
        Path chunkOutput = output.resolveSibling(output.getFileName() + ".chunk.cut");
        try (FileChannel source = FileChannel.open(input, StandardOpenOption.READ);
             FileChannel target = FileChannel.open(output, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (Chunk chunk : chunks) {
                try (FileChannel chunkFile = FileChannel.open(chunkInput, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                    transferFully(source, chunk.offset(), chunk.size(), chunkFile);
                }
                Files.deleteIfExists(chunkOutput);
                try (RecordingFile recording = new RecordingFile(chunkInput)) {
                    write(recording, chunkOutput, filter);
                }
                try (FileChannel cutChunk = FileChannel.open(chunkOutput, StandardOpenOption.READ)) {
                    transferFully(cutChunk, 0, cutChunk.size(), target);
                }
            }
        } finally {
            Files.deleteIfExists(chunkInput);
            Files.deleteIfExists(chunkOutput);
        }
    }

    private static void transferFully(FileChannel source, long position, long size, FileChannel target)
            throws IOException {
        long transferred = 0;
        while (transferred < size) {
            long count = source.transferTo(position + transferred, size - transferred, target);
            if (count <= 0) {
                throw new IOException("Could not copy a JFR chunk");
            }
            transferred += count;
        }
    }

    /**
     * Finds the chunks of a recording whose events a JDK 22+ reader of the whole file mistimes by more than
     * {@code tolerance}. Such a reader converts every chunk's ticks with the first chunk's start and frequency, so
     * this compares the start and the end of each later chunk, converted that way, with the times its own header
     * states. Cutting chunk by chunk keeps the right events regardless, but the whole recording and its cut still
     * read wrongly in such readers.
     */
    public static List<ClockMismatch> clockMismatches(Path input, Duration tolerance) throws IOException {
        List<Chunk> chunks = chunks(input.toAbsolutePath().normalize());
        Chunk first = chunks.get(0);
        List<ClockMismatch> mismatches = new ArrayList<>();
        for (int i = 1; i < chunks.size(); i++) {
            Chunk chunk = chunks.get(i);
            BigInteger endTicks = BigInteger.valueOf(chunk.startTicks()).add(BigInteger.valueOf(chunk.durationNanos())
                    .multiply(BigInteger.valueOf(chunk.ticksPerSecond())).divide(BigInteger.valueOf(NANOS_PER_SECOND)));
            long startError = first.convert(BigInteger.valueOf(chunk.startTicks()))
                    .subtract(BigInteger.valueOf(chunk.startNanos())).abs().min(BigInteger.valueOf(Long.MAX_VALUE))
                    .longValue();
            long endError = first.convert(endTicks)
                    .subtract(BigInteger.valueOf(chunk.startNanos()).add(BigInteger.valueOf(chunk.durationNanos())))
                    .abs().min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
            Duration error = Duration.ofNanos(Math.max(startError, endError));
            if (error.compareTo(tolerance) > 0) {
                mismatches.add(new ClockMismatch(i, error));
            }
        }
        return mismatches;
    }

    /** A chunk, by its index in the recording, and how far a JDK 22+ reader of the whole file mistimes its events. */
    public record ClockMismatch(int chunk, Duration error) {
    }

    private static boolean isJvmContextEvent(RecordedEvent event) {
        return JVM_CONTEXT_EVENTS.contains(event.getEventType().getName());
    }

    static Instant parseInstant(String value) {
        try {
            return Instant.ofEpochMilli(Long.parseLong(value));
        } catch (NumberFormatException notEpochMilliseconds) {
            return Instant.parse(value);
        }
    }

    static Instant parseTimeExpression(String value, Instant recordingStart) {
        if (value.matches("[0-9]+(ms|s|m|h)")) {
            int unitStart = 0;
            while (unitStart < value.length() && Character.isDigit(value.charAt(unitStart))) {
                unitStart++;
            }
            long amount = Long.parseLong(value.substring(0, unitStart));
            Duration offset = switch (value.substring(unitStart)) {
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                case "m" -> Duration.ofMinutes(amount);
                case "h" -> Duration.ofHours(amount);
                default -> throw new IllegalArgumentException("Unsupported relative time unit in " + value);
            };
            return recordingStart.plus(offset);
        }
        if (value.startsWith("P")) {
            return recordingStart.plus(Duration.parse(value));
        }
        return parseInstant(value);
    }

    static Path defaultOutput(Path input) {
        String name = input.getFileName().toString();
        String basename = name.endsWith(".jfr") ? name.substring(0, name.length() - 4) : name;
        return input.resolveSibling(basename + ".cut.jfr");
    }

    /** Returns the start, end and total duration recorded in the JFR chunk headers. */
    public static RecordingInfo recordingInfo(Path input) throws IOException {
        long firstStartNanos = Long.MAX_VALUE;
        long lastEndNanos = Long.MIN_VALUE;
        for (Chunk chunk : chunks(input.toAbsolutePath().normalize())) {
            final long endNanos;
            try {
                endNanos = Math.addExact(chunk.startNanos(), chunk.durationNanos());
            } catch (ArithmeticException overflow) {
                throw new IOException("JFR chunk timestamp overflow at offset " + chunk.offset() + " in " + input,
                        overflow);
            }
            firstStartNanos = Math.min(firstStartNanos, chunk.startNanos());
            lastEndNanos = Math.max(lastEndNanos, endNanos);
        }
        final long totalDurationNanos;
        try {
            totalDurationNanos = Math.subtractExact(lastEndNanos, firstStartNanos);
        } catch (ArithmeticException overflow) {
            throw new IOException("JFR recording duration overflow in " + input, overflow);
        }
        Instant start = epochNanosToInstant(firstStartNanos);
        Instant end = epochNanosToInstant(lastEndNanos);
        return new RecordingInfo(start, end, Duration.ofNanos(totalDurationNanos));
    }

    // The chunk headers of a recording, which must be complete: a truncated or unfinished chunk is rejected
    private static List<Chunk> chunks(Path input) throws IOException {
        List<Chunk> chunks = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(input, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long chunkOffset = 0;
            while (chunkOffset < fileSize) {
                ByteBuffer header = ByteBuffer.allocate(JFR_CHUNK_HEADER_SIZE);
                readFully(channel, header, chunkOffset);
                header.flip();
                for (byte expected : JFR_MAGIC) {
                    if (header.get() != expected) {
                        throw new IOException("Invalid JFR chunk at offset " + chunkOffset + " in " + input);
                    }
                }
                header.getShort();
                header.getShort();
                long chunkSize = header.getLong();
                header.getLong();
                header.getLong();
                long startNanos = header.getLong();
                long durationNanos = header.getLong();
                long startTicks = header.getLong();
                long ticksPerSecond = header.getLong();
                if (chunkSize < JFR_CHUNK_HEADER_SIZE || chunkSize > fileSize - chunkOffset
                        || durationNanos < 0 || ticksPerSecond <= 0) {
                    throw new IOException("Invalid JFR chunk header at offset " + chunkOffset + " in " + input);
                }
                chunks.add(new Chunk(chunkOffset, chunkSize, startNanos, durationNanos, startTicks, ticksPerSecond));
                chunkOffset += chunkSize;
            }
        }
        if (chunks.isEmpty()) {
            throw new IOException("Cannot inspect an empty JFR recording: " + input);
        }
        return chunks;
    }

    private record Chunk(long offset, long size, long startNanos, long durationNanos, long startTicks,
                         long ticksPerSecond) {
        // The epoch nanoseconds of a tick count, as this chunk's header converts it
        BigInteger convert(BigInteger ticks) {
            return BigInteger.valueOf(startNanos).add(ticks.subtract(BigInteger.valueOf(startTicks))
                    .multiply(BigInteger.valueOf(NANOS_PER_SECOND)).divide(BigInteger.valueOf(ticksPerSecond)));
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer target, long position) throws IOException {
        while (target.hasRemaining()) {
            int read = channel.read(target, position + target.position());
            if (read < 0) {
                throw new IOException("Truncated JFR chunk header at offset " + position);
            }
        }
    }

    private static Instant epochNanosToInstant(long epochNanos) {
        return Instant.ofEpochSecond(Math.floorDiv(epochNanos, NANOS_PER_SECOND),
                Math.floorMod(epochNanos, NANOS_PER_SECOND));
    }

    /** Time range stored in a JFR recording's chunk headers. */
    public record RecordingInfo(Instant start, Instant end, Duration duration) {
        Instant exclusiveEnd() {
            return end.equals(Instant.MAX) ? end : end.plusNanos(1);
        }
    }

    private static void write(RecordingFile recording, Path output, Predicate<RecordedEvent> filter)
            throws IOException {
        final Method writeMethod;
        try {
            writeMethod = RecordingFile.class.getMethod("write", Path.class, Predicate.class);
        } catch (NoSuchMethodException error) {
            throw new IOException("Cutting JFR recordings requires JDK 19 or newer", error);
        }
        try {
            writeMethod.invoke(recording, output, filter);
        } catch (IllegalAccessException error) {
            throw new IOException("Cannot access the public JFR recording writer", error);
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("JFR recording writer failed", cause);
        }
    }
}
