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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
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
 * <p>Boundaries can be supplied as absolute {@link Instant} values through {@link #cut(Path, Instant, Instant, Path)}.
 * {@link #cutUsingTimeExpressions(Path, String, String, Path)} additionally accepts ISO-8601 timestamps, epoch
 * milliseconds and offsets from the recording start such as {@code 5s}. A missing boundary selects the corresponding
 * beginning or end of the recording. {@link #recordingInfo(Path)} reads the source timestamps directly from its JFR
 * chunk headers.
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
        Path normalizedInput = input.toAbsolutePath().normalize();
        Path normalizedOutput = output.toAbsolutePath().normalize();
        if (normalizedInput.equals(normalizedOutput)) {
            throw new IllegalArgumentException("JFR cut output must differ from its input");
        }

        Path temporary = normalizedOutput.resolveSibling(normalizedOutput.getFileName() + ".tmp");
        Files.deleteIfExists(temporary);
        try (RecordingFile recording = new RecordingFile(normalizedInput)) {
            write(recording, temporary, event -> isJvmContextEvent(event)
                    || event.getStartTime().isBefore(to) && !event.getEndTime().isBefore(from));
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
        Path normalizedInput = input.toAbsolutePath().normalize();
        try (FileChannel channel = FileChannel.open(normalizedInput, StandardOpenOption.READ)) {
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
                if (chunkSize < JFR_CHUNK_HEADER_SIZE || chunkSize > fileSize - chunkOffset
                        || durationNanos < 0) {
                    throw new IOException("Invalid JFR chunk header at offset " + chunkOffset + " in " + input);
                }
                final long endNanos;
                try {
                    endNanos = Math.addExact(startNanos, durationNanos);
                } catch (ArithmeticException overflow) {
                    throw new IOException("JFR chunk timestamp overflow at offset " + chunkOffset + " in " + input,
                            overflow);
                }
                firstStartNanos = Math.min(firstStartNanos, startNanos);
                lastEndNanos = Math.max(lastEndNanos, endNanos);
                chunkOffset += chunkSize;
            }
        }
        if (firstStartNanos == Long.MAX_VALUE) {
            throw new IOException("Cannot inspect an empty JFR recording: " + input);
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
