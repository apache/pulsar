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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
 * Writes the events overlapping a time interval to a new JFR recording.
 *
 * <p>Call {@link #cut(Path, Instant, Instant, Path)} directly when embedding the cutter, or invoke this class as a
 * command-line application through the {@code runJfrCut} Gradle task.
 */
@Command(name = "jfr-cut", mixinStandardHelpOptions = true,
        description = "Write events overlapping a time interval to a new JFR recording")
public final class JfrCut implements Callable<Integer> {
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

    @Option(names = "--info", description = "Show the recording event range and duration")
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
     * Relative values such as {@code 5s} are measured from the first event in the recording. A {@code null} start
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

    /** Returns the first and last event times and their duration. */
    public static RecordingInfo recordingInfo(Path input) throws IOException {
        Instant start = null;
        Instant end = null;
        try (RecordingFile recording = new RecordingFile(input.toAbsolutePath().normalize())) {
            while (recording.hasMoreEvents()) {
                RecordedEvent event = recording.readEvent();
                if (start == null || event.getStartTime().isBefore(start)) {
                    start = event.getStartTime();
                }
                if (end == null || event.getEndTime().isAfter(end)) {
                    end = event.getEndTime();
                }
            }
        }
        if (start == null) {
            throw new IOException("Cannot cut a JFR recording with no events: " + input);
        }
        return new RecordingInfo(start, end, Duration.between(start, end));
    }

    /** Event time range found in a JFR recording. */
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
