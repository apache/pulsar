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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.testng.annotations.Test;

public class JfrCutTest {
    private static final String EVENT_NAME = "org.apache.pulsar.tests.performance.Marker";

    @Name(EVENT_NAME)
    static class MarkerEvent extends Event {
        String marker;
    }

    @Test
    public void cutsEventsToMeasurementInterval() throws Exception {
        Path directory = Files.createTempDirectory("jfr-cut-test");
        try {
            Path input = directory.resolve("input.jfr");
            Instant[] interval = createRecording(input);
            Path output = directory.resolve("output.jfr");

            JfrCut.cut(input, interval[0], interval[1], output);

            assertThat(markers(output)).containsExactly("measurement");
            assertThat(markers(input)).containsExactly("before", "measurement", "after");
            assertThat(eventNames(output)).contains("jdk.JVMInformation");
            assertThat(JfrCut.recordingInfo(output)).isEqualTo(JfrCut.recordingInfo(input));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void honorsExactEventBoundaries() throws Exception {
        Path directory = Files.createTempDirectory("jfr-boundary-test");
        try {
            Path input = directory.resolve("input.jfr");
            try (Recording recording = new Recording()) {
                recording.enable(MarkerEvent.class);
                recording.start();
                MarkerEvent duration = new MarkerEvent();
                duration.marker = "duration";
                duration.begin();
                Thread.sleep(10);
                duration.end();
                duration.commit();
                marker("instant");
                recording.stop();
                recording.dump(input);
            }
            var events = RecordingFile.readAllEvents(input).stream()
                    .filter(event -> event.getEventType().getName().equals(EVENT_NAME)).toList();
            var duration = events.stream().filter(event -> event.getString("marker").equals("duration"))
                    .findFirst().orElseThrow();
            var instant = events.stream().filter(event -> event.getString("marker").equals("instant"))
                    .findFirst().orElseThrow();
            Path output = directory.resolve("output.jfr");
            JfrCut.cut(input, duration.getEndTime(), instant.getStartTime().plusNanos(1), output);
            assertThat(markers(output)).containsExactly("instant");
            JfrCut.cutFrom(input, duration.getEndTime(), output);
            assertThat(markers(output)).containsExactly("instant");
            JfrCut.cut(input, instant.getStartTime(), instant.getStartTime().plusNanos(1), output);
            assertThat(markers(output)).containsExactly("instant");
            JfrCut.cut(input, duration.getStartTime(), instant.getStartTime(), output);
            assertThat(markers(output)).containsExactly("duration");
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void cutsTheMeasurementRecordingAndKeepsTheRecording() throws Exception {
        Path directory = Files.createTempDirectory("jfr-measurement-test");
        try {
            Path input = directory.resolve("profile.jfr");
            Instant[] interval = createRecording(input);
            Path measurement = JfrRecordingProcessor.measurementPath(input);

            JfrRecordingProcessor.process(Set.of(input), interval[0], interval[1]);

            assertThat(input).exists();
            assertThat(markers(measurement)).containsExactly("measurement");
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void cutsEachChunkByItsOwnClock() throws Exception {
        // Recorded by async-profiler's jfrsync on aarch64 before its clock was aligned with the JVM's: the JDK's chunk
        // counts ticks from the JVM's start and async-profiler's chunk from the host's boot, so a JDK 22+ reader of the
        // whole file puts async-profiler's events 672,402 s later. Each chunk was cut to 04:41:20.000-04:41:20.200.
        Path directory = Files.createTempDirectory("jfr-chunk-clock-test");
        try {
            Path input = mixedClockRecording(directory);
            Path output = directory.resolve("output.jfr");

            JfrCut.cut(input, Instant.parse("2026-09-27T04:41:20.050Z"), Instant.parse("2026-09-27T04:41:20.150Z"),
                    output);

            Map<String, Long> kept = eventCounts(output);
            Map<String, Long> recorded = eventCounts(input);
            // Both chunks keep the events inside the interval, and only those
            for (String name : List.of("jdk.ThreadPark", "jdk.ExecutionSample", "profiler.SignalSample")) {
                assertThat(kept.getOrDefault(name, 0L)).as(name).isPositive().isLessThan(recorded.get(name));
            }
            assertThat(JfrCut.recordingInfo(output)).isEqualTo(JfrCut.recordingInfo(input));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void findsChunksWhoseClockHasAnotherOrigin() throws Exception {
        Path directory = Files.createTempDirectory("jfr-clock-mismatch-test");
        try {
            List<JfrCut.ClockMismatch> mismatches =
                    JfrCut.clockMismatches(mixedClockRecording(directory), Duration.ofSeconds(1));

            assertThat(mismatches).hasSize(1);
            assertThat(mismatches.get(0).chunk()).isEqualTo(1);
            assertThat(mismatches.get(0).error()).isBetween(Duration.ofSeconds(672_402), Duration.ofSeconds(672_403));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void cutsRecordingsOfSeveralChunksWithOneClock() throws Exception {
        Path directory = Files.createTempDirectory("jfr-chunks-test");
        try {
            Path first = directory.resolve("first.jfr");
            createRecording(first);
            Path second = directory.resolve("second.jfr");
            Instant[] interval = createRecording(second);
            Path input = directory.resolve("input.jfr");
            Files.write(input, Files.readAllBytes(first));
            Files.write(input, Files.readAllBytes(second), StandardOpenOption.APPEND);
            Path output = directory.resolve("output.jfr");

            JfrCut.cut(input, interval[0], interval[1], output);

            assertThat(markers(input)).containsExactly("before", "measurement", "after", "before", "measurement",
                    "after");
            assertThat(markers(output)).containsExactly("measurement");
            assertThat(JfrCut.clockMismatches(input, Duration.ofSeconds(1))).isEmpty();
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void findsOnlyOriginalRecordings() throws Exception {
        Path directory = Files.createTempDirectory("jfr-discovery-test");
        try {
            Path original = directory.resolve("profile.jfr");
            Path measurement = directory.resolve("profile.measurement.jfr");
            Files.write(original, new byte[] {1});
            Files.write(measurement, new byte[] {1});
            Files.writeString(directory.resolve("profile.log"), "log");

            assertThat(JfrRecordingProcessor.findOriginalRecordings(directory)).containsExactlyInAnyOrder(original);
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void rejectsInvalidInterval() throws Exception {
        Path directory = Files.createTempDirectory("jfr-invalid-interval-test");
        try {
            Path input = directory.resolve("input.jfr");
            Files.write(input, new byte[] {1});
            Instant now = Instant.now();
            assertThatThrownBy(() -> JfrCut.cut(input, now, now, directory.resolve("output.jfr")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("start must be before");
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void parsesIsoInstantsAndEpochMilliseconds() {
        Instant instant = Instant.parse("2026-09-20T10:05:00Z");

        assertThat(JfrCut.parseInstant(instant.toString())).isEqualTo(instant);
        assertThat(JfrCut.parseInstant(Long.toString(instant.toEpochMilli()))).isEqualTo(instant);
    }

    @Test
    public void parsesRecordingRelativeTimes() {
        Instant start = Instant.parse("2026-09-20T10:00:00Z");

        assertThat(JfrCut.parseTimeExpression("500ms", start)).isEqualTo(start.plusMillis(500));
        assertThat(JfrCut.parseTimeExpression("5s", start)).isEqualTo(start.plusSeconds(5));
        assertThat(JfrCut.parseTimeExpression("2m", start)).isEqualTo(start.plus(2, ChronoUnit.MINUTES));
        assertThat(JfrCut.parseTimeExpression("1h", start)).isEqualTo(start.plus(1, ChronoUnit.HOURS));
        assertThat(JfrCut.parseTimeExpression("PT5S", start)).isEqualTo(start.plusSeconds(5));
    }

    @Test
    public void supportsOmittedTimeBoundaries() throws Exception {
        Path directory = Files.createTempDirectory("jfr-relative-cut-test");
        try {
            Path input = directory.resolve("input.jfr");
            Instant[] measurementInterval = createRecording(input);
            Path fromBeginning = directory.resolve("from-beginning.jfr");
            Path throughEnd = directory.resolve("through-end.jfr");

            JfrCut.cutUsingTimeExpressions(input, null, "1h", fromBeginning);
            JfrCut.cutUsingTimeExpressions(input, "0ms", null, throughEnd);

            assertThat(markers(fromBeginning)).containsExactly("before", "measurement", "after");
            assertThat(markers(throughEnd)).containsExactly("before", "measurement", "after");
            JfrCut.RecordingInfo info = JfrCut.recordingInfo(input);
            assertThat(info.start()).isBeforeOrEqualTo(measurementInterval[0]);
            assertThat(info.end()).isAfterOrEqualTo(measurementInterval[1]);
            assertThat(info.duration()).isEqualTo(Duration.between(info.start(), info.end()));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void derivesDefaultOutputBesideInput() {
        assertThat(JfrCut.defaultOutput(Path.of("/tmp/profile.jfr"))).isEqualTo(Path.of("/tmp/profile.cut.jfr"));
        assertThat(JfrCut.defaultOutput(Path.of("/tmp/profile"))).isEqualTo(Path.of("/tmp/profile.cut.jfr"));
    }

    private static Instant[] createRecording(Path path) throws Exception {
        try (Recording recording = new Recording()) {
            recording.enable(MarkerEvent.class);
            recording.enable("jdk.JVMInformation");
            recording.start();
            marker("before");
            Thread.sleep(10);
            Instant from = Instant.now();
            Thread.sleep(10);
            marker("measurement");
            Thread.sleep(10);
            Instant to = Instant.now();
            Thread.sleep(10);
            marker("after");
            recording.stop();
            recording.dump(path);
            return new Instant[] {from, to};
        }
    }

    private static Path mixedClockRecording(Path directory) throws Exception {
        Path recording = directory.resolve("mixed-clock-origins.jfr");
        try (InputStream resource = JfrCutTest.class.getResourceAsStream("/jfr/mixed-clock-origins.jfr.gz")) {
            assertThat(resource).isNotNull();
            try (InputStream gzip = new GZIPInputStream(resource)) {
                Files.copy(gzip, recording);
            }
        }
        return recording;
    }

    private static Map<String, Long> eventCounts(Path path) throws Exception {
        return RecordingFile.readAllEvents(path).stream()
                .collect(Collectors.groupingBy(event -> event.getEventType().getName(), Collectors.counting()));
    }

    private static void marker(String marker) {
        MarkerEvent event = new MarkerEvent();
        event.marker = marker;
        event.commit();
    }

    private static List<String> markers(Path path) throws Exception {
        return RecordingFile.readAllEvents(path).stream()
                .filter(event -> event.getEventType().getName().equals(EVENT_NAME))
                .map(event -> event.getString("marker"))
                .toList();
    }

    private static Set<String> eventNames(Path path) throws Exception {
        return RecordingFile.readAllEvents(path).stream()
                .map(event -> event.getEventType().getName())
                .collect(Collectors.toSet());
    }

    private static void deleteDirectory(Path directory) throws Exception {
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
