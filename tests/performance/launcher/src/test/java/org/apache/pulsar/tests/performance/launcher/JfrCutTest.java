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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.testng.annotations.DataProvider;
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

            assertEquals(markers(output), List.of("measurement"));
            assertEquals(markers(input), List.of("before", "measurement", "after"));
            assertTrue(eventNames(output).contains("jdk.JVMInformation"));
            assertEquals(JfrCut.recordingInfo(output), JfrCut.recordingInfo(input));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test(dataProvider = "retentionModes")
    public void appliesIndependentRetentionOptions(boolean retainOriginal, boolean createMeasurement)
            throws Exception {
        Path directory = Files.createTempDirectory("jfr-retention-test");
        try {
            Path input = directory.resolve("profile.jfr");
            Instant[] interval = createRecording(input);
            Path measurement = JfrRecordingProcessor.measurementPath(input);

            JfrRecordingProcessor.process(Set.of(input), interval[0], interval[1],
                    retainOriginal, createMeasurement);

            assertEquals(Files.exists(input), retainOriginal);
            assertEquals(Files.exists(measurement), createMeasurement);
            if (createMeasurement) {
                assertEquals(markers(measurement), List.of("measurement"));
            }
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

            assertEquals(JfrRecordingProcessor.findOriginalRecordings(directory), Set.of(original));
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
            try {
                JfrCut.cut(input, now, now, directory.resolve("output.jfr"));
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("start must be before"));
                return;
            }
            fail("Expected an invalid interval to be rejected");
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void parsesIsoInstantsAndEpochMilliseconds() {
        Instant instant = Instant.parse("2026-09-20T10:05:00Z");

        assertEquals(JfrCut.parseInstant(instant.toString()), instant);
        assertEquals(JfrCut.parseInstant(Long.toString(instant.toEpochMilli())), instant);
    }

    @Test
    public void parsesRecordingRelativeTimes() {
        Instant start = Instant.parse("2026-09-20T10:00:00Z");

        assertEquals(JfrCut.parseTimeExpression("500ms", start), start.plusMillis(500));
        assertEquals(JfrCut.parseTimeExpression("5s", start), start.plusSeconds(5));
        assertEquals(JfrCut.parseTimeExpression("2m", start), start.plus(2, ChronoUnit.MINUTES));
        assertEquals(JfrCut.parseTimeExpression("1h", start), start.plus(1, ChronoUnit.HOURS));
        assertEquals(JfrCut.parseTimeExpression("PT5S", start), start.plusSeconds(5));
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

            assertEquals(markers(fromBeginning), List.of("before", "measurement", "after"));
            assertEquals(markers(throughEnd), List.of("before", "measurement", "after"));
            JfrCut.RecordingInfo info = JfrCut.recordingInfo(input);
            assertTrue(!info.start().isAfter(measurementInterval[0]));
            assertTrue(!info.end().isBefore(measurementInterval[1]));
            assertEquals(info.duration(), Duration.between(info.start(), info.end()));
        } finally {
            deleteDirectory(directory);
        }
    }

    @Test
    public void derivesDefaultOutputBesideInput() {
        assertEquals(JfrCut.defaultOutput(Path.of("/tmp/profile.jfr")), Path.of("/tmp/profile.cut.jfr"));
        assertEquals(JfrCut.defaultOutput(Path.of("/tmp/profile")), Path.of("/tmp/profile.cut.jfr"));
    }

    @DataProvider
    public Object[][] retentionModes() {
        return new Object[][] {
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        };
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
