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

import io.github.merlimat.slog.Logger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class JfrRecordingProcessor {
    private static final String JFR_SUFFIX = ".jfr";
    private static final String MEASUREMENT_SUFFIX = ".measurement.jfr";
    private static final Duration CLOCK_TOLERANCE = Duration.ofSeconds(1);

    private JfrRecordingProcessor() {
    }

    static Set<Path> findOriginalRecordings(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return Set.of();
        }
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(JFR_SUFFIX))
                    .filter(path -> !path.getFileName().toString().endsWith(MEASUREMENT_SUFFIX))
                    .map(path -> path.toAbsolutePath().normalize())
                    .sorted()
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    /** Cuts each recording to the measurement into the measurement recording beside it, keeping the recording. */
    static void process(Set<Path> recordings, Instant from, Instant to) throws IOException {
        IOException failure = null;
        for (Path recording : recordings) {
            try {
                JfrCut.cut(recording, from, to, measurementPath(recording));
                warnAboutClockMismatches(recording);
            } catch (IOException error) {
                if (failure == null) {
                    failure = new IOException("Failed to process JFR recordings");
                }
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    // The cut keeps the right events of a chunk whose clock has another origin, but whole-file readers mistime them
    private static void warnAboutClockMismatches(Path recording) throws IOException {
        for (JfrCut.ClockMismatch mismatch : JfrCut.clockMismatches(recording, CLOCK_TOLERANCE)) {
            log().warn().attr("recording", recording).attr("chunk", mismatch.chunk())
                    .attr("errorSeconds", mismatch.error().toMillis() / 1000.0)
                    .log("A chunk of the recording has a clock that doesn't line up with the first chunk's. The"
                            + " measurement recording keeps its events, but JDK 22+ readers of a whole recording,"
                            + " such as jfr print and the jonoffcpu correlator, mistime them");
        }
    }

    private static Logger log() {
        return Logger.get(JfrRecordingProcessor.class);
    }

    static Path measurementPath(Path recording) {
        String name = recording.getFileName().toString();
        return recording.resolveSibling(name.substring(0, name.length() - JFR_SUFFIX.length())
                + MEASUREMENT_SUFFIX);
    }
}
