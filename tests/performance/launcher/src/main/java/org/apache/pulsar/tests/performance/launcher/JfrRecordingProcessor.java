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
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class JfrRecordingProcessor {
    private static final String JFR_SUFFIX = ".jfr";
    private static final String MEASUREMENT_SUFFIX = ".measurement.jfr";

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

    static void process(Set<Path> recordings, Instant from,
                        boolean retainOriginal, boolean createMeasurementRecording) throws IOException {
        IOException failure = null;
        for (Path recording : recordings) {
            try {
                if (createMeasurementRecording) {
                    JfrCut.cutFrom(recording, from, measurementPath(recording));
                }
                if (!retainOriginal) {
                    Files.delete(recording);
                }
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

    static Path measurementPath(Path recording) {
        String name = recording.getFileName().toString();
        return recording.resolveSibling(name.substring(0, name.length() - JFR_SUFFIX.length())
                + MEASUREMENT_SUFFIX);
    }
}
