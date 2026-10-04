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
package org.apache.pulsar.tests.performance.tools;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeoutException;

/**
 * The gateways' measurement start, for the applications that join after it: a file in the coordination directory
 * with the start's epoch milliseconds, written atomically.
 */
final class MeasurementStartMarker {
    private static final long POLL_INTERVAL_MILLIS = 20;

    private MeasurementStartMarker() {
    }

    static void mark(Path directory, String runId, long epochMs) throws IOException {
        Files.createDirectories(directory);
        Path marker = marker(directory, runId);
        Path temporary = marker.resolveSibling(marker.getFileName() + ".tmp");
        Files.writeString(temporary, Long.toString(epochMs), StandardCharsets.UTF_8);
        Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Waits for the measurement to start, and returns its epoch milliseconds.
     *
     * @throws TimeoutException when the measurement doesn't start before the deadline
     */
    static long await(Path directory, String runId, long deadlineNanos) throws Exception {
        Path marker = marker(directory, runId);
        while (deadlineNanos - System.nanoTime() > 0) {
            try {
                return Long.parseLong(Files.readString(marker, StandardCharsets.UTF_8).trim());
            } catch (NoSuchFileException e) {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            }
        }
        throw new TimeoutException("Timed out waiting for the measurement to start");
    }

    private static Path marker(Path directory, String runId) {
        return directory.resolve("measurement-start-" + runId);
    }
}
