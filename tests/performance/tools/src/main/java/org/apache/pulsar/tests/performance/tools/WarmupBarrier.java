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
import java.nio.file.Files;
import java.nio.file.Path;

/** Coordinates warmup delivery without adding per-message work after warmup has completed. */
final class WarmupBarrier {
    private static final long POLL_INTERVAL_MILLIS = 20;

    private WarmupBarrier() {
    }

    static void markApplicationComplete(Path directory, String runId, int round, int application) throws IOException {
        Files.createDirectories(directory);
        Files.writeString(marker(directory, runId, round, application), "complete\n");
    }

    static void awaitApplications(Path directory, String runId, int round, int applications, long deadlineNanos)
            throws Exception {
        while (deadlineNanos - System.nanoTime() > 0) {
            boolean complete = true;
            for (int application = 0; application < applications; application++) {
                if (!Files.isRegularFile(marker(directory, runId, round, application))) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        throw new IllegalStateException("Timed out waiting for all " + applications
                + " applications to receive warmup round " + round);
    }

    /**
     * Tells the launcher that the producer is about to send its first measured message, after every warmup round
     * has been received, so that the launcher can let the host cool down before the measurement.
     */
    static void markReadyForMeasurement(Path directory, String runId) throws IOException {
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("measurement-" + runId + ".ready"), "ready\n");
    }

    /** Waits until the launcher lets the measurement start. */
    static void awaitMeasurementStart(Path directory, String runId, long deadlineNanos) throws Exception {
        Path start = directory.resolve("measurement-" + runId + ".start");
        while (!Files.isRegularFile(start)) {
            if (deadlineNanos - System.nanoTime() <= 0) {
                throw new IllegalStateException("Timed out waiting for the launcher to start the measurement");
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
    }

    private static Path marker(Path directory, String runId, int round, int application) {
        return directory.resolve("warmup-" + runId + "-round-" + round + "-application-" + application + ".complete");
    }
}
