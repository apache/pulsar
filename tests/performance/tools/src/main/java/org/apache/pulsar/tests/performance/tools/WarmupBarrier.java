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
import java.time.Duration;

/** Coordinates warmup delivery without adding per-message work after warmup has completed. */
final class WarmupBarrier {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(20);

    private WarmupBarrier() {
    }

    static void markApplicationComplete(Path directory, int round, int application) throws IOException {
        Files.createDirectories(directory);
        Files.writeString(marker(directory, round, application), "complete\n");
    }

    static void awaitApplications(Path directory, int round, int applications, int timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            boolean complete = true;
            for (int application = 0; application < applications; application++) {
                if (!Files.isRegularFile(marker(directory, round, application))) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return;
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
        throw new IllegalStateException("Timed out waiting for all " + applications
                + " applications to receive warmup round " + round);
    }

    private static Path marker(Path directory, int round, int application) {
        return directory.resolve("warmup-round-" + round + "-application-" + application + ".complete");
    }
}
