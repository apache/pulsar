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
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The launcher has prepared the late applications' join, with {@code applications.skipMessages},
 * {@code applications.resetToMillis} or {@code applications.unloadBeforeJoin}: a file in the coordination directory,
 * which the applications that join later wait for before they open their pods. It has a line {@code <application
 * index>=<messages>} for each application that reset its subscription: the messages that its backlog then had, which
 * it expects.
 */
public final class JoinPreparedMarker {
    private static final long POLL_INTERVAL_MILLIS = 20;

    private JoinPreparedMarker() {
    }

    public static void mark(Path directory, String runId) throws IOException {
        mark(directory, runId, Map.of());
    }

    /** Marks the join prepared, with the messages that each application that reset its subscription expects. */
    public static void mark(Path directory, String runId, Map<Integer, Long> expectedMessages) throws IOException {
        Files.createDirectories(directory);
        Path marker = marker(directory, runId);
        Path temporary = marker.resolveSibling(marker.getFileName() + ".tmp");
        StringBuilder content = new StringBuilder();
        expectedMessages.forEach((application, messages) ->
                content.append(application).append('=').append(messages).append('\n'));
        Files.writeString(temporary, content);
        Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Waits for the launcher to prepare the join, and returns the messages that each application that reset its
     * subscription expects.
     *
     * @throws TimeoutException when it doesn't before the deadline
     */
    static Map<Integer, Long> await(Path directory, String runId, long deadlineNanos) throws Exception {
        Path marker = marker(directory, runId);
        while (!Files.exists(marker)) {
            if (deadlineNanos - System.nanoTime() <= 0) {
                throw new TimeoutException("Timed out waiting for the launcher to prepare the join");
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        Map<Integer, Long> expectedMessages = new LinkedHashMap<>();
        for (String line : Files.readAllLines(marker)) {
            int separator = line.indexOf('=');
            if (separator > 0) {
                expectedMessages.put(Integer.parseInt(line.substring(0, separator).trim()),
                        Long.parseLong(line.substring(separator + 1).trim()));
            }
        }
        return expectedMessages;
    }

    private static Path marker(Path directory, String runId) {
        return directory.resolve("join-prepared-" + runId);
    }
}
