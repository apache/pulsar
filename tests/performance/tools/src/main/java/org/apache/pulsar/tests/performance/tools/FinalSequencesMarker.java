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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * Each device's next sequence once the gateways have sent every message, for the applications that read the topics
 * compacted, which have each device's latest state when they have its last message: a file in the coordination
 * directory, written atomically when the gateways finish.
 */
final class FinalSequencesMarker {
    private FinalSequencesMarker() {
    }

    static void mark(Path directory, String runId, long[] nextSequences) throws IOException {
        Files.createDirectories(directory);
        Path marker = marker(directory, runId);
        Path temporary = marker.resolveSibling(marker.getFileName() + ".tmp");
        try (var data = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temporary)))) {
            data.writeInt(nextSequences.length);
            for (long sequence : nextSequences) {
                data.writeLong(sequence);
            }
        }
        Files.move(temporary, marker, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** The devices' next sequences, or empty while the gateways haven't finished. */
    static Optional<long[]> read(Path directory, String runId) throws IOException {
        try (var data = new DataInputStream(new BufferedInputStream(Files.newInputStream(
                marker(directory, runId))))) {
            long[] nextSequences = new long[data.readInt()];
            for (int device = 0; device < nextSequences.length; device++) {
                nextSequences[device] = data.readLong();
            }
            return Optional.of(nextSequences);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    private static Path marker(Path directory, String runId) {
        return directory.resolve("final-sequences-" + runId);
    }
}
