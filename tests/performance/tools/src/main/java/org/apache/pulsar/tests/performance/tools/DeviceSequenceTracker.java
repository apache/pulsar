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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/** Tracks the first-observed sequence for bounded numeric device identifiers without per-message allocation. */
final class DeviceSequenceTracker {
    private static final int STATE_VERSION = 1;
    private static final int LOCK_COUNT = 1024;
    private static final int MAX_VIOLATION_SAMPLES = 100;
    private final long[] nextExpected;
    private final Object[] locks = new Object[LOCK_COUNT];
    private final ConcurrentHashMap<Integer, Set<Long>> pending = new ConcurrentHashMap<>();
    private final AtomicLong uniqueMessages = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong orderingViolations = new AtomicLong();
    private final AtomicLong invalidMessages = new AtomicLong();
    private final ConcurrentLinkedQueue<String> violationSamples = new ConcurrentLinkedQueue<>();

    DeviceSequenceTracker(int deviceCount) {
        nextExpected = new long[deviceCount];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }

    void received(long deviceId, long sequence) {
        received(deviceId, sequence, null, 0, null, null);
    }

    void received(long deviceId, long sequence, Object messageId, long sentNanos, String topic, String thread) {
        if (deviceId < 0 || deviceId >= nextExpected.length || sequence < 0) {
            invalidMessages.incrementAndGet();
            return;
        }
        int index = Math.toIntExact(deviceId);
        synchronized (locks[index & (locks.length - 1)]) {
            long expected = nextExpected[index];
            if (sequence < expected) {
                duplicates.incrementAndGet();
                return;
            }
            if (sequence > expected) {
                Set<Long> devicePending = pending.computeIfAbsent(index, ignored -> new HashSet<>());
                if (devicePending.add(sequence)) {
                    uniqueMessages.incrementAndGet();
                    long violationNumber = orderingViolations.incrementAndGet();
                    if (violationNumber <= MAX_VIOLATION_SAMPLES) {
                        violationSamples.add("device=" + deviceId + " expected=" + expected
                                + " observed=" + sequence + formatContext(messageId, sentNanos, topic, thread));
                    }
                } else {
                    duplicates.incrementAndGet();
                }
                return;
            }
            uniqueMessages.incrementAndGet();
            Set<Long> devicePending = pending.get(index);
            if (devicePending != null && violationSamples.size() < MAX_VIOLATION_SAMPLES * 2) {
                violationSamples.add("device=" + deviceId + " recovered=" + sequence
                        + formatContext(messageId, sentNanos, topic, thread));
            }
            expected++;
            while (devicePending != null && devicePending.remove(expected)) {
                expected++;
            }
            nextExpected[index] = expected;
            if (devicePending != null && devicePending.isEmpty()) {
                pending.remove(index, devicePending);
            }
        }
    }

    long uniqueMessages() {
        return uniqueMessages.get();
    }

    void invalidMessage() {
        invalidMessages.incrementAndGet();
    }

    Summary summary() {
        return new Summary(uniqueMessages.get(), duplicates.get(), orderingViolations.get(), invalidMessages.get());
    }

    void writeState(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (var output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            output.writeInt(STATE_VERSION);
            output.writeInt(nextExpected.length);
            for (int i = 0; i < nextExpected.length; i++) {
                synchronized (locks[i & (locks.length - 1)]) {
                    output.writeLong(nextExpected[i]);
                }
            }
        }
    }

    void writeViolationSamples(Path path) throws IOException {
        Files.write(path, violationSamples);
    }

    private static String formatContext(Object messageId, long sentNanos, String topic, String thread) {
        if (messageId == null) {
            return "";
        }
        return " messageId=" + messageId + " sentNanos=" + sentNanos
                + " topic=" + topic + " thread=" + thread;
    }

    record Summary(long uniqueMessages, long duplicates, long orderingViolations, long invalidMessages) {
        boolean valid() {
            return orderingViolations == 0 && invalidMessages == 0;
        }
    }
}
