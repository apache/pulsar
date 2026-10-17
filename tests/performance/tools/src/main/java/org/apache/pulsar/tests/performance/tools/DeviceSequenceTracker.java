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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.MessageIdAdv;

/**
 * Tracks the first-observed sequence for bounded numeric device identifiers without per-message allocation. A tracker
 * that starts at each device's first sequence, for a subscription whose position was moved past the start, expects
 * each device's messages from the first one that it receives; the application checks that it received as many
 * messages as follow the position.
 */
final class DeviceSequenceTracker {
    private static final int STATE_VERSION = 1;
    private static final int LOCK_COUNT = 1024;
    private static final int MAX_VIOLATION_SAMPLES = 100;
    // Whether a device's messages must arrive in order; without it, a message after a gap waits for the gap like an
    // ordering violation does, but isn't counted as one
    private final boolean checkOrdering;
    private final long[] nextExpected;
    // Each device's latest in-order message: its sequence and its message ID's ledger, entry and batch index, to tell
    // a redelivered duplicate, with the same message ID, from a message persisted twice, with another
    private final long[] lastKeyedSequence;
    private final long[] lastLedgerId;
    private final long[] lastEntryId;
    private final int[] lastBatchIndex;
    private final Object[] locks = new Object[LOCK_COUNT];
    private final ConcurrentHashMap<Integer, Set<Long>> pending = new ConcurrentHashMap<>();
    private final AtomicLong uniqueMessages = new AtomicLong();
    // With startAtFirstSequence: whether each device has had a message
    private final boolean[] started;
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong orderingViolations = new AtomicLong();
    private final AtomicLong invalidMessages = new AtomicLong();
    private final AtomicLong redeliveredDuplicates = new AtomicLong();
    private final AtomicLong republishedDuplicates = new AtomicLong();
    private final ConcurrentLinkedQueue<String> republishedSamples = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> violationSamples = new ConcurrentLinkedQueue<>();

    DeviceSequenceTracker(int deviceCount) {
        this(deviceCount, false, true);
    }

    /**
     * @param startAtFirstSequence whether each device's messages are expected from the first one that it receives,
     *                             for a subscription whose position was moved past the start
     * @param checkOrdering whether each device's messages must arrive in order
     */
    DeviceSequenceTracker(int deviceCount, boolean startAtFirstSequence, boolean checkOrdering) {
        started = startAtFirstSequence ? new boolean[deviceCount] : null;
        this.checkOrdering = checkOrdering;
        nextExpected = new long[deviceCount];
        lastKeyedSequence = new long[deviceCount];
        Arrays.fill(lastKeyedSequence, -1);
        lastLedgerId = new long[deviceCount];
        lastEntryId = new long[deviceCount];
        lastBatchIndex = new int[deviceCount];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }

    void received(long deviceId, long sequence) {
        received(deviceId, sequence, null, 0, null, null, -1, 0);
    }

    /**
     * Counts a received message. The message's ID, send time, topic, receiving thread, redelivery count and receipt
     * time go into the samples of ordering violations and republished duplicates.
     */
    void received(long deviceId, long sequence, Object messageId, long sentNanos, String topic, String thread,
                  int redeliveryCount, long receivedEpochMs) {
        if (deviceId < 0 || deviceId >= nextExpected.length || sequence < 0) {
            invalidMessages.incrementAndGet();
            return;
        }
        int index = Math.toIntExact(deviceId);
        synchronized (locks[index & (locks.length - 1)]) {
            if (started != null && !started[index]) {
                started[index] = true;
                nextExpected[index] = sequence;
            }
            long expected = nextExpected[index];
            if (sequence < expected) {
                duplicates.incrementAndGet();
                if (messageId instanceof MessageIdAdv id && sequence == lastKeyedSequence[index]) {
                    if (id.getLedgerId() == lastLedgerId[index] && id.getEntryId() == lastEntryId[index]
                            && id.getBatchIndex() == lastBatchIndex[index]) {
                        redeliveredDuplicates.incrementAndGet();
                    } else if (republishedDuplicates.incrementAndGet() <= MAX_VIOLATION_SAMPLES) {
                        republishedSamples.add("device=" + deviceId + " sequence=" + sequence
                                + formatContext(messageId, sentNanos, topic, thread, redeliveryCount, receivedEpochMs));
                    }
                }
                return;
            }
            if (sequence > expected) {
                Set<Long> devicePending = pending.computeIfAbsent(index, ignored -> new HashSet<>());
                if (devicePending.add(sequence)) {
                    uniqueMessages.incrementAndGet();
                    if (!checkOrdering) {
                        return;
                    }
                    long violationNumber = orderingViolations.incrementAndGet();
                    if (violationNumber <= MAX_VIOLATION_SAMPLES) {
                        violationSamples.add("device=" + deviceId + " expected=" + expected + " observed=" + sequence
                                + formatContext(messageId, sentNanos, topic, thread, redeliveryCount, receivedEpochMs));
                    }
                } else {
                    duplicates.incrementAndGet();
                }
                return;
            }
            uniqueMessages.incrementAndGet();
            if (messageId instanceof MessageIdAdv id) {
                lastKeyedSequence[index] = sequence;
                lastLedgerId[index] = id.getLedgerId();
                lastEntryId[index] = id.getEntryId();
                lastBatchIndex[index] = id.getBatchIndex();
            }
            Set<Long> devicePending = pending.get(index);
            if (checkOrdering && devicePending != null && violationSamples.size() < MAX_VIOLATION_SAMPLES * 2) {
                violationSamples.add("device=" + deviceId + " recovered=" + sequence
                        + formatContext(messageId, sentNanos, topic, thread, redeliveryCount, receivedEpochMs));
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
        return new Summary(uniqueMessages.get(), duplicates.get(), orderingViolations.get(), invalidMessages.get(),
                redeliveredDuplicates.get(), republishedDuplicates.get());
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

    /** Writes the first duplicates that came with another message ID than the message they repeat. */
    void writeRepublishedSamples(Path path) throws IOException {
        if (!republishedSamples.isEmpty()) {
            Files.write(path, republishedSamples);
        }
    }

    private static String formatContext(Object messageId, long sentNanos, String topic, String thread,
                                        int redeliveryCount, long receivedEpochMs) {
        if (messageId == null) {
            return "";
        }
        return " messageId=" + messageId + " sentNanos=" + sentNanos
                + " topic=" + topic + " thread=" + thread + " redeliveryCount=" + redeliveryCount
                + " receivedEpochMs=" + receivedEpochMs;
    }

    /**
     * The messages received.
     *
     * @param duplicates every message received again
     * @param redeliveredDuplicates the duplicates of a device's latest message with the same message ID: redelivered
     * @param republishedDuplicates the duplicates of a device's latest message with another message ID: the broker
     *     persisted the message twice
     */
    record Summary(long uniqueMessages, long duplicates, long orderingViolations, long invalidMessages,
                   long redeliveredDuplicates, long republishedDuplicates) {
        boolean valid() {
            return orderingViolations == 0 && invalidMessages == 0;
        }
    }
}
