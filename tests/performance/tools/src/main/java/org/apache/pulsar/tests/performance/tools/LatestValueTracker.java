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
import java.util.concurrent.atomic.AtomicLong;

/**
 * The latest state of each device, as a pod that reads the devices' compacted topics keeps it: a reader of a compacted
 * topic receives each device's latest message that compaction kept, and then every later one, so a device's sequences
 * only rise, with gaps. A device's sequence that falls is an ordering violation: the pod would go back to an older
 * state. The pod is current when each device's latest sequence is the last one that the gateways sent.
 */
final class LatestValueTracker {
    /** What a received message did to the device's state. */
    enum Outcome {
        /** A newer sequence: the device's latest state. */
        UPDATE,
        /** The device's latest sequence again. */
        REPEAT,
        /** An older sequence than the device's latest: an ordering violation. */
        REGRESSION,
        /** A message of no known device. */
        INVALID
    }

    private static final int LOCK_COUNT = 1024;
    private final long[] latest;
    private final Object[] locks = new Object[LOCK_COUNT];
    private final AtomicLong received = new AtomicLong();

    LatestValueTracker(int deviceCount) {
        latest = new long[deviceCount];
        Arrays.fill(latest, -1);
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }

    Outcome received(long deviceId, long sequence) {
        received.incrementAndGet();
        if (deviceId < 0 || deviceId >= latest.length || sequence < 0) {
            return Outcome.INVALID;
        }
        int device = (int) deviceId;
        synchronized (locks[device & (LOCK_COUNT - 1)]) {
            long current = latest[device];
            if (sequence > current) {
                latest[device] = sequence;
                return Outcome.UPDATE;
            }
            return sequence == current ? Outcome.REPEAT : Outcome.REGRESSION;
        }
    }

    /**
     * Writes each device's next sequence after its latest one, in the format of the gateways' state, which the
     * launcher compares with it.
     */
    void writeState(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (var output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            output.writeInt(DeviceSequenceTracker.STATE_VERSION);
            output.writeInt(latest.length);
            for (int device = 0; device < latest.length; device++) {
                synchronized (locks[device & (LOCK_COUNT - 1)]) {
                    output.writeLong(latest[device] + 1);
                }
            }
        }
    }

    /** The devices whose latest sequence passes the last one that the gateways sent: they can't have sent it. */
    long aheadDevices(long[] nextSequences) {
        long ahead = 0;
        for (int device = 0; device < latest.length; device++) {
            long next = device < nextSequences.length ? nextSequences[device] : 0;
            synchronized (locks[device & (LOCK_COUNT - 1)]) {
                if (latest[device] >= next) {
                    ahead++;
                }
            }
        }
        return ahead;
    }

    /** The messages received, of any outcome. */
    long receivedMessages() {
        return received.get();
    }

    /**
     * The devices whose latest sequence isn't the last one that the gateways sent, given each device's next sequence
     * at the gateways: 0 for a device without messages.
     */
    long staleDevices(long[] nextSequences) {
        long stale = 0;
        for (int device = 0; device < latest.length; device++) {
            long expected = device < nextSequences.length ? nextSequences[device] - 1 : -1;
            synchronized (locks[device & (LOCK_COUNT - 1)]) {
                if (latest[device] != expected) {
                    stale++;
                }
            }
        }
        return stale;
    }
}
