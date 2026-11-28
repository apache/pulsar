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

import static org.assertj.core.api.Assertions.assertThat;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.Test;

public class DeviceSequenceTrackerTest {
    @Test
    public void acceptsOrderedFirstDeliveryAndDuplicates() {
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(2);
        tracker.received(0, 0);
        tracker.received(0, 0);
        tracker.received(1, 0);
        tracker.received(0, 1);

        assertThat(tracker.summary()).isEqualTo(new DeviceSequenceTracker.Summary(3, 1, 0, 0, 0, 0));
        assertThat(tracker.summary().valid()).isTrue();
    }

    @Test
    public void startsAtEachDevicesFirstSequenceAfterSkippedMessages() {
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(3, true, true);
        tracker.received(0, 5);
        tracker.received(0, 6);
        tracker.received(1, 0);
        tracker.received(1, 1);
        // a gap after the first message is still a violation
        tracker.received(2, 3);
        tracker.received(2, 5);

        assertThat(tracker.uniqueMessages()).isEqualTo(6);
        assertThat(tracker.summary().orderingViolations()).isEqualTo(1);
        // an earlier sequence than the first is a duplicate
        tracker.received(0, 4);
        assertThat(tracker.summary().duplicates()).isEqualTo(1);
    }

    @Test
    public void rejectsGapAndInvalidDevice() {
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(1);
        tracker.received(0, 1);
        tracker.received(0, 1);
        tracker.received(0, 0);
        tracker.received(2, 0);

        assertThat(tracker.summary().orderingViolations()).isEqualTo(1);
        assertThat(tracker.summary().duplicates()).isEqualTo(1);
        assertThat(tracker.summary().uniqueMessages()).isEqualTo(2);
        assertThat(tracker.summary().invalidMessages()).isEqualTo(1);
        assertThat(tracker.summary().valid()).isFalse();
    }

    @Test
    public void acceptsAnyOrderWithoutTheOrderingCheck() throws Exception {
        // A Shared subscription delivers a device's messages in any order
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(1, false, false);
        tracker.received(0, 2);
        tracker.received(0, 0);
        tracker.received(0, 2);
        tracker.received(0, 1);
        tracker.received(0, 1);

        assertThat(tracker.summary()).isEqualTo(new DeviceSequenceTracker.Summary(3, 2, 0, 0, 0, 0));
        assertThat(tracker.summary().valid()).isTrue();
        Path samples = Files.createTempFile("ordering-violations", ".txt");
        try {
            tracker.writeViolationSamples(samples);
            assertThat(Files.readAllLines(samples)).isEmpty();
        } finally {
            Files.deleteIfExists(samples);
        }
    }

    @Test
    public void persistsTheContiguousPrefixWithoutTheOrderingCheck() throws Exception {
        // The persisted state is each device's next expected sequence, which the launcher compares with the gateways'
        // sequences to find lost messages: a gap that stays open keeps it at the gap
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(2, false, false);
        tracker.received(0, 2);
        tracker.received(0, 0);
        tracker.received(1, 1);
        assertThat(readState(tracker)).containsExactly(1, 0);
        tracker.received(0, 1);
        tracker.received(1, 0);
        assertThat(readState(tracker)).containsExactly(3, 2);
    }

    private static long[] readState(DeviceSequenceTracker tracker) throws Exception {
        Path state = Files.createTempFile("device-sequences", ".bin");
        try {
            tracker.writeState(state);
            try (var input = new DataInputStream(Files.newInputStream(state))) {
                input.readInt();
                long[] nextExpected = new long[input.readInt()];
                for (int i = 0; i < nextExpected.length; i++) {
                    nextExpected[i] = input.readLong();
                }
                return nextExpected;
            }
        } finally {
            Files.deleteIfExists(state);
        }
    }

    @Test
    public void tellsRedeliveredDuplicatesFromMessagesPersistedTwice() throws Exception {
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(1);
        tracker.received(0, 0, new MessageIdImpl(5, 0, -1), 0, "t", "x", 0, 0);
        // The same message again: redelivered
        tracker.received(0, 0, new MessageIdImpl(5, 0, -1), 0, "t", "x", 0, 0);
        // The same sequence in another entry: the broker persisted it twice
        tracker.received(0, 0, new MessageIdImpl(5, 7, -1), 0, "t", "x", 2, 1791000000123L);
        tracker.received(0, 1, new MessageIdImpl(5, 1, -1), 0, "t", "x", 0, 0);
        // An older sequence than the latest can't be classified
        tracker.received(0, 0, new MessageIdImpl(5, 0, -1), 0, "t", "x", 0, 0);

        assertThat(tracker.summary()).isEqualTo(new DeviceSequenceTracker.Summary(2, 3, 0, 0, 1, 1));
        // Message IDs are compared in full, not by a hash that two of them can share
        tracker.received(0, 2, new MessageIdImpl(1, 0, -1), 0, "t", "x", 0, 0);
        tracker.received(0, 2, new MessageIdImpl(368, 350, -1), 0, "t", "x", 0, 0);
        assertThat(tracker.summary().republishedDuplicates()).isEqualTo(2);
        Path samples = Files.createTempFile("republished", ".txt");
        tracker.writeRepublishedSamples(samples);
        assertThat(Files.readString(samples)).contains("device=0 sequence=0 messageId=5:7:-1")
                .contains("redeliveryCount=2 receivedEpochMs=1791000000123");
        Files.delete(samples);
    }

    @Test
    public void samplesOrderingViolationsWithTheirReceipts() throws Exception {
        DeviceSequenceTracker tracker = new DeviceSequenceTracker(1);
        tracker.received(0, 1, new MessageIdImpl(5, 4, -1), 0, "t", "x", 0, 1791000000001L);
        tracker.received(0, 0, new MessageIdImpl(5, 3, -1), 0, "t", "x", 1, 1791000000002L);

        assertThat(tracker.summary().orderingViolations()).isEqualTo(1);
        Path samples = Files.createTempFile("ordering", ".txt");
        tracker.writeViolationSamples(samples);
        assertThat(Files.readString(samples))
                .contains("device=0 expected=0 observed=1 messageId=5:4:-1")
                .contains("redeliveryCount=0 receivedEpochMs=1791000000001")
                .contains("device=0 recovered=0 messageId=5:3:-1")
                .contains("redeliveryCount=1 receivedEpochMs=1791000000002");
        Files.delete(samples);
    }
}
