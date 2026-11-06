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
import org.testng.annotations.Test;

public class LatestValueTrackerTest {
    @Test
    public void keepsEachDevicesLatestSequence() {
        LatestValueTracker tracker = new LatestValueTracker(3);
        // a compacted topic's latest message of a device, then the later ones
        assertThat(tracker.received(0, 4)).isEqualTo(LatestValueTracker.Outcome.UPDATE);
        assertThat(tracker.received(0, 6)).isEqualTo(LatestValueTracker.Outcome.UPDATE);
        assertThat(tracker.received(0, 6)).isEqualTo(LatestValueTracker.Outcome.REPEAT);
        assertThat(tracker.received(0, 5)).isEqualTo(LatestValueTracker.Outcome.REGRESSION);
        assertThat(tracker.received(1, 0)).isEqualTo(LatestValueTracker.Outcome.UPDATE);
        assertThat(tracker.received(3, 0)).isEqualTo(LatestValueTracker.Outcome.INVALID);
        assertThat(tracker.receivedMessages()).isEqualTo(6);
        // the gateways' next sequences: device 2 sent nothing
        assertThat(tracker.staleDevices(new long[] {7, 1, 0})).isZero();
        assertThat(tracker.staleDevices(new long[] {8, 1, 0})).isEqualTo(1);
        assertThat(tracker.staleDevices(new long[] {8, 2, 1})).isEqualTo(3);
        // device 0's latest sequence, 6, passes the gateways' last one, 5
        assertThat(tracker.aheadDevices(new long[] {6, 1, 0})).isEqualTo(1);
        assertThat(tracker.aheadDevices(new long[] {7, 1, 0})).isZero();
    }

    @Test
    public void writesTheStateAsTheGatewaysDo() throws Exception {
        LatestValueTracker tracker = new LatestValueTracker(2);
        tracker.received(0, 4);
        Path state = Files.createTempDirectory("state").resolve("application-state.bin");
        tracker.writeState(state);
        try (var input = new DataInputStream(Files.newInputStream(state))) {
            assertThat(input.readInt()).isEqualTo(DeviceSequenceTracker.STATE_VERSION);
            assertThat(input.readInt()).isEqualTo(2);
            assertThat(input.readLong()).isEqualTo(5);
            assertThat(input.readLong()).isZero();
        }
    }

    @Test
    public void handsTheFinalSequencesOver() throws Exception {
        Path directory = Files.createTempDirectory("final-sequences");
        assertThat(FinalSequencesMarker.read(directory, "run")).isEmpty();
        FinalSequencesMarker.mark(directory, "run", new long[] {3, 0, 9});
        assertThat(FinalSequencesMarker.read(directory, "run")).hasValueSatisfying(
                sequences -> assertThat(sequences).containsExactly(3, 0, 9));
        assertThat(FinalSequencesMarker.read(directory, "other-run")).isEmpty();
    }
}
