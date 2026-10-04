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
import org.testng.annotations.Test;

public class CatchUpTrackerTest {
    @Test
    public void catchesUpWhenEveryTopicDeliveredWithinTheThresholdAfterJoining() {
        CatchUpTracker tracker = new CatchUpTracker(2, 1_000);
        // before the application joins, nothing counts
        tracker.received("t0", 10_000, 10_100, () -> 1);
        tracker.joined(20_000);
        assertThat(tracker.caughtUpEpochMs()).isZero();
        // an old message, beyond the threshold, then one topic caught up at the threshold's boundary
        tracker.received("t0", 1_000, 21_000, () -> 2);
        tracker.received("t0", 21_000, 22_000, () -> 3);
        assertThat(tracker.caughtUpEpochMs()).isZero();
        // the other topic too: caught up, with the messages received by then
        tracker.received("t1", 22_500, 23_000, () -> 400);
        assertThat(tracker.caughtUpEpochMs()).isEqualTo(23_000);
        assertThat(tracker.messagesWhenCaughtUp()).isEqualTo(400);
        // the first catch-up counts
        tracker.received("t1", 30_000, 30_001, () -> 500);
        assertThat(tracker.caughtUpEpochMs()).isEqualTo(23_000);
        assertThat(tracker.messagesWhenCaughtUp()).isEqualTo(400);
        assertThat(tracker.joinEpochMs()).isEqualTo(20_000);
        assertThat(tracker.thresholdMillis()).isEqualTo(1_000);
    }

    @Test
    public void catchesUpAtTheLatestTopicsReceiptWhenListenersRecordOutOfOrder() {
        CatchUpTracker tracker = new CatchUpTracker(2, 1_000);
        tracker.joined(20_000);
        // concurrent listeners: t1's receipt at 23,000 is recorded before t0's earlier one at 22,000
        tracker.received("t1", 22_900, 23_000, () -> 10);
        tracker.received("t0", 21_900, 22_000, () -> 11);
        assertThat(tracker.caughtUpEpochMs()).isEqualTo(23_000);
    }

    @Test
    public void anApplicationThatJoinedAtTheStartDoesntCatchUp() {
        CatchUpTracker tracker = new CatchUpTracker(1, 1_000);
        tracker.received("t0", 1_000, 1_010, () -> 1);
        assertThat(tracker.caughtUpEpochMs()).isZero();
    }
}
