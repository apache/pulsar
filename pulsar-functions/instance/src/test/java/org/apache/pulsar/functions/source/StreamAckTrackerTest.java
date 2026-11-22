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
package org.apache.pulsar.functions.source;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class StreamAckTrackerTest {

    @Test
    public void testInOrderCompletionAcknowledgesEachRecord() {
        List<Integer> acks = new ArrayList<>();
        StreamAckTracker<Integer> tracker = new StreamAckTracker<>(acks::add);
        StreamAckTracker.Entry<Integer> e1 = tracker.track(1);
        StreamAckTracker.Entry<Integer> e2 = tracker.track(2);
        tracker.complete(e1);
        tracker.complete(e2);
        assertThat(acks).containsExactly(1, 2);
        assertThat(tracker.inFlightCount()).isZero();
    }

    @Test
    public void testOutOfOrderCompletionWaitsForEarlierRecords() {
        List<Integer> acks = new ArrayList<>();
        StreamAckTracker<Integer> tracker = new StreamAckTracker<>(acks::add);
        StreamAckTracker.Entry<Integer> e1 = tracker.track(1);
        StreamAckTracker.Entry<Integer> e2 = tracker.track(2);
        StreamAckTracker.Entry<Integer> e3 = tracker.track(3);
        StreamAckTracker.Entry<Integer> e4 = tracker.track(4);

        tracker.complete(e3);
        tracker.complete(e2);
        // record 1 is still in flight, so nothing may be acknowledged
        assertThat(acks).isEmpty();

        tracker.complete(e1);
        // one cumulative acknowledgment covers records 1 to 3
        assertThat(acks).containsExactly(3);
        assertThat(tracker.inFlightCount()).isEqualTo(1);

        tracker.complete(e4);
        assertThat(acks).containsExactly(3, 4);
    }

    @Test
    public void testCompleteThroughAcknowledgesEarlierRecords() {
        List<Integer> acks = new ArrayList<>();
        StreamAckTracker<Integer> tracker = new StreamAckTracker<>(acks::add);
        StreamAckTracker.Entry<Integer> e1 = tracker.track(1);
        StreamAckTracker.Entry<Integer> e2 = tracker.track(2);
        StreamAckTracker.Entry<Integer> e3 = tracker.track(3);
        StreamAckTracker.Entry<Integer> e4 = tracker.track(4);

        tracker.complete(e2);
        // a cumulative acknowledgment of record 3 covers the records before it
        tracker.completeThrough(e3);
        assertThat(acks).containsExactly(3);
        assertThat(tracker.inFlightCount()).isEqualTo(1);

        // records already covered are ignored
        tracker.completeThrough(e1);
        tracker.complete(e1);
        assertThat(acks).containsExactly(3);

        tracker.completeThrough(e4);
        assertThat(acks).containsExactly(3, 4);
    }

    @Test
    public void testCompletingTwiceIsIgnored() {
        List<Integer> acks = new ArrayList<>();
        StreamAckTracker<Integer> tracker = new StreamAckTracker<>(acks::add);
        StreamAckTracker.Entry<Integer> e1 = tracker.track(1);
        tracker.complete(e1);
        tracker.complete(e1);
        assertThat(acks).containsExactly(1);
    }
}
