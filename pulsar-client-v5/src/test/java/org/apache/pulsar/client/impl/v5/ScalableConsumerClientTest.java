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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.api.proto.ScalableConsumerType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.Test;

public class ScalableConsumerClientTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/consumer-client-test");

    @Test
    public void testSetListenerDeliversAssignmentReceivedBeforeIt() {
        ScalableConsumerClient session = newSession();
        // Arrives while the consumer is still attaching its segments, before it sets the listener.
        session.onAssignmentUpdate(assignment(0L, 1L));
        List<List<Long>> delivered = new ArrayList<>();

        session.setListener((newSegments, oldSegments) -> delivered.add(segmentIds(newSegments)));

        assertThat(delivered).containsExactly(List.of(0L, 1L));
    }

    @Test(timeOut = 30_000)
    public void testAssignmentArrivingWhileListenerRunsIsDeliveredAfterIt() throws Exception {
        ScalableConsumerClient session = newSession();
        session.onAssignmentUpdate(assignment(0L));
        List<List<Long>> delivered = new CopyOnWriteArrayList<>();
        AtomicReference<List<Long>> applied = new AtomicReference<>();
        CountDownLatch delivering = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        // The consumer sets the listener on the thread that finished attaching its initial segments.
        Thread caller = new Thread(() -> session.setListener((newSegments, oldSegments) -> {
            delivered.add(segmentIds(newSegments));
            if (delivered.size() == 1) {
                // Hold the replay before it is applied, as a thread preempted there would be.
                delivering.countDown();
                awaitUninterruptibly(resume);
            }
            // The consumers converge onto the assignment they were handed last.
            applied.set(segmentIds(newSegments));
        }), "set-listener");
        caller.start();
        List<List<Long>> deliveredDuringReplay;
        try {
            assertThat(delivering.await(10, TimeUnit.SECONDS)).isTrue();

            // The group moves segment 0 to another member and gives this one segment 1, on the I/O thread,
            // while the caller's thread is still replaying the older assignment: the I/O thread must neither
            // wait for that delivery nor call the listener alongside it.
            session.onAssignmentUpdate(assignment(1L));
            deliveredDuringReplay = List.copyOf(delivered);
        } finally {
            // Let the caller's thread finish even if an assertion failed.
            resume.countDown();
            caller.join();
        }
        assertThat(applied.get()).as("assignment applied last").containsExactly(1L);
        assertThat(deliveredDuringReplay).as("assignments delivered while the replay was running")
                .containsExactly(List.of(0L));
        assertThat(delivered).containsExactly(List.of(0L), List.of(1L));
    }

    @Test
    public void testListenerErrorDoesNotStopLaterDeliveries() {
        ScalableConsumerClient session = newSession();
        session.onAssignmentUpdate(assignment(0L));
        List<List<Long>> delivered = new ArrayList<>();
        session.setListener((newSegments, oldSegments) -> {
            delivered.add(segmentIds(newSegments));
            if (delivered.size() == 2) {
                throw new Error("listener failure");
            }
        });

        // Whether or not the Error reaches the I/O thread, the assignments that follow must still be delivered.
        catchThrowable(() -> session.onAssignmentUpdate(assignment(1L)));
        session.onAssignmentUpdate(assignment(0L, 1L));

        assertThat(delivered).containsExactly(List.of(0L), List.of(1L), List.of(0L, 1L));
    }

    private static ScalableConsumerClient newSession() {
        return new ScalableConsumerClient(mock(PulsarClientImpl.class), TOPIC, "sub", "consumer",
                ScalableConsumerType.STREAM);
    }

    /** An assignment of the given whole segments, at the same layout epoch as every other one here. */
    private static ScalableConsumerAssignment assignment(long... segmentIds) {
        ScalableConsumerAssignment assignment = new ScalableConsumerAssignment().setLayoutEpoch(1L);
        HashRange range = HashRange.of(0, 0xffff);
        for (long segmentId : segmentIds) {
            assignment.addSegment()
                    .setSegmentId(segmentId)
                    .setHashStart(range.start())
                    .setHashEnd(range.end())
                    .setSegmentTopic(SegmentTopicName.fromParent(TOPIC, range, segmentId).toString());
        }
        return assignment;
    }

    private static List<Long> segmentIds(List<ActiveSegment> segments) {
        return segments.stream()
                .map(ActiveSegment::segmentId)
                .toList();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
