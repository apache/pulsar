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
package org.apache.pulsar.broker.service.scalable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.scalable.HashRange;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class ConsumerSessionTest {

    private static final Duration GRACE = Duration.ofMillis(100);
    private static final Logger PARENT_LOG = Logger.get(ConsumerSessionTest.class);

    /** Convenience wrapper: fresh mock scheduler that returns a mock ScheduledFuture. */
    private static final class TestContext {
        final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        final ScheduledFuture<?> future = mock(ScheduledFuture.class);
        final Runnable onGraceExpiry = mock(Runnable.class);

        @SuppressWarnings({"unchecked", "rawtypes"})
        TestContext() {
            // Every schedule(...) call returns the same mock future so we can verify cancel.
            when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                    .thenReturn((ScheduledFuture) future);
        }

        ConsumerSession session(String name, long consumerId, TransportCnx cnx) {
            return new ConsumerSession(name, consumerId, cnx, GRACE, scheduler, onGraceExpiry, PARENT_LOG);
        }

        ConsumerSession restored(String name) {
            return ConsumerSession.restored(name, GRACE, scheduler, onGraceExpiry, PARENT_LOG);
        }
    }

    private static ConsumerAssignment buildAssignment(long epoch, long... segmentIds) {
        List<ConsumerAssignment.AssignedSegment> segments = new java.util.ArrayList<>();
        for (int i = 0; i < segmentIds.length; i++) {
            long id = segmentIds[i];
            int start = i * 0x4000;
            int end = start + 0x3FFF;
            segments.add(new ConsumerAssignment.AssignedSegment(
                    id, HashRange.of(start, end),
                    "persistent://tenant/ns/my-scalable-seg-" + id));
        }
        return new ConsumerAssignment(epoch, segments);
    }

    // --- Construction / identity ---

    @Test
    public void testConstructionWithConnectionMarksConnected() {
        TestContext ctx = new TestContext();
        TransportCnx cnx = mock(TransportCnx.class);

        ConsumerSession session = ctx.session("c1", 10L, cnx);

        assertEquals(session.getConsumerName(), "c1");
        assertEquals(session.getConsumerId(), 10L);
        assertSame(session.getCnx(), cnx);
        assertTrue(session.isConnected());
        assertNull(session.getGraceTimer(), "no timer is armed until markDisconnected");
        verify(ctx.scheduler, never())
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testRestoredStartsDisconnectedWithGraceTimerArmed() {
        TestContext ctx = new TestContext();

        ConsumerSession session = ctx.restored("c-restored");

        assertEquals(session.getConsumerName(), "c-restored");
        assertEquals(session.getConsumerId(), -1L);
        assertNull(session.getCnx());
        assertFalse(session.isConnected());
        // restored() arms the grace timer internally so callers can't forget to schedule it.
        assertNotNull(session.getGraceTimer(), "restored() must arm the grace timer");
        verify(ctx.scheduler, times(1))
                .schedule(eq(ctx.onGraceExpiry), eq(GRACE.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testEqualsAndHashCodeOnConsumerNameOnly() {
        TestContext ctx = new TestContext();
        TransportCnx cnx1 = mock(TransportCnx.class);
        TransportCnx cnx2 = mock(TransportCnx.class);
        ConsumerSession a = ctx.session("same", 1L, cnx1);
        ConsumerSession b = ctx.session("same", 2L, cnx2);
        ConsumerSession c = ctx.session("other", 1L, cnx1);

        assertEquals(a, b, "sessions with the same consumerName must be equal");
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    // --- attach / markDisconnected ---

    @Test
    public void testAttachUpdatesIdAndCnxAndMarksConnected() {
        TestContext ctx = new TestContext();
        ConsumerSession session = ctx.restored("c1");
        TransportCnx fresh = mock(TransportCnx.class);

        session.attach(77L, fresh);

        assertEquals(session.getConsumerId(), 77L);
        assertSame(session.getCnx(), fresh);
        assertTrue(session.isConnected());
    }

    @Test
    public void testAttachCancelsPendingGraceTimer() {
        TestContext ctx = new TestContext();
        // restored() arms the timer via the scheduler mock; attach should cancel it.
        ConsumerSession session = ctx.restored("c1");
        assertSame(session.getGraceTimer(), ctx.future);

        session.attach(1L, mock(TransportCnx.class));

        verify(ctx.future).cancel(false);
        assertNull(session.getGraceTimer());
    }

    @Test
    public void testMarkDisconnectedClearsCnxKeepsConsumerIdAndArmsTimer() {
        TestContext ctx = new TestContext();
        TransportCnx cnx = mock(TransportCnx.class);
        ConsumerSession session = ctx.session("c1", 10L, cnx);

        session.markDisconnected();

        assertFalse(session.isConnected());
        assertNull(session.getCnx());
        assertEquals(session.getConsumerId(), 10L, "consumerId is preserved until reattach");
        assertSame(session.getGraceTimer(), ctx.future, "markDisconnected arms the grace timer");
        verify(ctx.scheduler, times(1))
                .schedule(eq(ctx.onGraceExpiry), eq(GRACE.toMillis()), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testMarkDisconnectedTwiceCancelsPreviousTimer() {
        // startGraceTimer cancels any prior timer before scheduling a new one; a double
        // markDisconnected (edge case) should leave only the latest timer armed.
        TestContext ctx = new TestContext();
        @SuppressWarnings({"unchecked", "rawtypes"})
        ScheduledFuture<?> second = mock(ScheduledFuture.class);
        when(ctx.scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenReturn((ScheduledFuture) ctx.future, (ScheduledFuture) second);

        ConsumerSession session = ctx.session("c1", 10L, mock(TransportCnx.class));

        session.markDisconnected();
        session.markDisconnected();

        verify(ctx.future).cancel(false);
        assertSame(session.getGraceTimer(), second);
    }

    @Test
    public void testCancelGraceTimerIsIdempotent() {
        TestContext ctx = new TestContext();
        ConsumerSession session = ctx.restored("c1");
        assertSame(session.getGraceTimer(), ctx.future);

        session.cancelGraceTimer();
        session.cancelGraceTimer(); // second call is a no-op

        verify(ctx.future, times(1)).cancel(false);
        assertNull(session.getGraceTimer());
    }

    // --- sendAssignmentUpdate ---

    @Test
    public void testSendAssignmentUpdateWritesViaCommandSender() {
        TestContext ctx = new TestContext();
        TransportCnx cnx = mock(TransportCnx.class);
        PulsarCommandSender sender = mock(PulsarCommandSender.class);
        when(cnx.getCommandSender()).thenReturn(sender);
        ConsumerSession session = ctx.session("c1", 42L, cnx);

        session.sendAssignmentUpdate(buildAssignment(5L, 0L, 1L));

        ArgumentCaptor<ScalableConsumerAssignment> captor =
                ArgumentCaptor.forClass(ScalableConsumerAssignment.class);
        verify(sender).sendScalableTopicAssignmentUpdate(eq(42L), captor.capture());
        ScalableConsumerAssignment proto = captor.getValue();
        assertEquals(proto.getLayoutEpoch(), 5L);
        assertEquals(proto.getSegmentsCount(), 2);
        assertEquals(proto.getSegmentAt(0).getSegmentId(), 0L);
        assertEquals(proto.getSegmentAt(1).getSegmentId(), 1L);
    }

    @Test
    public void testSendAssignmentUpdateIsNoopWhenDisconnected() {
        TestContext ctx = new TestContext();
        ConsumerSession session = ctx.restored("c1");
        // restored() leaves the session disconnected with no cnx — should not NPE, should not call anything.
        session.sendAssignmentUpdate(buildAssignment(0L, 0L));
        // no mocks to verify — just ensure no exception
    }

    @Test
    public void testSendAssignmentUpdateIsNoopAfterMarkDisconnected() {
        TestContext ctx = new TestContext();
        TransportCnx cnx = mock(TransportCnx.class);
        PulsarCommandSender sender = mock(PulsarCommandSender.class);
        when(cnx.getCommandSender()).thenReturn(sender);
        ConsumerSession session = ctx.session("c1", 10L, cnx);

        session.markDisconnected();
        session.sendAssignmentUpdate(buildAssignment(0L, 0L));

        verify(sender, never()).sendScalableTopicAssignmentUpdate(anyLong(), any());
    }

    @Test
    public void testSendAssignmentUpdateIsNoopWhenCommandSenderNull() {
        TestContext ctx = new TestContext();
        TransportCnx cnx = mock(TransportCnx.class);
        when(cnx.getCommandSender()).thenReturn(null); // connection tearing down
        ConsumerSession session = ctx.session("c1", 10L, cnx);

        // no exception, no interaction
        session.sendAssignmentUpdate(buildAssignment(0L, 0L));
    }

    // --- toProto ---

    @Test
    public void testToProtoConvertsAllFields() {
        ConsumerAssignment assignment = buildAssignment(9L, 3L, 4L, 5L);

        ScalableConsumerAssignment proto = ConsumerSession.toProto(assignment);

        assertEquals(proto.getLayoutEpoch(), 9L);
        assertEquals(proto.getSegmentsCount(), 3);
        // hash ranges are derived in buildAssignment as 0x4000-wide contiguous ranges
        assertEquals(proto.getSegmentAt(0).getSegmentId(), 3L);
        assertEquals(proto.getSegmentAt(0).getHashStart(), 0x0000);
        assertEquals(proto.getSegmentAt(0).getHashEnd(), 0x3FFF);
        assertEquals(proto.getSegmentAt(0).getSegmentTopic(),
                "persistent://tenant/ns/my-scalable-seg-3");
        assertEquals(proto.getSegmentAt(1).getSegmentId(), 4L);
        assertEquals(proto.getSegmentAt(1).getHashStart(), 0x4000);
        assertEquals(proto.getSegmentAt(2).getSegmentId(), 5L);
        assertEquals(proto.getSegmentAt(2).getHashStart(), 0x8000);
    }

    @Test
    public void testToProtoHandlesEmptyAssignment() {
        ConsumerAssignment empty = new ConsumerAssignment(0L, List.of());

        ScalableConsumerAssignment proto = ConsumerSession.toProto(empty);

        assertEquals(proto.getLayoutEpoch(), 0L);
        assertEquals(proto.getSegmentsCount(), 0);
    }
}
