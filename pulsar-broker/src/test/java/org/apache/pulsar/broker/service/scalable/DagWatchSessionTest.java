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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentState;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Focused unit tests for {@link DagWatchSession}.
 *
 * <p>The deep namespace-lookup codepath inside {@code start()} (resolving per-segment
 * brokers and the controller broker) is covered by integration tests; these unit tests
 * stay in the parts that don't need a live {@code NamespaceService}: session lifecycle,
 * notification filtering, and the DAG proto built by {@link DagWatchSession#pushUpdate}.
 */
public class DagWatchSessionTest {

    private static final String TOPIC_PATH = "/admin/scalable-topics/tenant/ns/my-scalable";
    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/my-scalable");
    private static final long SESSION_ID = 42L;

    private ScalableTopicResources resources;
    private ServerCnx cnx;
    private ChannelHandlerContext ctx;
    private BrokerService brokerService;
    private DagWatchSession session;

    @BeforeMethod
    public void setup() {
        resources = mock(ScalableTopicResources.class);
        cnx = mock(ServerCnx.class);
        ctx = mock(ChannelHandlerContext.class);
        brokerService = mock(BrokerService.class);

        when(resources.topicPath(TOPIC)).thenReturn(TOPIC_PATH);
        when(cnx.ctx()).thenReturn(ctx);
        // Default: metadata store has a valid registerListener hook.
        var store = mock(org.apache.pulsar.metadata.api.MetadataStore.class);
        when(resources.getStore()).thenReturn(store);

        session = new DagWatchSession(SESSION_ID, TOPIC, cnx, resources, brokerService);
    }

    // --- identity / lifecycle ---

    @Test
    public void testSessionIdIsPreserved() {
        assertEquals(session.getSessionId(), SESSION_ID);
    }

    @Test
    public void testCloseIsIdempotent() {
        session.close();
        session.close(); // must not throw
    }

    // --- start() ---

    @Test
    public void testStartFailsWhenTopicMetadataMissing() {
        when(resources.getScalableTopicMetadataAsync(TOPIC, true))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        CompletableFuture<ScalableTopicLayoutResponse> future = session.start();

        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("expected failure");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof IllegalStateException, "got: " + cause);
            assertTrue(cause.getMessage().contains("not found"), cause.getMessage());
        }
    }

    @Test
    public void testStartRegistersMetadataStoreListener() {
        // Regardless of outcome, start() should wire up a notification listener so that
        // subsequent metadata changes flow into the session.
        when(resources.getScalableTopicMetadataAsync(TOPIC, true))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        session.start();

        verify(resources.getStore()).registerListener(any());
    }

    // --- onNotification filtering ---

    @Test
    public void testNotificationForUnrelatedPathIsIgnored() {
        session.onNotification(new Notification(NotificationType.Modified, "/some/other/path"));

        verify(resources, never()).getScalableTopicMetadataAsync(any(), anyBoolean());
    }

    @Test
    public void testDeletedNotificationIsIgnored() {
        session.onNotification(new Notification(NotificationType.Deleted, TOPIC_PATH));

        verify(resources, never()).getScalableTopicMetadataAsync(any(), anyBoolean());
    }

    @Test
    public void testNotificationAfterCloseIsIgnored() {
        session.close();
        session.onNotification(new Notification(NotificationType.Modified, TOPIC_PATH));

        verify(resources, never()).getScalableTopicMetadataAsync(any(), anyBoolean());
    }

    @Test
    public void testNotificationOnMatchingPathTriggersReload() {
        // Return an empty optional so we stop before the NamespaceService calls inside
        // buildResponse — we only care that the reload was kicked off.
        when(resources.getScalableTopicMetadataAsync(TOPIC, true))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        session.onNotification(new Notification(NotificationType.Modified, TOPIC_PATH));

        verify(resources, times(1)).getScalableTopicMetadataAsync(TOPIC, true);
    }

    // --- pushUpdate ---

    @Test
    public void testPushUpdateWritesDagToConnection() {
        ScalableTopicLayoutResponse response = buildSampleResponse(
                7L,
                Map.of(
                        0L, seg(0, 0x0000, 0x7FFF, SegmentState.SEALED, new long[]{}, new long[]{2L, 3L}, 0L, 5L),
                        2L, seg(2, 0x0000, 0x3FFF, SegmentState.ACTIVE, new long[]{0L}, new long[]{}, 7L, -1L),
                        3L, seg(3, 0x4000, 0x7FFF, SegmentState.ACTIVE, new long[]{0L}, new long[]{}, 7L, -1L)),
                Map.of(
                        2L, "pulsar://broker-a:6650",
                        3L, "pulsar://broker-b:6650"));

        session.pushUpdate(response);

        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(ctx).writeAndFlush(captor.capture());

        BaseCommand cmd = parseFrame(captor.getValue());
        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_UPDATE);
        assertEquals(cmd.getScalableTopicUpdate().getSessionId(), SESSION_ID);

        var dag = cmd.getScalableTopicUpdate().getDag();
        assertEquals(dag.getEpoch(), 7L);
        assertEquals(dag.getSegmentsCount(), 3);

        // sealed parent should not have parentIds; its childIds should be 2, 3
        var parent = findSegment(dag, 0L);
        assertEquals(parent.getState(), org.apache.pulsar.common.api.proto.SegmentState.SEALED);
        assertEquals(parent.getChildIdsCount(), 2);
        assertEquals(parent.getChildIdAt(0), 2L);
        assertEquals(parent.getChildIdAt(1), 3L);
        assertEquals(parent.getCreatedAtEpoch(), 0L);
        assertEquals(parent.getSealedAtEpoch(), 5L);

        // active children should reference parent 0
        var childA = findSegment(dag, 2L);
        assertEquals(childA.getState(), org.apache.pulsar.common.api.proto.SegmentState.ACTIVE);
        assertEquals(childA.getParentIdsCount(), 1);
        assertEquals(childA.getParentIdAt(0), 0L);
        assertEquals(childA.getCreatedAtEpoch(), 7L);
        // sealedAtEpoch is only written when non-negative
        assertTrue(!childA.hasSealedAtEpoch() || childA.getSealedAtEpoch() == 0,
                "active segment should not have sealedAtEpoch set");

        // broker addresses only for the 2 active segments
        assertEquals(dag.getSegmentBrokersCount(), 2);
        Map<Long, String> brokerAddrs = new LinkedHashMap<>();
        for (int i = 0; i < dag.getSegmentBrokersCount(); i++) {
            brokerAddrs.put(dag.getSegmentBrokerAt(i).getSegmentId(),
                    dag.getSegmentBrokerAt(i).getBrokerUrl());
        }
        assertEquals(brokerAddrs.get(2L), "pulsar://broker-a:6650");
        assertEquals(brokerAddrs.get(3L), "pulsar://broker-b:6650");
    }

    @Test
    public void testPushUpdateAfterCloseIsNoop() {
        ScalableTopicLayoutResponse response = buildSampleResponse(
                0L,
                Map.of(0L, seg(0, 0x0000, 0xFFFF, SegmentState.ACTIVE, new long[]{}, new long[]{}, 0L, -1L)),
                Map.of());

        session.close();
        session.pushUpdate(response);

        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    public void testPushUpdateWithNullBrokerAddressMapOmitsBrokerSection() {
        // buildDagProto guards against a null address map (e.g., when upstream namespace
        // lookup short-circuits) and should not throw.
        ScalableTopicLayoutResponse response = new ScalableTopicLayoutResponse(
                1L,
                Map.of(0L, seg(0, 0x0000, 0xFFFF, SegmentState.ACTIVE, new long[]{}, new long[]{}, 0L, -1L)),
                null, null, null, null);

        session.pushUpdate(response);

        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(ctx).writeAndFlush(captor.capture());
        BaseCommand cmd = parseFrame(captor.getValue());
        assertEquals(cmd.getScalableTopicUpdate().getDag().getSegmentBrokersCount(), 0);
    }

    // --- onMetadataChanged ---

    @Test
    public void testOnMetadataChangedAfterCloseIsNoop() {
        session.close();
        // Build a minimal metadata object; close should short-circuit before any work runs.
        ScalableTopicMetadata md = ScalableTopicController.createInitialMetadata(1, Map.of());
        session.onMetadataChanged(md);

        verify(ctx, never()).writeAndFlush(any());
    }

    // ==== helpers ====

    private static SegmentInfo seg(long id, int start, int end, SegmentState state,
                                   long[] parents, long[] children,
                                   long createdAt, long sealedAt) {
        return new SegmentInfo(
                id,
                HashRange.of(start, end),
                state,
                toList(parents),
                toList(children),
                createdAt,
                sealedAt);
    }

    private static java.util.List<Long> toList(long[] arr) {
        java.util.List<Long> out = new java.util.ArrayList<>(arr.length);
        for (long v : arr) {
            out.add(v);
        }
        return out;
    }

    private static ScalableTopicLayoutResponse buildSampleResponse(
            long epoch,
            Map<Long, SegmentInfo> segments,
            Map<Long, String> brokerAddrs) {
        return new ScalableTopicLayoutResponse(epoch, segments, brokerAddrs, null, null, null);
    }

    private static org.apache.pulsar.common.api.proto.SegmentInfoProto findSegment(
            org.apache.pulsar.common.api.proto.ScalableTopicDAG dag, long id) {
        for (int i = 0; i < dag.getSegmentsCount(); i++) {
            if (dag.getSegmentAt(i).getSegmentId() == id) {
                return dag.getSegmentAt(i);
            }
        }
        fail("segment " + id + " not found");
        return null;
    }

    private static BaseCommand parseFrame(ByteBuf frame) {
        assertNotNull(frame);
        try {
            frame.skipBytes(4); // total size
            int cmdSize = (int) frame.readUnsignedInt();
            BaseCommand cmd = new BaseCommand();
            cmd.parseFrom(frame, cmdSize);
            // materialize() copies fields out of the backing buffer so it's safe to
            // release the frame before the caller reads fields back.
            cmd.materialize();
            return cmd;
        } finally {
            frame.release();
        }
    }
}
