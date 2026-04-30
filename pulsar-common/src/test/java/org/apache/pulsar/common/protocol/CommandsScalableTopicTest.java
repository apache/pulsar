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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.ScalableConsumerAssignment;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentInfoProto;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.Test;

/**
 * Roundtrip tests for the {@code Commands.newScalableTopic*} factory methods:
 * encode a command, reparse the serialized wire frame, and verify the fields
 * survive the trip.
 */
public class CommandsScalableTopicTest {

    /**
     * Wire format emitted by {@link Commands#serializeWithPrecalculatedSerializedSize}:
     * {@code [4-byte TOTAL_SIZE][4-byte CMD_SIZE][CMD bytes]}. Parse it back into a
     * {@link BaseCommand} for assertions.
     */
    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4); // total size
            int cmdSize = (int) frame.readUnsignedInt();
            BaseCommand cmd = new BaseCommand();
            cmd.parseFrom(frame, cmdSize);
            // Materialize copies every field out of the backing buffer so it's safe to
            // release {@code frame} before the caller reads fields back.
            cmd.materialize();
            return cmd;
        } finally {
            frame.release();
        }
    }

    @Test
    public void testNewScalableTopicLookup() {
        ByteBuf frame = Commands.newScalableTopicLookup(42L, "topic://tenant/ns/my-scalable");
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_LOOKUP);
        assertTrue(cmd.hasScalableTopicLookup());
        assertEquals(cmd.getScalableTopicLookup().getSessionId(), 42L);
        assertEquals(cmd.getScalableTopicLookup().getTopic(), "topic://tenant/ns/my-scalable");
    }

    @Test
    public void testNewScalableTopicClose() {
        ByteBuf frame = Commands.newScalableTopicClose(99L);
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_CLOSE);
        assertTrue(cmd.hasScalableTopicClose());
        assertEquals(cmd.getScalableTopicClose().getSessionId(), 99L);
    }

    @Test
    public void testNewScalableTopicUpdate() {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(7L);
        SegmentInfoProto active = dag.addSegment()
                .setSegmentId(0L)
                .setHashStart(0x0000)
                .setHashEnd(0x7FFF)
                .setState(SegmentState.ACTIVE)
                .setCreatedAtEpoch(0L);
        active.addChildId(2L);
        active.addChildId(3L);
        dag.addSegment()
                .setSegmentId(2L)
                .setHashStart(0x0000)
                .setHashEnd(0x3FFF)
                .setState(SegmentState.ACTIVE)
                .setCreatedAtEpoch(7L)
                .addParentId(0L);
        dag.addSegmentBroker().setSegmentId(2L).setBrokerUrl("pulsar://broker-a:6650");

        ByteBuf frame = Commands.newScalableTopicUpdate(77L, dag);
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_UPDATE);
        assertTrue(cmd.hasScalableTopicUpdate());
        assertEquals(cmd.getScalableTopicUpdate().getSessionId(), 77L);
        assertFalse(cmd.getScalableTopicUpdate().hasError(),
                "successful update must not carry an error field");

        ScalableTopicDAG got = cmd.getScalableTopicUpdate().getDag();
        assertEquals(got.getEpoch(), 7L);
        assertEquals(got.getSegmentsCount(), 2);
        SegmentInfoProto parent = got.getSegmentAt(0);
        assertEquals(parent.getSegmentId(), 0L);
        assertEquals(parent.getState(), SegmentState.ACTIVE);
        assertEquals(parent.getChildIdsCount(), 2);
        assertEquals(parent.getChildIdAt(0), 2L);
        assertEquals(parent.getChildIdAt(1), 3L);

        SegmentInfoProto child = got.getSegmentAt(1);
        assertEquals(child.getSegmentId(), 2L);
        assertEquals(child.getHashStart(), 0x0000);
        assertEquals(child.getHashEnd(), 0x3FFF);
        assertEquals(child.getParentIdsCount(), 1);
        assertEquals(child.getParentIdAt(0), 0L);

        assertEquals(got.getSegmentBrokersCount(), 1);
        assertEquals(got.getSegmentBrokerAt(0).getSegmentId(), 2L);
        assertEquals(got.getSegmentBrokerAt(0).getBrokerUrl(), "pulsar://broker-a:6650");
    }

    @Test
    public void testNewScalableTopicError() {
        ByteBuf frame = Commands.newScalableTopicError(15L, ServerError.TopicNotFound,
                "Scalable topic not found: topic://t/n/x");
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_UPDATE);
        assertTrue(cmd.hasScalableTopicUpdate());
        assertEquals(cmd.getScalableTopicUpdate().getSessionId(), 15L);
        assertTrue(cmd.getScalableTopicUpdate().hasError());
        assertEquals(cmd.getScalableTopicUpdate().getError(), ServerError.TopicNotFound);
        assertEquals(cmd.getScalableTopicUpdate().getMessage(),
                "Scalable topic not found: topic://t/n/x");
    }

    @Test
    public void testNewScalableTopicSubscribeResponseSuccess() {
        ScalableConsumerAssignment assignment = new ScalableConsumerAssignment().setLayoutEpoch(3L);
        assignment.addSegment()
                .setSegmentId(2L)
                .setHashStart(0x0000)
                .setHashEnd(0x3FFF)
                .setSegmentTopic("persistent://tenant/ns/my-scalable-0000-3fff-0000000000000002");
        assignment.addSegment()
                .setSegmentId(3L)
                .setHashStart(0x4000)
                .setHashEnd(0x7FFF)
                .setSegmentTopic("persistent://tenant/ns/my-scalable-4000-7fff-0000000000000003");

        ByteBuf frame = Commands.newScalableTopicSubscribeResponse(123L, assignment);
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE);
        assertTrue(cmd.hasScalableTopicSubscribeResponse());
        assertEquals(cmd.getScalableTopicSubscribeResponse().getRequestId(), 123L);
        assertFalse(cmd.getScalableTopicSubscribeResponse().hasError());

        ScalableConsumerAssignment got = cmd.getScalableTopicSubscribeResponse().getAssignment();
        assertEquals(got.getLayoutEpoch(), 3L);
        assertEquals(got.getSegmentsCount(), 2);
        assertEquals(got.getSegmentAt(0).getSegmentId(), 2L);
        assertEquals(got.getSegmentAt(0).getHashStart(), 0x0000);
        assertEquals(got.getSegmentAt(0).getHashEnd(), 0x3FFF);
        assertEquals(got.getSegmentAt(0).getSegmentTopic(),
                "persistent://tenant/ns/my-scalable-0000-3fff-0000000000000002");
        assertEquals(got.getSegmentAt(1).getSegmentId(), 3L);
    }

    @Test
    public void testNewScalableTopicSubscribeError() {
        ByteBuf frame = Commands.newScalableTopicSubscribeError(456L,
                ServerError.AuthorizationError, "not authorized");
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_SUBSCRIBE_RESPONSE);
        assertTrue(cmd.hasScalableTopicSubscribeResponse());
        assertEquals(cmd.getScalableTopicSubscribeResponse().getRequestId(), 456L);
        assertTrue(cmd.getScalableTopicSubscribeResponse().hasError());
        assertEquals(cmd.getScalableTopicSubscribeResponse().getError(), ServerError.AuthorizationError);
        assertEquals(cmd.getScalableTopicSubscribeResponse().getMessage(), "not authorized");
    }

    @Test
    public void testNewScalableTopicAssignmentUpdate() {
        ScalableConsumerAssignment assignment = new ScalableConsumerAssignment().setLayoutEpoch(11L);
        assignment.addSegment()
                .setSegmentId(5L)
                .setHashStart(0x8000)
                .setHashEnd(0xBFFF)
                .setSegmentTopic("persistent://t/n/seg-5");

        ByteBuf frame = Commands.newScalableTopicAssignmentUpdate(789L, assignment);
        BaseCommand cmd = parseFrame(frame);

        assertEquals(cmd.getType(), BaseCommand.Type.SCALABLE_TOPIC_ASSIGNMENT_UPDATE);
        assertTrue(cmd.hasScalableTopicAssignmentUpdate());
        assertEquals(cmd.getScalableTopicAssignmentUpdate().getConsumerId(), 789L);

        ScalableConsumerAssignment got = cmd.getScalableTopicAssignmentUpdate().getAssignment();
        assertNotNull(got);
        assertEquals(got.getLayoutEpoch(), 11L);
        assertEquals(got.getSegmentsCount(), 1);
        assertEquals(got.getSegmentAt(0).getSegmentId(), 5L);
        assertEquals(got.getSegmentAt(0).getHashStart(), 0x8000);
        assertEquals(got.getSegmentAt(0).getHashEnd(), 0xBFFF);
        assertEquals(got.getSegmentAt(0).getSegmentTopic(), "persistent://t/n/seg-5");
    }

    /**
     * Different session IDs must not collide — the wire layout must preserve them
     * independently across successive calls.
     */
    @Test
    public void testSessionIdIsolation() {
        BaseCommand a = parseFrame(Commands.newScalableTopicLookup(1L, "a"));
        BaseCommand b = parseFrame(Commands.newScalableTopicLookup(Long.MAX_VALUE, "b"));
        assertEquals(a.getScalableTopicLookup().getSessionId(), 1L);
        assertEquals(b.getScalableTopicLookup().getSessionId(), Long.MAX_VALUE);
        assertEquals(a.getScalableTopicLookup().getTopic(), "a");
        assertEquals(b.getScalableTopicLookup().getTopic(), "b");
    }
}
