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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.Test;

/**
 * Roundtrip tests for the {@code Commands.newWatchTcAssignments*} factory methods: encode a
 * command, reparse the serialized wire frame, and verify the fields survive the trip.
 */
public class CommandsTcAssignmentsTest {

    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4); // total size
            int cmdSize = (int) frame.readUnsignedInt();
            BaseCommand cmd = new BaseCommand();
            cmd.parseFrom(frame, cmdSize);
            cmd.materialize();
            return cmd;
        } finally {
            frame.release();
        }
    }

    @Test
    public void testNewWatchTcAssignments() {
        BaseCommand cmd = parseFrame(Commands.newWatchTcAssignments(7L));
        assertEquals(cmd.getType(), BaseCommand.Type.WATCH_TC_ASSIGNMENTS);
        assertTrue(cmd.hasWatchTcAssignments());
        assertEquals(cmd.getWatchTcAssignments().getWatchId(), 7L);
    }

    @Test
    public void testNewWatchTcAssignmentsClose() {
        BaseCommand cmd = parseFrame(Commands.newWatchTcAssignmentsClose(7L));
        assertEquals(cmd.getType(), BaseCommand.Type.WATCH_TC_ASSIGNMENTS_CLOSE);
        assertTrue(cmd.hasWatchTcAssignmentsClose());
        assertEquals(cmd.getWatchTcAssignmentsClose().getWatchId(), 7L);
    }

    @Test
    public void testNewWatchTcAssignmentsSnapshot() {
        Map<Integer, String[]> leaders = new HashMap<>();
        leaders.put(0, new String[] {"pulsar://b0:6650", "pulsar+ssl://b0:6651"});
        leaders.put(2, new String[] {"pulsar://b2:6650", null}); // partition 1 mid-election (absent)

        BaseCommand cmd = parseFrame(Commands.newWatchTcAssignmentsSnapshot(7L, 3, leaders));
        assertEquals(cmd.getType(), BaseCommand.Type.WATCH_TC_ASSIGNMENTS_UPDATE);
        assertTrue(cmd.hasWatchTcAssignmentsUpdate());
        var update = cmd.getWatchTcAssignmentsUpdate();
        assertEquals(update.getWatchId(), 7L);
        assertFalse(update.hasError());
        assertTrue(update.hasSnapshot());

        var snapshot = update.getSnapshot();
        assertEquals(snapshot.getParallelism(), 3);
        assertEquals(snapshot.getAssignmentsCount(), 2);

        // Decode into a map for order-independent assertions.
        Map<Long, String[]> decoded = new HashMap<>();
        for (int i = 0; i < snapshot.getAssignmentsCount(); i++) {
            var a = snapshot.getAssignmentAt(i);
            decoded.put(a.getTcId(), new String[] {
                    a.hasBrokerServiceUrl() ? a.getBrokerServiceUrl() : null,
                    a.hasBrokerServiceUrlTls() ? a.getBrokerServiceUrlTls() : null});
        }
        assertEquals(decoded.get(0L)[0], "pulsar://b0:6650");
        assertEquals(decoded.get(0L)[1], "pulsar+ssl://b0:6651");
        assertEquals(decoded.get(2L)[0], "pulsar://b2:6650");
        assertNull(decoded.get(2L)[1]);
        assertNull(decoded.get(1L)); // mid-election partition omitted
    }

    @Test
    public void testNewWatchTcAssignmentsError() {
        BaseCommand cmd = parseFrame(
                Commands.newWatchTcAssignmentsError(7L, ServerError.NotAllowedError, "disabled"));
        assertEquals(cmd.getType(), BaseCommand.Type.WATCH_TC_ASSIGNMENTS_UPDATE);
        var update = cmd.getWatchTcAssignmentsUpdate();
        assertEquals(update.getWatchId(), 7L);
        assertTrue(update.hasError());
        assertEquals(update.getError(), ServerError.NotAllowedError);
        assertEquals(update.getMessage(), "disabled");
        assertFalse(update.hasSnapshot());
    }

    @Test
    public void testConnectedAdvertisesTcMetadataDiscoveryFlag() {
        BaseCommand on = parseFrame(Commands.newConnected(
                /* clientProtocolVersion */ 21, /* maxMessageSize */ 1024,
                /* supportsTopicWatchers */ true, /* supportsScalableTopics */ true,
                /* supportsTcMetadataDiscovery */ true));
        assertTrue(on.getConnected().getFeatureFlags().isSupportsTcMetadataDiscovery());

        BaseCommand off = parseFrame(Commands.newConnected(
                21, 1024, true, true));
        assertFalse(off.getConnected().getFeatureFlags().isSupportsTcMetadataDiscovery());
    }
}
