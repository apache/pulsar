/**
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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import java.time.Clock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionsSnapshotBuilderTest {

    private final String localCluster = "a";
    private long currentTime = 0;
    private Clock clock;
    private ServiceConfiguration conf;
    private ReplicatedSubscriptionsController controller;
    private List<ByteBuf> markers;

    @BeforeMethod
    public void setup() {
        clock = mock(Clock.class);
        when(clock.millis()).thenAnswer(invocation -> currentTime);

        conf = new ServiceConfiguration();
        conf.setReplicatedSubscriptionsSnapshotTimeoutSeconds(3);

        markers = new ArrayList<>();

        controller = mock(ReplicatedSubscriptionsController.class);
        when(controller.localCluster()).thenReturn(localCluster);
        doAnswer(invocation -> {
            ByteBuf marker = invocation.getArgument(0, ByteBuf.class);
            Commands.skipMessageMetadata(marker);
            markers.add(marker);
            return null;
        }).when(controller)
                .writeMarker(any(ByteBuf.class));
    }

    @Test
    public void testBuildSnapshotWith2Clusters() throws Exception {
        List<String> remoteClusters = Collections.singletonList("b");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertTrue(markers.isEmpty());

        builder.start();

        // Should have sent out a marker to initiate the snapshot
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshotRequest request = Markers
                .parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);

        // Simulate the responses coming back
        ReplicatedSubscriptionsSnapshotResponse response = new ReplicatedSubscriptionsSnapshotResponse()
                .setSnapshotId("snapshot-1");
        response.setCluster()
                .setCluster("b")
                .setMessageId()
                .setLedgerId(11)
                .setEntryId(11);
        builder.receivedSnapshotResponse(new PositionImpl(1, 1), response);

        // At this point the snapshot should be created
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(markers.remove(0));
        assertEquals(snapshot.getClustersCount(), 1);
        assertEquals(snapshot.getClusterAt(0).getCluster(), "b");
        assertEquals(snapshot.getClusterAt(0).getMessageId().getLedgerId(), 11);
        assertEquals(snapshot.getClusterAt(0).getMessageId().getEntryId(), 11);

        assertEquals(snapshot.getLocalMessageId().getLedgerId(), 1);
        assertEquals(snapshot.getLocalMessageId().getEntryId(), 1);
    }

    @Test
    public void testBuildSnapshotWith3Clusters() throws Exception {
        List<String> remoteClusters = Arrays.asList("b", "c");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertTrue(markers.isEmpty());

        builder.start();

        // Should have sent out a marker to initiate the snapshot
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshotRequest request = Markers
                .parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);

        // Simulate the responses coming back
        ReplicatedSubscriptionsSnapshotResponse response1 = new ReplicatedSubscriptionsSnapshotResponse()
                .setSnapshotId("snapshot-1");
        response1.setCluster()
                .setCluster("b")
                .setMessageId()
                .setLedgerId(11)
                .setEntryId(11);
        builder.receivedSnapshotResponse(new PositionImpl(1, 1), response1);

        // No markers should be sent out
        assertTrue(markers.isEmpty());

        ReplicatedSubscriptionsSnapshotResponse response2 = new ReplicatedSubscriptionsSnapshotResponse()
                .setSnapshotId("snapshot-1");
        response2.setCluster()
                .setCluster("c")
                .setMessageId()
                .setLedgerId(22)
                .setEntryId(22);
        builder.receivedSnapshotResponse(new PositionImpl(2, 2), response2);

        // Since we have 2 remote clusters, a 2nd round of snapshot will be taken
        assertEquals(markers.size(), 1);
        request = Markers.parseReplicatedSubscriptionsSnapshotRequest(markers.remove(0));
        assertEquals(request.getSourceCluster(), localCluster);

        // Responses coming back
        ReplicatedSubscriptionsSnapshotResponse response3 = new ReplicatedSubscriptionsSnapshotResponse()
                .setSnapshotId("snapshot-1");
        response3.setCluster()
                .setCluster("b")
                .setMessageId()
                .setLedgerId(33)
                .setEntryId(33);
        builder.receivedSnapshotResponse(new PositionImpl(3, 3), response3);

        // No markers should be sent out
        assertTrue(markers.isEmpty());

        ReplicatedSubscriptionsSnapshotResponse response4 = new ReplicatedSubscriptionsSnapshotResponse()
                .setSnapshotId("snapshot-1");
        response4.setCluster()
                .setCluster("c")
                .setMessageId()
                .setLedgerId(44)
                .setEntryId(44);
        builder.receivedSnapshotResponse(new PositionImpl(4, 4), response4);

        // At this point the snapshot should be created
        assertEquals(markers.size(), 1);
        ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(markers.remove(0));
        assertEquals(snapshot.getClustersCount(), 2);
        assertEquals(snapshot.getClusterAt(0).getCluster(), "b");
        assertEquals(snapshot.getClusterAt(0).getMessageId().getLedgerId(), 11);
        assertEquals(snapshot.getClusterAt(0).getMessageId().getEntryId(), 11);

        assertEquals(snapshot.getClusterAt(1).getCluster(), "c");
        assertEquals(snapshot.getClusterAt(1).getMessageId().getLedgerId(), 22);
        assertEquals(snapshot.getClusterAt(1).getMessageId().getEntryId(), 22);

        assertEquals(snapshot.getLocalMessageId().getLedgerId(), 4);
        assertEquals(snapshot.getLocalMessageId().getEntryId(), 4);
    }

    @Test
    public void testBuildTimeout() {
        List<String> remoteClusters = Collections.singletonList("b");

        ReplicatedSubscriptionsSnapshotBuilder builder = new ReplicatedSubscriptionsSnapshotBuilder(controller,
                remoteClusters, conf, clock);

        assertFalse(builder.isTimedOut());

        builder.start();

        currentTime = 2000;

        assertFalse(builder.isTimedOut());

        currentTime = 5000;

        assertTrue(builder.isTimedOut());
    }
}
