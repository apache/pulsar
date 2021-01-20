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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsUpdate;
import org.testng.annotations.Test;

public class MarkersTest {
    @Test
    public void testSnapshotRequest() throws Exception {
        ByteBuf buf = Markers.newReplicatedSubscriptionsSnapshotRequest("sid", "us-west");

        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);
        assertEquals(msgMetadata.getReplicateTosCount(), 0);

        ReplicatedSubscriptionsSnapshotRequest request = Markers.parseReplicatedSubscriptionsSnapshotRequest(buf);

        assertEquals(request.getSnapshotId(), "sid");
        assertEquals(request.getSourceCluster(), "us-west");
    }

    @Test
    public void testSnapshotResponse() throws Exception {
        ByteBuf buf = Markers.newReplicatedSubscriptionsSnapshotResponse("sid", "us-west", "us-east", 5, 7);

        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);
        assertEquals(msgMetadata.getReplicateTosCount(), 1);
        assertEquals(msgMetadata.getReplicateToAt(0), "us-west");

        ReplicatedSubscriptionsSnapshotResponse response = Markers.parseReplicatedSubscriptionsSnapshotResponse(buf);

        assertEquals(response.getSnapshotId(), "sid");
        assertEquals(response.getCluster().getCluster(), "us-east");
        assertEquals(response.getCluster().getMessageId().getLedgerId(), 5);
        assertEquals(response.getCluster().getMessageId().getEntryId(), 7);
    }

    @Test
    public void testSnapshot() throws Exception {
        Map<String, MarkersMessageIdData> clusters = new TreeMap<>();
        clusters.put("us-east", new MarkersMessageIdData().setLedgerId(10).setEntryId(11));
        clusters.put("us-cent", new MarkersMessageIdData().setLedgerId(20).setEntryId(21));

        ByteBuf buf = Markers.newReplicatedSubscriptionsSnapshot("sid", "us-west", 5, 7, clusters);

        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);
        assertEquals(msgMetadata.getReplicateTosCount(), 1);
        assertEquals(msgMetadata.getReplicateToAt(0), "us-west");

        ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(buf);

        assertEquals(snapshot.getSnapshotId(), "sid");

        assertEquals(snapshot.getLocalMessageId().getLedgerId(), 5);
        assertEquals(snapshot.getLocalMessageId().getEntryId(), 7);

        assertEquals(snapshot.getClustersCount(), 2);
        assertEquals(snapshot.getClusterAt(0).getCluster(), "us-cent");
        assertEquals(snapshot.getClusterAt(0).getMessageId().getLedgerId(), 20);
        assertEquals(snapshot.getClusterAt(0).getMessageId().getEntryId(), 21);
        assertEquals(snapshot.getClusterAt(1).getCluster(), "us-east");
        assertEquals(snapshot.getClusterAt(1).getMessageId().getLedgerId(), 10);
        assertEquals(snapshot.getClusterAt(1).getMessageId().getEntryId(), 11);
    }

    @Test
    public void testUpdate() {
        Map<String, MarkersMessageIdData> clusters = new TreeMap<>();
        clusters.put("us-east", new MarkersMessageIdData().setLedgerId(10).setEntryId(11));
        clusters.put("us-cent", new MarkersMessageIdData().setLedgerId(20).setEntryId(21));

        ByteBuf buf = Markers.newReplicatedSubscriptionsUpdate("sub-1", clusters);

        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);
        assertEquals(msgMetadata.getReplicateTosCount(), 0);

        ReplicatedSubscriptionsUpdate snapshot = Markers.parseReplicatedSubscriptionsUpdate(buf);

        assertEquals(snapshot.getSubscriptionName(), "sub-1");

        assertEquals(snapshot.getClustersCount(), 2);
        assertEquals(snapshot.getClusterAt(0).getCluster(), "us-cent");
        assertEquals(snapshot.getClusterAt(0).getMessageId().getLedgerId(), 20);
        assertEquals(snapshot.getClusterAt(0).getMessageId().getEntryId(), 21);
        assertEquals(snapshot.getClusterAt(1).getCluster(), "us-east");
        assertEquals(snapshot.getClusterAt(1).getMessageId().getLedgerId(), 10);
        assertEquals(snapshot.getClusterAt(1).getMessageId().getEntryId(), 11);
    }

    @Test
    public void testTxnCommitMarker() throws IOException {
        long sequenceId = 1L;
        long mostBits = 1234L;
        long leastBits = 2345L;

        ByteBuf buf = Markers.newTxnCommitMarker(sequenceId, mostBits, leastBits);
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);

        assertEquals(msgMetadata.getMarkerType(), MarkerType.TXN_COMMIT_VALUE);
        assertEquals(msgMetadata.getSequenceId(), sequenceId);
        assertEquals(msgMetadata.getTxnidMostBits(), mostBits);
        assertEquals(msgMetadata.getTxnidLeastBits(), leastBits);
    }

    @Test
    public void testTxnAbortMarker() throws IOException {
        long sequenceId = 1L;
        long mostBits = 1234L;
        long leastBits = 2345L;

        ByteBuf buf = Markers.newTxnAbortMarker(sequenceId, mostBits, leastBits);

        MessageMetadata msgMetadata = Commands.parseMessageMetadata(buf);

        assertEquals(msgMetadata.getMarkerType(), MarkerType.TXN_ABORT_VALUE);
        assertEquals(msgMetadata.getSequenceId(), sequenceId);
        assertEquals(msgMetadata.getTxnidMostBits(), mostBits);
        assertEquals(msgMetadata.getTxnidLeastBits(), leastBits);
    }

}
