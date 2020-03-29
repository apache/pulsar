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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ClusterMessageId;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MarkerType;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsUpdate;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;

@UtilityClass
@SuppressWarnings("checkstyle:JavadocType")
public class Markers {

    private static ByteBuf newMessage(MarkerType markerType, Optional<String> restrictToCluster, ByteBuf payload) {
        MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
        msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder.setProducerName("pulsar.marker");
        msgMetadataBuilder.setSequenceId(0);
        msgMetadataBuilder.setMarkerType(markerType.getNumber());

        restrictToCluster.ifPresent(msgMetadataBuilder::addReplicateTo);

        MessageMetadata msgMetadata = msgMetadataBuilder.build();
        try {
            return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, payload);
        } finally {
            msgMetadata.recycle();
            msgMetadataBuilder.recycle();
        }
    }

    public static boolean isServerOnlyMarker(MessageMetadata msgMetadata) {
        // In future, if we add more marker types that can be also sent to clients
        // we'll have to do finer check here.
        return msgMetadata.hasMarkerType();
    }

    public static boolean isReplicatedSubscriptionSnapshotMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
                && msgMetadata.hasMarkerType()
                && msgMetadata.getMarkerType() == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_VALUE;
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsSnapshotRequest(String snapshotId, String sourceCluster) {
        ReplicatedSubscriptionsSnapshotRequest.Builder builder = ReplicatedSubscriptionsSnapshotRequest.newBuilder();
        builder.setSnapshotId(snapshotId);
        builder.setSourceCluster(sourceCluster);

        ReplicatedSubscriptionsSnapshotRequest req = builder.build();

        int size = req.getSerializedSize();

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(size);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(payload);
        try {
            req.writeTo(outStream);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST, Optional.empty(), payload);
        } finally {
            payload.release();
            builder.recycle();
            req.recycle();
            outStream.recycle();
        }
    }

    public static ReplicatedSubscriptionsSnapshotRequest parseReplicatedSubscriptionsSnapshotRequest(ByteBuf payload)
            throws IOException {
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(payload);
        ReplicatedSubscriptionsSnapshotRequest.Builder builder = null;

        try {
            builder = ReplicatedSubscriptionsSnapshotRequest.newBuilder();
            return builder.mergeFrom(inStream, null).build();
        } finally {
            builder.recycle();
            inStream.recycle();
        }
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsSnapshotResponse(String snapshotId, String replyToCluster,
            String cluster, long ledgerId, long entryId) {
        ReplicatedSubscriptionsSnapshotResponse.Builder builder = ReplicatedSubscriptionsSnapshotResponse.newBuilder();
        builder.setSnapshotId(snapshotId);

        MessageIdData.Builder msgIdBuilder = MessageIdData.newBuilder();
        msgIdBuilder.setLedgerId(ledgerId);
        msgIdBuilder.setEntryId(entryId);

        ClusterMessageId.Builder clusterMessageIdBuilder = ClusterMessageId.newBuilder();
        clusterMessageIdBuilder.setCluster(cluster);
        clusterMessageIdBuilder.setMessageId(msgIdBuilder);

        builder.setCluster(clusterMessageIdBuilder);
        ReplicatedSubscriptionsSnapshotResponse response = builder.build();

        int size = response.getSerializedSize();

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(size);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(payload);
        try {
            response.writeTo(outStream);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE, Optional.of(replyToCluster),
                    payload);
        } finally {
            msgIdBuilder.recycle();
            clusterMessageIdBuilder.recycle();
            payload.release();
            builder.recycle();
            response.recycle();
            outStream.recycle();
        }
    }

    public static ReplicatedSubscriptionsSnapshotResponse parseReplicatedSubscriptionsSnapshotResponse(ByteBuf payload)
            throws IOException {
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(payload);
        ReplicatedSubscriptionsSnapshotResponse.Builder builder = null;

        try {
            builder = ReplicatedSubscriptionsSnapshotResponse.newBuilder();
            return builder.mergeFrom(inStream, null).build();
        } finally {
            builder.recycle();
            inStream.recycle();
        }
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsSnapshot(String snapshotId, String sourceCluster, long ledgerId,
            long entryId, Map<String, MessageIdData> clusterIds) {
        ReplicatedSubscriptionsSnapshot.Builder builder = ReplicatedSubscriptionsSnapshot.newBuilder();
        builder.setSnapshotId(snapshotId);

        MessageIdData.Builder msgIdBuilder = MessageIdData.newBuilder();
        msgIdBuilder.setLedgerId(ledgerId);
        msgIdBuilder.setEntryId(entryId);
        builder.setLocalMessageId(msgIdBuilder);

        clusterIds.forEach((cluster, msgId) -> {
            ClusterMessageId.Builder clusterMessageIdBuilder = ClusterMessageId.newBuilder()
                    .setCluster(cluster)
                    .setMessageId(msgId);
            builder.addClusters(clusterMessageIdBuilder);
            clusterMessageIdBuilder.recycle();
        });

        ReplicatedSubscriptionsSnapshot snapshot = builder.build();

        int size = snapshot.getSerializedSize();

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(size);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(payload);
        try {
            snapshot.writeTo(outStream);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT, Optional.of(sourceCluster), payload);
        } finally {
            payload.release();
            builder.recycle();
            snapshot.recycle();
            outStream.recycle();
        }
    }

    public static ReplicatedSubscriptionsSnapshot parseReplicatedSubscriptionsSnapshot(ByteBuf payload)
            throws IOException {
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(payload);
        ReplicatedSubscriptionsSnapshot.Builder builder = null;

        try {
            builder = ReplicatedSubscriptionsSnapshot.newBuilder();
            return builder.mergeFrom(inStream, null).build();
        } finally {
            builder.recycle();
            inStream.recycle();
        }
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsUpdate(String subscriptionName,
        Map<String, MessageIdData> clusterIds) {
        ReplicatedSubscriptionsUpdate.Builder builder = ReplicatedSubscriptionsUpdate.newBuilder();
        builder.setSubscriptionName(subscriptionName);

        clusterIds.forEach((cluster, msgId) -> {
            ClusterMessageId.Builder clusterMessageIdBuilder = ClusterMessageId.newBuilder()
                    .setCluster(cluster)
                    .setMessageId(msgId);
            builder.addClusters(clusterMessageIdBuilder);
            clusterMessageIdBuilder.recycle();
        });

        ReplicatedSubscriptionsUpdate update = builder.build();

        int size = update.getSerializedSize();

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(size);
        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(payload);
        try {
            update.writeTo(outStream);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_UPDATE, Optional.empty(), payload);
        } finally {
            payload.release();
            builder.recycle();
            update.recycle();
            outStream.recycle();
        }
    }

    public static ReplicatedSubscriptionsUpdate parseReplicatedSubscriptionsUpdate(ByteBuf payload)
            throws IOException {
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(payload);
        ReplicatedSubscriptionsUpdate.Builder builder = null;

        try {
            builder = ReplicatedSubscriptionsUpdate.newBuilder();
            return builder.mergeFrom(inStream, null).build();
        } finally {
            builder.recycle();
            inStream.recycle();
        }
    }

    public static boolean isTxnCommitMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
               && msgMetadata.hasMarkerType()
               && msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE;
    }

    public static ByteBuf newTxnCommitMarker(long sequenceId, long txnMostBits,
                                             long txnLeastBits, MessageIdData messageIdData) {
        return newTxnMarker(MarkerType.TXN_COMMIT, sequenceId, txnMostBits, txnLeastBits, Optional.of(messageIdData));
    }

    public static boolean isTxnAbortMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
               && msgMetadata.hasMarkerType()
               && msgMetadata.getMarkerType() == MarkerType.TXN_ABORT_VALUE;
    }

    public static ByteBuf newTxnAbortMarker(long sequenceId, long txnMostBits,
                                            long txnLeastBits) {
        return newTxnMarker(MarkerType.TXN_ABORT, sequenceId, txnMostBits, txnLeastBits, Optional.empty());
    }

    public static PulsarMarkers.TxnCommitMarker parseCommitMarker(ByteBuf payload) throws IOException {
        ByteBufCodedInputStream inStream = ByteBufCodedInputStream.get(payload);

        PulsarMarkers.TxnCommitMarker.Builder builder = null;

        try {
            builder = PulsarMarkers.TxnCommitMarker.newBuilder();
            return builder.mergeFrom(inStream, null).build();
        } finally {
            builder.recycle();
            inStream.recycle();
        }
    }

    @SneakyThrows
    private static ByteBuf newTxnMarker(MarkerType markerType, long sequenceId, long txnMostBits,
                                        long txnLeastBits, Optional<MessageIdData> messageIdData) {
        MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
        msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        msgMetadataBuilder.setProducerName("pulsar.txn.marker");
        msgMetadataBuilder.setSequenceId(sequenceId);
        msgMetadataBuilder.setMarkerType(markerType.getNumber());
        msgMetadataBuilder.setTxnidMostBits(txnMostBits);
        msgMetadataBuilder.setTxnidLeastBits(txnLeastBits);

        MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf payload;
        if (messageIdData.isPresent()) {
            PulsarMarkers.TxnCommitMarker commitMarker = PulsarMarkers.TxnCommitMarker.newBuilder()
                                                                                      .setMessageId(messageIdData.get())
                                                                                      .build();
            int size = commitMarker.getSerializedSize();
            payload = PooledByteBufAllocator.DEFAULT.buffer(size);
            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(payload);
            commitMarker.writeTo(outStream);
        } else {
            payload = PooledByteBufAllocator.DEFAULT.buffer();
        }

        try {
            return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, payload);
        } finally {
            payload.release();
            msgMetadata.recycle();
            msgMetadataBuilder.recycle();
        }
    }
}
