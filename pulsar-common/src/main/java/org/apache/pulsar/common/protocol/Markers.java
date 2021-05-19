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
import io.netty.util.concurrent.FastThreadLocal;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotRequest;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshotResponse;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsUpdate;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;

@UtilityClass
@SuppressWarnings("checkstyle:JavadocType")
public class Markers {

    private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA = //
            new FastThreadLocal<MessageMetadata>() {
                @Override
                protected MessageMetadata initialValue() throws Exception {
                    return new MessageMetadata();
                }
            };

    private static final FastThreadLocal<ReplicatedSubscriptionsSnapshotRequest> LOCAL_SNAPSHOT_REQUEST = //
            new FastThreadLocal<ReplicatedSubscriptionsSnapshotRequest>() {
                @Override
                protected ReplicatedSubscriptionsSnapshotRequest initialValue() throws Exception {
                    return new ReplicatedSubscriptionsSnapshotRequest();
                }
            };

    private static final FastThreadLocal<ReplicatedSubscriptionsSnapshotResponse> LOCAL_SNAPSHOT_RESPONSE = //
            new FastThreadLocal<ReplicatedSubscriptionsSnapshotResponse>() {
                @Override
                protected ReplicatedSubscriptionsSnapshotResponse initialValue() throws Exception {
                    return new ReplicatedSubscriptionsSnapshotResponse();
                }
            };

    private static final FastThreadLocal<ReplicatedSubscriptionsSnapshot> LOCAL_SNAPSHOT = //
            new FastThreadLocal<ReplicatedSubscriptionsSnapshot>() {
                @Override
                protected ReplicatedSubscriptionsSnapshot initialValue() throws Exception {
                    return new ReplicatedSubscriptionsSnapshot();
                }
            };

    private static final FastThreadLocal<ReplicatedSubscriptionsUpdate> LOCAL_SUBSCRIPTION_UPDATE = //
            new FastThreadLocal<ReplicatedSubscriptionsUpdate>() {
                @Override
                protected ReplicatedSubscriptionsUpdate initialValue() throws Exception {
                    return new ReplicatedSubscriptionsUpdate();
                }
            };

    private static ByteBuf newMessage(MarkerType markerType, Optional<String> restrictToCluster, ByteBuf payload) {
        MessageMetadata msgMetadata = LOCAL_MESSAGE_METADATA.get()
                .clear()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("pulsar.marker")
                .setSequenceId(0)
                .setMarkerType(markerType.getValue());

        restrictToCluster.ifPresent(msgMetadata::addReplicateTo);

        return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, payload);
    }

    public static boolean isServerOnlyMarker(MessageMetadata msgMetadata) {
        // In future, if we add more marker types that can be also sent to clients
        // we'll have to do finer check here.
        return msgMetadata.hasMarkerType();
    }

    public static boolean isReplicatedSubscriptionSnapshotMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
                && msgMetadata.hasMarkerType()
                && msgMetadata.getMarkerType() == MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT.getValue();
    }

    public static ByteBuf newReplicatedSubscriptionsSnapshotRequest(String snapshotId, String sourceCluster) {
        ReplicatedSubscriptionsSnapshotRequest req = LOCAL_SNAPSHOT_REQUEST.get()
                .clear()
                .setSnapshotId(snapshotId)
                .setSourceCluster(sourceCluster);
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(req.getSerializedSize());

        try {
            req.writeTo(payload);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST, Optional.empty(), payload);
        } finally {
            payload.release();
        }
    }

    public static ReplicatedSubscriptionsSnapshotRequest parseReplicatedSubscriptionsSnapshotRequest(ByteBuf payload)
            throws IOException {
        ReplicatedSubscriptionsSnapshotRequest req = LOCAL_SNAPSHOT_REQUEST.get();
        req.parseFrom(payload, payload.readableBytes());
        return req;
    }

    public static ByteBuf newReplicatedSubscriptionsSnapshotResponse(String snapshotId, String replyToCluster,
            String cluster, long ledgerId, long entryId) {
        ReplicatedSubscriptionsSnapshotResponse response = LOCAL_SNAPSHOT_RESPONSE.get()
                .clear()
                .setSnapshotId(snapshotId);
        response
                .setCluster()
                .setCluster(cluster)
                .setMessageId()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(response.getSerializedSize());
        try {
            response.writeTo(payload);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_RESPONSE, Optional.of(replyToCluster),
                    payload);
        } finally {
            payload.release();
        }
    }

    public static ReplicatedSubscriptionsSnapshotResponse parseReplicatedSubscriptionsSnapshotResponse(ByteBuf payload)
            throws IOException {
        ReplicatedSubscriptionsSnapshotResponse response = LOCAL_SNAPSHOT_RESPONSE.get();
        response.parseFrom(payload, payload.readableBytes());
        return response;
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsSnapshot(String snapshotId, String sourceCluster, long ledgerId,
            long entryId, Map<String, MarkersMessageIdData> clusterIds) {
        ReplicatedSubscriptionsSnapshot snapshot = LOCAL_SNAPSHOT.get()
                .clear()
                .setSnapshotId(snapshotId);
        snapshot.setLocalMessageId()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        clusterIds.forEach((cluster, msgId) -> {
            snapshot.addCluster()
                    .setCluster(cluster)
                    .setMessageId().copyFrom(msgId);
        });

        int size = snapshot.getSerializedSize();
        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(size);
        try {
            snapshot.writeTo(payload);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT, Optional.of(sourceCluster), payload);
        } finally {
            payload.release();
        }
    }

    public static ReplicatedSubscriptionsSnapshot parseReplicatedSubscriptionsSnapshot(ByteBuf payload)
            throws IOException {
        ReplicatedSubscriptionsSnapshot snapshot = LOCAL_SNAPSHOT.get();
        snapshot.parseFrom(payload, payload.readableBytes());
        return snapshot;
    }

    @SneakyThrows
    public static ByteBuf newReplicatedSubscriptionsUpdate(String subscriptionName,
        Map<String, MarkersMessageIdData> clusterIds) {
        ReplicatedSubscriptionsUpdate update = LOCAL_SUBSCRIPTION_UPDATE.get()
                .clear()
                .setSubscriptionName(subscriptionName);

        clusterIds.forEach((cluster, msgId) -> {
            update.addCluster()
                    .setCluster(cluster)
                    .setMessageId().copyFrom(msgId);
        });

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(update.getSerializedSize());

        try {
            update.writeTo(payload);
            return newMessage(MarkerType.REPLICATED_SUBSCRIPTION_UPDATE, Optional.empty(), payload);
        } finally {
            payload.release();
        }
    }

    public static ReplicatedSubscriptionsUpdate parseReplicatedSubscriptionsUpdate(ByteBuf payload) {
        ReplicatedSubscriptionsUpdate update = LOCAL_SUBSCRIPTION_UPDATE.get();
        update.parseFrom(payload, payload.readableBytes());
        return update;
    }

    public static boolean isTxnCommitMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
               && msgMetadata.hasMarkerType()
               && msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT.getValue();
    }

    public static boolean isTxnMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
                && msgMetadata.hasMarkerType()
                && (msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT.getValue()
                || msgMetadata.getMarkerType() == MarkerType.TXN_ABORT.getValue());
    }

    public static ByteBuf newTxnCommitMarker(long sequenceId, long txnMostBits,
                                             long txnLeastBits) {
        return newTxnMarker(MarkerType.TXN_COMMIT, sequenceId, txnMostBits, txnLeastBits);
    }

    public static boolean isTxnAbortMarker(MessageMetadata msgMetadata) {
        return msgMetadata != null
               && msgMetadata.hasMarkerType()
               && msgMetadata.getMarkerType() == MarkerType.TXN_ABORT.getValue();
    }

    public static ByteBuf newTxnAbortMarker(long sequenceId, long txnMostBits,
                                            long txnLeastBits) {
        return newTxnMarker(
                MarkerType.TXN_ABORT, sequenceId, txnMostBits, txnLeastBits);
    }

    private static ByteBuf newTxnMarker(MarkerType markerType, long sequenceId, long txnMostBits,
                                        long txnLeastBits) {
        MessageMetadata msgMetadata = LOCAL_MESSAGE_METADATA.get()
                .clear()
                .setPublishTime(System.currentTimeMillis())
                .setProducerName("pulsar.txn.marker")
                .setSequenceId(sequenceId)
                .setMarkerType(markerType.getValue())
                .setTxnidMostBits(txnMostBits)
                .setTxnidLeastBits(txnLeastBits);

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.buffer(0);

        try {
            return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c,
                    msgMetadata, payload);
        } finally {
            payload.release();
        }
    }
}
