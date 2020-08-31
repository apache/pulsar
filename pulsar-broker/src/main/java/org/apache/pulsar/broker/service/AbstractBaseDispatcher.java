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

package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.TransactionReader;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionEntryImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;


@Slf4j
public abstract class AbstractBaseDispatcher implements Dispatcher {

    protected final Subscription subscription;

    protected AbstractBaseDispatcher(Subscription subscription) {
        this.subscription = subscription;
    }

    /**
     * Filter messages that are being sent to a consumers.
     * <p>
     * Messages can be filtered out for multiple reasons:
     * <ul>
     * <li>Checksum or metadata corrupted
     * <li>Message is an internal marker
     * <li>Message is not meant to be delivered immediately
     * </ul>
     *
     * @param entries
     *            a list of entries as read from storage
     *
     * @param batchSizes
     *            an array where the batch size for each entry (the number of messages within an entry) is stored. This
     *            array needs to be of at least the same size as the entries list
     *
     * @param sendMessageInfo
     *            an object where the total size in messages and bytes will be returned back to the caller
     */
    public void filterEntriesForConsumer(List<Entry> entries, EntryBatchSizes batchSizes,
                                         SendMessageInfo sendMessageInfo, EntryBatchIndexesAcks indexesAcks,
                                         ManagedCursor cursor, TransactionReader transactionReader) {
        int totalMessages = 0;
        long totalBytes = 0;
        int totalChunkedMessages = 0;

        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            Entry entry = entries.get(i);
            if (entry == null) {
                continue;
            }

            ByteBuf metadataAndPayload = entry.getDataBuffer();

            MessageMetadata msgMetadata = Commands.peekMessageMetadata(metadataAndPayload, subscription.toString(), -1);

            try {
                if (Markers.isTxnCommitMarker(msgMetadata)) {
                    entries.set(i, null);
                    transactionReader.addPendingTxn(msgMetadata.getTxnidMostBits(), msgMetadata.getTxnidLeastBits());
                    continue;
                } else if (msgMetadata == null || Markers.isServerOnlyMarker(msgMetadata)) {
                    PositionImpl pos = (PositionImpl) entry.getPosition();
                    // Message metadata was corrupted or the messages was a server-only marker

                    if (Markers.isReplicatedSubscriptionSnapshotMarker(msgMetadata)) {
                        processReplicatedSubscriptionSnapshot(pos, metadataAndPayload);
                    }

                    entries.set(i, null);
                    entry.release();
                    subscription.acknowledgeMessage(Collections.singletonList(pos), AckType.Individual,
                            Collections.emptyMap());
                    continue;
                } else if (msgMetadata.hasDeliverAtTime()
                        && trackDelayedDelivery(entry.getLedgerId(), entry.getEntryId(), msgMetadata)) {
                    // The message is marked for delayed delivery. Ignore for now.
                    entries.set(i, null);
                    entry.release();
                    continue;
                }

                if (entry instanceof TransactionEntryImpl) {
                    ((TransactionEntryImpl) entry).setStartBatchIndex(
                            transactionReader.calculateStartBatchIndex(msgMetadata.getNumMessagesInBatch()));
                }

                int batchSize = msgMetadata.getNumMessagesInBatch();
                totalMessages += batchSize;
                totalBytes += metadataAndPayload.readableBytes();
                totalChunkedMessages += msgMetadata.hasChunkId() ? 1: 0;
                batchSizes.setBatchSize(i, batchSize);
                if (indexesAcks != null && cursor != null) {
                    long[] ackSet = cursor.getDeletedBatchIndexesAsLongArray(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()));
                    if (ackSet != null) {
                        indexesAcks.setIndexesAcks(i, Pair.of(batchSize, ackSet));
                    } else {
                        indexesAcks.setIndexesAcks(i,null);
                    }
                }
            } finally {
                msgMetadata.recycle();
            }
        }

        sendMessageInfo.setTotalMessages(totalMessages);
        sendMessageInfo.setTotalBytes(totalBytes);
        sendMessageInfo.setTotalChunkedMessages(totalChunkedMessages);
    }

    private void processReplicatedSubscriptionSnapshot(PositionImpl pos, ByteBuf headersAndPayload) {
        // Remove the protobuf headers
        Commands.skipMessageMetadata(headersAndPayload);

        try {
            ReplicatedSubscriptionsSnapshot snapshot = Markers.parseReplicatedSubscriptionsSnapshot(headersAndPayload);
            subscription.processReplicatedSubscriptionSnapshot(snapshot);
        } catch (Throwable t) {
            log.warn("Failed to process replicated subscription snapshot at {} -- {}", pos, t.getMessage(), t);
            return;
        }
    }

    public void resetCloseFuture() {
        // noop
    }

    public static final String NONE_KEY = "NONE_KEY";

    protected byte[] peekStickyKey(ByteBuf metadataAndPayload) {
        metadataAndPayload.markReaderIndex();
        PulsarApi.MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);
        metadataAndPayload.resetReaderIndex();
        byte[] key = NONE_KEY.getBytes();
        if (metadata.hasOrderingKey()) {
            return metadata.getOrderingKey().toByteArray();
        } else if (metadata.hasPartitionKey()) {
            return metadata.getPartitionKey().getBytes();
        }
        metadata.recycle();
        return key;
    }

}
