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
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarMarkers.ReplicatedSubscriptionsSnapshot;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

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
            SendMessageInfo sendMessageInfo) {
        int totalMessages = 0;
        long totalBytes = 0;

        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            Entry entry = entries.get(i);
            ByteBuf metadataAndPayload = entry.getDataBuffer();

            MessageMetadata msgMetadata = Commands.peekMessageMetadata(metadataAndPayload, subscription.toString(), -1);

            try {
                if (msgMetadata == null || Markers.isServerOnlyMarker(msgMetadata)) {
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

                int batchSize = msgMetadata.getNumMessagesInBatch();
                totalMessages += batchSize;
                totalBytes += metadataAndPayload.readableBytes();
                batchSizes.setBatchSize(i, batchSize);
            } finally {
                msgMetadata.recycle();
            }
        }

        sendMessageInfo.setTotalMessages(totalMessages);
        sendMessageInfo.setTotalBytes(totalBytes);
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
        String key = metadata.getPartitionKey();
        if (log.isDebugEnabled()) {
            log.debug("Parse message metadata, partition key is {}, ordering key is {}", key, metadata.getOrderingKey());
        }
        if (StringUtils.isNotBlank(key) || metadata.hasOrderingKey()) {
            return metadata.hasOrderingKey() ? metadata.getOrderingKey().toByteArray() : key.getBytes();
        }
        metadata.recycle();
        return NONE_KEY.getBytes();
    }
}
