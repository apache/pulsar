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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.collections.ConcurrentLongPairSet;

/**
 * Transaction message reader.
 */
@Slf4j
public class TransactionMessageReader {

    private final Subscription subscription;
    private final Dispatcher dispatcher;
    private final Executor executor;

    private final ConcurrentLongPairSet pendingReadPosition;
    private final ArrayList<Entry> abortMarkerList;

    public TransactionMessageReader(Subscription subscription,
                                    Dispatcher dispatcher, Executor executor) {
        this.subscription = subscription;
        this.dispatcher = dispatcher;
        this.executor = executor;
        this.pendingReadPosition = new ConcurrentLongPairSet(256, 1);
        this.abortMarkerList = new ArrayList<>();
    }

    public boolean shouldSendToConsumer(PulsarApi.MessageMetadata msgMetadata, Entry entry,
                                            List<Entry> entries, int entryIndex) {
        if (pendingReadPosition.remove(entries.get(entryIndex).getLedgerId(), entries.get(entryIndex).getEntryId())) {
            return true;
        }
        if (Markers.isTxnCommitMarker(msgMetadata)) {
            getTransactionPositionList(entry);
        } else if (Markers.isTxnAbortMarker(msgMetadata)) {
            abortMarkerList.add(entry);
            handleAbort();
        }
        entries.set(entryIndex, null);
        return false;
    }

    private void getTransactionPositionList(Entry entry) {
        try {
            ByteBuf byteBuf = entry.getDataBuffer();
            Commands.skipMessageMetadata(byteBuf);
            PulsarMarkers.TxnCommitMarker commitMarker = Markers.parseCommitMarker(byteBuf);
            for (PulsarMarkers.MessageIdData messageIdData : commitMarker.getMessageIdList()) {
                pendingReadPosition.add(messageIdData.getLedgerId(), messageIdData.getEntryId());
                dispatcher.addMessageToRedelivery(messageIdData.getLedgerId(), messageIdData.getEntryId());
            }
        } catch (IOException e) {
            log.error("Failed to get transaction message id list.", e);
        }
    }

    private void handleAbort() {
        executor.execute(() -> {
            List<Position> positionList = new ArrayList<>();
            for (Entry abortEntry : abortMarkerList) {
                ByteBuf byteBuf = abortEntry.getDataBuffer();
                Commands.parseMessageMetadata(byteBuf);
                PulsarMarkers.TxnCommitMarker abortMarker = null;
                try {
                    abortMarker = Markers.parseCommitMarker(byteBuf);
                    for (PulsarMarkers.MessageIdData messageIdData : abortMarker.getMessageIdList()) {
                        positionList.add(PositionImpl.get(messageIdData.getLedgerId(), messageIdData.getEntryId()));
                    }
                } catch (IOException e) {
                    log.error("Failed to parse abort marker.", e);
                }
            }
            subscription.acknowledgeMessage(positionList, PulsarApi.CommandAck.AckType.Individual, Collections.emptyMap());
        });
    }

}
