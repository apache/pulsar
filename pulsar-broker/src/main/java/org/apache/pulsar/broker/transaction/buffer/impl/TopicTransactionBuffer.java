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
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Markers;

/**
 * Transaction buffer based on normal persistent topic.
 */
@Slf4j
public class TopicTransactionBuffer implements TransactionBuffer {

    private final PersistentTopic topic;

    public TopicTransactionBuffer(PersistentTopic topic) {
        this.topic = topic;
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        topic.publishMessage(buffer, (e, ledgerId, entryId) -> {
            if (e != null) {
                log.error("Failed to append buffer to txn {}", txnId, e);
                completableFuture.completeExceptionally(e);
                return;
            }
            completableFuture.complete(PositionImpl.get(ledgerId, entryId));
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return null;
    }
    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, List<MessageIdData> sendMessageIdList) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} commit on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ByteBuf commitMarker = Markers.newTxnCommitMarker(-1L, txnID.getMostSigBits(),
                txnID.getLeastSigBits(), getMessageIdDataList(sendMessageIdList));
        topic.publishMessage(commitMarker, (e, ledgerId, entryId) -> {
            if (e != null) {
                log.error("Failed to commit for txn {}", txnID, e);
                completableFuture.completeExceptionally(e);
                return;
            }
            completableFuture.complete(null);
        });
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, List<MessageIdData> sendMessageIdList) {
        if (log.isDebugEnabled()) {
            log.debug("Transaction {} abort on topic {}.", txnID.toString(), topic.getName());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ByteBuf abortMarker = Markers.newTxnAbortMarker(
                -1L, txnID.getMostSigBits(), txnID.getLeastSigBits(), getMessageIdDataList(sendMessageIdList));
        topic.publishMessage(abortMarker, (e, ledgerId, entryId) -> {
            if (e != null) {
                log.error("Failed to abort for txn {}", txnID, e);
                completableFuture.completeExceptionally(e);
                return;
            }
            completableFuture.complete(null);
        });
        return completableFuture;
    }

    private List<PulsarMarkers.MessageIdData> getMessageIdDataList(List<MessageIdData> sendMessageIdList) {
        List<PulsarMarkers.MessageIdData> messageIdDataList = new ArrayList<>(sendMessageIdList.size());
        for (MessageIdData msgIdData : sendMessageIdList) {
            messageIdDataList.add(
                    PulsarMarkers.MessageIdData.newBuilder()
                            .setLedgerId(msgIdData.getLedgerId())
                            .setEntryId(msgIdData.getEntryId()).build());
        }
        return messageIdDataList;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }
}
