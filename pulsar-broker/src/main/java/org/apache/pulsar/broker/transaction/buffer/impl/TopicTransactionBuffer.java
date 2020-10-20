package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class TopicTransactionBuffer implements TransactionBuffer {

    private PersistentTopic topic;

    public TopicTransactionBuffer(PersistentTopic topic) {
        this.topic = topic;
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, long batchSize, ByteBuf buffer) {
        CompletableFuture<Position> completableFuture = new CompletableFuture<>();
        topic.publishMessage(buffer, (e, ledgerId, entryId) -> {
            if (e != null) {
                log.error("Failed to appendBufferToTxn for txn {}", txnId, e);
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
            log.debug("Transaction {} create a commit marker", txnID.toString());
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        List<PulsarMarkers.MessageIdData> messageIdDataList = new ArrayList<>();
        for (MessageIdData msgIdData : sendMessageIdList) {
            messageIdDataList.add(
                    PulsarMarkers.MessageIdData.newBuilder()
                            .setLedgerId(msgIdData.getLedgerId())
                            .setEntryId(msgIdData.getEntryId())
                            .setPartition(msgIdData.getPartition()).build());
        }
        ByteBuf commitMarker = Markers.newTxnCommitMarker(-1L, txnID.getMostSigBits(),
                txnID.getLeastSigBits(), null, messageIdDataList);
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
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        topic.getSubscriptions().values();
        return null;
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
