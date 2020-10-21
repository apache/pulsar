package org.apache.pulsar.broker.service.persistent;

import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class TransactionMessageReader {

    private ManagedLedgerImpl managedLedger;

    private final ConcurrentLinkedQueue<ByteBuf> pendingTxnQueue;

    private boolean isTxnRead = false;

    public TransactionMessageReader(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
        this.pendingTxnQueue = Queues.newConcurrentLinkedQueue();
    }

    public void addPendingTxn(ByteBuf byteBuf) {
        pendingTxnQueue.add(byteBuf);
    }

    public boolean havePendingTxnToRead() {
        return pendingTxnQueue.size() > 0;
    }

    /**
     * Read transaction messages.
     *
     * @param readMessageNum messages num to read
     * @param ctx context object
     * @param readEntriesCallback ReadEntriesCallback
     */
    public void read(int readMessageNum, Object ctx, AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        ByteBuf byteBuf = pendingTxnQueue.poll();
        Commands.parseMessageMetadata(byteBuf);
        PulsarMarkers.TxnCommitMarker txnCommitMarker = null;
        try {
            txnCommitMarker = Markers.parseCommitMarker(byteBuf);
        } catch (IOException e) {
            log.error("Failed to parse commit marker.", e);
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(
                            new Exception("Failed to parse commit marker.")), ctx);
            return;
        }

        final List<Entry> entryList = new ArrayList<>();
        List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
        for (PulsarMarkers.MessageIdData messageIdData : txnCommitMarker.getMessageIdListList()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            completableFutureList.add(future);
            managedLedger.asyncReadEntry(PositionImpl.get(messageIdData.getLedgerId(), messageIdData.getEntryId()),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            entryList.add(entry);
                            future.complete(null);
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            future.completeExceptionally(exception);
                        }
                    }, null);
        }
        FutureUtil.waitForAll(completableFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to read transaction messages.", throwable);
                readEntriesCallback.readEntriesFailed(
                        ManagedLedgerException.getManagedLedgerException(
                                new Exception("Failed to read transaction messages.")), ctx);
                return;
            }
            isTxnRead = true;
            readEntriesCallback.readEntriesComplete(entryList, ctx);
        });
    }

    public boolean isTxnRead() {
        return isTxnRead;
    }

    public void finishTxnRead() {
        isTxnRead = false;
    }

}
