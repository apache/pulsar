package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.transaction.coordinator.impl.TopicBaseTransactionMetadataStore.TopicBaseTransactionWriter;

class TopicBaseTransactionWriterImpl implements TopicBaseTransactionWriter {

    private ManagedLedger managedLedger;

    public TopicBaseTransactionWriterImpl(String tcID, ManagedLedgerFactory factory) throws Exception {
        this.managedLedger = factory.open(tcID);

    }

    @Override
    public CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry) {
        int transactionMetadataEntrySize = transactionMetadataEntry.getSerializedSize();

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);

        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        try {
            transactionMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e){
            completableFuture.completeExceptionally(e);
            return completableFuture;
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }
}
