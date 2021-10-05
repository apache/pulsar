package org.apache.pulsar.broker.service;


import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.OffloadFilter;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;

public class OffloadFilterImp implements OffloadFilter {
    PersistentTopic persistentTopic;
    public OffloadFilterImp(PersistentTopic persistentTopic) {
        this.persistentTopic = persistentTopic;
    }

    @Override
    public boolean CheckIfNeedOffload(LedgerEntry ledgerEntry) {
        MessageMetadata messageMetadata = Commands.parseMessageMetadata(ledgerEntry.getEntryBuffer());

        if (messageMetadata.hasTxnidLeastBits() && messageMetadata.hasTxnidMostBits()){
            if (persistentTopic.isTxnAborted(new TxnID(messageMetadata.getTxnidMostBits(),
                    messageMetadata.getTxnidLeastBits()))){
                return false;
            }
            if (Markers.isTxnMarker(messageMetadata)){
                return false;
            }
        }
        return true;
    }

    @Override
    public PositionImpl getMaxReadPosition() {
        return persistentTopic.getMaxReadPosition();
    }

    @Override
    public boolean isTransactionBufferReady() {
        return persistentTopic.getTransactionBufferStats().state.equals("Ready");
    }
}
