package org.apache.pulsar.broker.transaction.buffer.metadata.v2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionBufferSnapshotIndex {
    public long sequenceID;
    public long maxReadPositionLedgerID;
    public long maxReadPositionEntryID;
    public long persistentPositionLedgerID;
    public long persistentPositionEntryID;
}
