package org.apache.pulsar.broker.transaction.buffer.metadata.v2;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionBufferSnapshot {
    private String topicName;
    private long sequenceId;
    private long maxReadPositionLedgerId;
    private long maxReadPositionEntryId;
    private List<TxnIDData> aborts;
}
