package org.apache.pulsar.client.impl.transaction;

import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.client.api.transaction.TransactionResult;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * This class represents the result of transaction end on topic request.
 */
@Data
@Builder
public class TransactionEndOnTopicResult implements TransactionResult {

    private TxnID txnID;

    private long committedLedgerId;

    private long committedEntryId;

    private int partition;

}
