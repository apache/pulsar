package org.apache.pulsar.client.impl.transaction;

import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.client.api.transaction.TransactionResult;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * This class represents the result of transaction end on subscription request.
 */
@Data
@Builder
public class TransactionEndOnSubResult implements TransactionResult {

    private TxnID txnID;

}
