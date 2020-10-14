package org.apache.pulsar.client.impl.transaction;

import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.client.api.transaction.TransactionResult;
import org.apache.pulsar.client.api.transaction.TxnID;

@Data
@Builder
public class TransactionEndOnSubResult implements TransactionResult {

    private TxnID txnID;

}
