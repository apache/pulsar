package org.apache.pulsar.transaction.buffer;

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;

public interface TransactionCursor {

    /**
     * Get the specified transaction meta.
     *
     * @param txnID
     * @return
     */
    TransactionMeta getTxnMeta(TxnID txnID) throws TransactionNotFoundException;

    /**
     * Get the specified transaction meta, if the transaction meta is not exist, create a new transaction meta and
     * return.
     *
     * @param txnID
     * @return
     */
    TransactionMeta getOrCreateTxnMeta(TxnID txnID);

    /**
     * Add the transaction id to the committed transaction index.
     *
     * @param ledgerId
     * @param txnID
     * @param position
     */
    void addTxnToCommittedIndex(long ledgerId, TxnID txnID, Position position);

}
