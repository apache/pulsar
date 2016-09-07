package org.apache.bookkeeper.mledger.proto;

/**
 */
public class PendingBookieOpsStats {
    public long dataLedgerOpenOp;
    public long dataLedgerCloseOp;
    public long dataLedgerCreateOp;
    public long dataLedgerDeleteOp;
    public long cursorLedgerOpenOp;
    public long cursorLedgerCloseOp;
    public long cursorLedgerCreateOp;
    public long cursorLedgerDeleteOp;
}
