package org.apache.bookkeeper.mledger.rubbish;


import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;


@Data
@Builder
public class RubbishLedger {
    /**
     * Partitioned topic name without domain. Likes public/default/test-topic-partition-1 or
     * public/default/test-topic
     */
    private String topicName;

    /**
     * The rubbish source. managed-ledger, managed-cursor and schema-storage.
     */
    private RubbishSource rubbishSource;

    /**
     * The rubbish type. ledger or offload-ledger.
     */
    private RubbishType rubbishType;

    /**
     * ledgerInfo. If ledger, just holds ledgerId. If offload-ledger, holds ledgerId and offload context uuid.
     */
    private ManagedLedgerInfo.LedgerInfo ledgerInfo;

    /**
     * When consumer received rubbish ledger, maybe the ledger still in use, we need check the ledger is in use.
     * In some cases, we needn't check the ledger still in use.
     */
    private boolean checkLedgerStillInUse;

    /**
     * Extent properties.
     */
    private Map<String, String> properties = new HashMap<>();

    public RubbishLedger() {
    }

    public RubbishLedger(String topicName, RubbishSource rubbishSource, RubbishType rubbishType,
                         ManagedLedgerInfo.LedgerInfo ledgerInfo, boolean checkLedgerStillInUse) {
        this.topicName = topicName;
        this.rubbishSource = rubbishSource;
        this.rubbishType = rubbishType;
        this.ledgerInfo = ledgerInfo;
        this.checkLedgerStillInUse = checkLedgerStillInUse;
    }
}
