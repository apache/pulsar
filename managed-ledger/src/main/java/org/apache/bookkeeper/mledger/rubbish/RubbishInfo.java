package org.apache.bookkeeper.mledger.rubbish;


import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RubbishInfo {
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
     * When consumer received rubbish info, maybe the ledger still in use, we need check the ledger is in use.
     * In some cases, we needn't check the ledger still in use.
     */
    private boolean checkLedgerStillInUse;

    /**
     * Extent properties.
     */
    private Map<String, String> properties;

}
