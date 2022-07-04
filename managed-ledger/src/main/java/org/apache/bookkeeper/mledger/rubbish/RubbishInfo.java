package org.apache.bookkeeper.mledger.rubbish;


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

    private String topicName;
    
    private RubbishSource rubbishSource;
    
    private RubbishType rubbishType;

    private ManagedLedgerInfo.LedgerInfo ledgerInfo;

    private boolean checkLedgerStillInUse;

}
