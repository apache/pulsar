/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.rubbish;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;


@Getter
@Setter
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
