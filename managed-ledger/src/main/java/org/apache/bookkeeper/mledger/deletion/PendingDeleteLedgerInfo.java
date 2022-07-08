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
package org.apache.bookkeeper.mledger.deletion;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;


@Getter
@Setter
public class PendingDeleteLedgerInfo {
    /**
     * Partitioned topic name without domain. Likes public/default/test-topic-partition-1 or
     * public/default/test-topic
     */
    private String topicName;

    /**
     * The ledger component . managed-ledger, managed-cursor and schema-storage.
     */
    private LedgerComponent ledgerComponent;

    /**
     * The ledger type. ledger or offload-ledger.
     */
    private LedgerType ledgerType;

    /**
     * LedgerId.
     */
    private Long ledgerId;

    /**
     * Context, holds offload info. If bk ledger, the context is null.
     */
    private MLDataFormats.ManagedLedgerInfo.LedgerInfo context;

    /**
     * When consumer received pending delete ledger, maybe the ledger still in use, we need check the ledger is in use.
     * In some cases, we needn't check the ledger still in use.
     */
    private boolean checkLedgerStillInUse;

    /**
     * Extent properties.
     */
    private Map<String, String> properties = new HashMap<>();

    public PendingDeleteLedgerInfo() {
    }

    public PendingDeleteLedgerInfo(String topicName, LedgerComponent ledgerComponent, LedgerType ledgerType,
                                   Long ledgerId, MLDataFormats.ManagedLedgerInfo.LedgerInfo context,
                                   boolean checkLedgerStillInUse) {
        this.topicName = topicName;
        this.ledgerComponent = ledgerComponent;
        this.ledgerType = ledgerType;
        this.ledgerId = ledgerId;
        this.context = context;
        this.checkLedgerStillInUse = checkLedgerStillInUse;
    }
}
