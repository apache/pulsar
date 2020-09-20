/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

import java.util.List;
import java.util.Map;

/**
 * Persistent topic internal statistics.
 */
public class PersistentTopicInternalStats {

    public long entriesAddedCounter;
    public long numberOfEntries;
    public long totalSize;

    public long currentLedgerEntries;
    public long currentLedgerSize;
    public String lastLedgerCreatedTimestamp;
    public String lastLedgerCreationFailureTimestamp;

    public int waitingCursorsCount;
    public int pendingAddEntriesCount;

    public String lastConfirmedEntry;
    public String state;

    public List<LedgerInfo> ledgers;
    public Map<String, CursorStats> cursors;

    // LedgerInfo for compacted topic if exist.
    public LedgerInfo compactedLedger;

    /**
     * Ledger information.
     */
    public static class LedgerInfo {
        public long ledgerId;
        public long entries;
        public long size;
        public boolean offloaded;
    }

    /**
     * Pulsar cursor statistics.
     */
    public static class CursorStats {
        public String markDeletePosition;
        public String readPosition;
        public boolean waitingReadOp;
        public int pendingReadOps;

        public long messagesConsumedCounter;
        public long cursorLedger;
        public long cursorLedgerLastEntry;
        public String individuallyDeletedMessages;
        public String lastLedgerSwitchTimestamp;
        public String state;
        public long numberOfEntriesSinceFirstNotAckedMessage;
        public int totalNonContiguousDeletedMessagesRange;

        public Map<String, Long> properties;
    }
}
