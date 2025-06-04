/*
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
 * ManagedLedger internal statistics.
 */
public class ManagedLedgerInternalStats {

    /** Messages published since this broker loaded this managedLedger. */
    public long entriesAddedCounter;

    /** The total number of entries being tracked. */
    public long numberOfEntries;

    /** The total storage size of all messages (in bytes). */
    public long totalSize;

    /** The count of messages written to the ledger that is currently open for writing. */
    public long currentLedgerEntries;

    /** The size of messages written to the ledger that is currently open for writing (in bytes). */
    public long currentLedgerSize;

    /** The time when the last ledger is created. */
    public String lastLedgerCreatedTimestamp;

    /** The time when the last ledger failed. */
    public String lastLedgerCreationFailureTimestamp;

    /** The number of cursors that are "caught up" and waiting for a new message to be published. */
    public int waitingCursorsCount;

    /** The number of messages that complete (asynchronous) write requests. */
    public int pendingAddEntriesCount;

    /** The ledgerid: entryid of the last message that is written successfully.
     * If the entryid is -1, then the ledger is open, yet no entries are written. */
    public String lastConfirmedEntry;

    /** The state of this ledger for writing.
     * The state LedgerOpened means that a ledger is open for saving published messages. */
    public String state;

    /** The ordered list of all ledgers for this topic holding messages. */
    public List<LedgerInfo> ledgers;

    /** The list of all cursors on this topic. Each subscription in the topic stats has a cursor. */
    public Map<String, CursorStats> cursors;

    /**
     * Ledger information.
     */
    public static class LedgerInfo {
        public long ledgerId;
        public long entries;
        public long size;
        public boolean offloaded;
        public String metadata;
        public boolean underReplicated;
        public Map<String, String> properties;
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
        public boolean active;
        public long numberOfEntriesSinceFirstNotAckedMessage;
        public int totalNonContiguousDeletedMessagesRange;
        public boolean subscriptionHavePendingRead;
        public boolean subscriptionHavePendingReplayRead;

        public Map<String, Long> properties;
    }
}
