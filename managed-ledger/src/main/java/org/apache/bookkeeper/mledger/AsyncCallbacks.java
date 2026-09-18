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
package org.apache.bookkeeper.mledger;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;

/**
 * Definition of all the callbacks used for the ManagedLedger asynchronous API.
 *
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
@SuppressWarnings("checkstyle:javadoctype")
public interface AsyncCallbacks {

    interface OpenLedgerCallback {
        void openLedgerComplete(ManagedLedger ledger, Object ctx);

        void openLedgerFailed(ManagedLedgerException exception, Object ctx);
    }

    interface OpenReadOnlyCursorCallback {
        void openReadOnlyCursorComplete(ReadOnlyCursor cursor, Object ctx);

        void openReadOnlyCursorFailed(ManagedLedgerException exception, Object ctx);
    }

    interface OpenReadOnlyManagedLedgerCallback {
        void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx);

        void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx);
    }

    interface DeleteLedgerCallback {
        void deleteLedgerComplete(Object ctx);

        void deleteLedgerFailed(ManagedLedgerException exception, Object ctx);
    }

    interface OpenCursorCallback {
        void openCursorComplete(ManagedCursor cursor, Object ctx);

        void openCursorFailed(ManagedLedgerException exception, Object ctx);
    }

    interface DeleteCursorCallback {
        void deleteCursorComplete(Object ctx);

        void deleteCursorFailed(ManagedLedgerException exception, Object ctx);
    }

    interface AddEntryCallback {
        void addComplete(Position position, ByteBuf entryData, Object ctx);

        void addFailed(ManagedLedgerException exception, Object ctx);
    }

    interface CloseCallback {
        void closeComplete(Object ctx);

        void closeFailed(ManagedLedgerException exception, Object ctx);
    }

    /**
     * Completion of an entry read. Ordinary multi-entry cursor reads use the completion policy selected when opening
     * the ledger in {@link ManagedLedgerConfig}; replay paths and failures can still invoke this interface inline.
     * Implementations must not block. A caller needing a different execution context can wrap its callback
     * to hand off processing to its own executor, and must release the returned entries if that handoff is rejected.
     * Future adapters in {@link ManagedLedgerUtils} do not introduce an executor handoff.
     */
    interface ReadEntriesCallback {
        /**
         * May be invoked inline when enabled in the ledger configuration, including on the calling thread for a cache
         * hit. The default ledger configuration restricts ordinary cursor read completions to the ledger executor,
         * with bounded inline completion when already on that executor.
         * The broker enables inline completion on other threads by default. At the nesting limit, enabled mode
         * may continue on a JVM common-pool worker without Netty or ledger-executor thread affinity; it falls back
         * to the ledger executor when common-pool parallelism is at most one.
         * The recipient owns the returned entries and must release each entry after processing or discarding it,
         * including when its own shutdown or cancellation makes the result unnecessary.
         * Callers chaining cursor reads must coordinate result processing as described in {@link ManagedCursor}.
         */
        void readEntriesComplete(List<Entry> entries, Object ctx);

        /**
         * May be invoked inline, including for validation failures before an asynchronous read is started.
         * A rejected completion handoff can fail after the cursor position advances. Recovery must
         * restore the required position before reading again.
         */
        void readEntriesFailed(ManagedLedgerException exception, Object ctx);
    }

    /**
     * Completion of a single-entry read. Like {@link ReadEntriesCallback}, callbacks may run inline
     * without fixed thread affinity, and the recipient must release the returned entry after use.
     */
    interface ReadEntryCallback {
        void readEntryComplete(Entry entry, Object ctx);

        void readEntryFailed(ManagedLedgerException exception, Object ctx);
    }

    interface MarkDeleteCallback {
        void markDeleteComplete(Object ctx);

        void markDeleteFailed(ManagedLedgerException exception, Object ctx);
    }

    interface ClearBacklogCallback {
        void clearBacklogComplete(Object ctx);

        void clearBacklogFailed(ManagedLedgerException exception, Object ctx);
    }

    interface SkipEntriesCallback {
        void skipEntriesComplete(Object ctx);

        void skipEntriesFailed(ManagedLedgerException exception, Object ctx);
    }

    interface DeleteCallback {
        void deleteComplete(Object ctx);

        void deleteFailed(ManagedLedgerException exception, Object ctx);
    }

    interface TerminateCallback {
        void terminateComplete(Position lastCommittedPosition, Object ctx);

        void terminateFailed(ManagedLedgerException exception, Object ctx);
    }

    interface FindEntryCallback {
        void findEntryComplete(Position position, Object ctx);

        void findEntryFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx);
    }

    interface ScanCallback {
        void scanComplete(Position position, ScanOutcome scanOutcome, Object ctx);

        void scanFailed(ManagedLedgerException exception, Optional<Position> failedReadPosition, Object ctx);
    }

    interface ResetCursorCallback {
        void resetComplete(Object ctx);

        void resetFailed(ManagedLedgerException exception, Object ctx);
    }

    interface ManagedLedgerInfoCallback {
        void getInfoComplete(ManagedLedgerInfo info, Object ctx);

        void getInfoFailed(ManagedLedgerException exception, Object ctx);
    }

    interface OffloadCallback {
        void offloadComplete(Position pos, Object ctx);

        void offloadFailed(ManagedLedgerException exception, Object ctx);
    }

    interface UpdatePropertiesCallback {
        void updatePropertiesComplete(Map<String, String> properties, Object ctx);

        void updatePropertiesFailed(ManagedLedgerException exception, Object ctx);
    }
}
