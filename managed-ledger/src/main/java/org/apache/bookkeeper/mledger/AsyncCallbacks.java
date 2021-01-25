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
package org.apache.bookkeeper.mledger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

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

    interface ReadEntriesCallback {
        void readEntriesComplete(List<Entry> entries, Object ctx);

        void readEntriesFailed(ManagedLedgerException exception, Object ctx);
    }

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
