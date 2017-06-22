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

import com.google.common.annotations.Beta;

/**
 * Definition of all the callbacks used for the ManagedLedger asynchronous API.
 *
 */
@Beta
public interface AsyncCallbacks {

    public interface OpenLedgerCallback {
        public void openLedgerComplete(ManagedLedger ledger, Object ctx);

        public void openLedgerFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface DeleteLedgerCallback {
        public void deleteLedgerComplete(Object ctx);

        public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface OpenCursorCallback {
        public void openCursorComplete(ManagedCursor cursor, Object ctx);

        public void openCursorFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface DeleteCursorCallback {
        public void deleteCursorComplete(Object ctx);

        public void deleteCursorFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface AddEntryCallback {
        public void addComplete(Position position, Object ctx);

        public void addFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface CloseCallback {
        public void closeComplete(Object ctx);

        public void closeFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface ReadEntriesCallback {
        public void readEntriesComplete(List<Entry> entries, Object ctx);

        public void readEntriesFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface ReadEntryCallback {
        public void readEntryComplete(Entry entry, Object ctx);

        public void readEntryFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface MarkDeleteCallback {
        public void markDeleteComplete(Object ctx);

        public void markDeleteFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface ClearBacklogCallback {
        public void clearBacklogComplete(Object ctx);

        public void clearBacklogFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface SkipEntriesCallback {
        public void skipEntriesComplete(Object ctx);

        public void skipEntriesFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface DeleteCallback {
        public void deleteComplete(Object ctx);

        public void deleteFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface TerminateCallback {
        public void terminateComplete(Position lastCommittedPosition, Object ctx);

        public void terminateFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface FindEntryCallback {
        public void findEntryComplete(Position position, Object ctx);

        public void findEntryFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface ResetCursorCallback {
        public void resetComplete(Object ctx);

        public void resetFailed(ManagedLedgerException exception, Object ctx);
    }

    public interface ManagedLedgerInfoCallback {
        public void getInfoComplete(ManagedLedgerInfo info, Object ctx);

        public void getInfoFailed(ManagedLedgerException exception, Object ctx);
    }

}
