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
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
@SuppressWarnings("checkstyle:javadoctype")
public class ManagedLedgerInfo {
    /** Z-Node version. */
    public long version;
    public String creationDate;
    public String modificationDate;

    public List<LedgerInfo> ledgers;
    public PositionInfo terminatedPosition;

    public Map<String, CursorInfo> cursors;

    public Map<String, String> properties;

    public static class LedgerInfo {
        public long ledgerId;
        public Long entries;
        public Long size;
        public Long timestamp;
        public boolean isOffloaded;
    }

    public static class CursorInfo {
        /** Z-Node version. */
        public long version;
        public String creationDate;
        public String modificationDate;

        // If the ledger id is -1, then the mark-delete position is
        // the one from the (ledgerId, entryId) snapshot below
        public long cursorsLedgerId;

        // Last snapshot of the mark-delete position
        public PositionInfo markDelete;
        public List<MessageRangeInfo> individualDeletedMessages;
        public Map<String, Long> properties;
    }

    public static class PositionInfo {
        public long ledgerId;
        public long entryId;

        @Override
        public String toString() {
            return String.format("%d:%d", ledgerId, entryId);
        }
    }

    public static class MessageRangeInfo {
        // Starting of the range (not included)
        public PositionInfo from = new PositionInfo();

        // End of the range (included)
        public PositionInfo to = new PositionInfo();

        @Override
        public String toString() {
            return String.format("(%s, %s]", from, to);
        }
    }
}
