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

/**
 *  This is a marked interface for ledger handle that represent offloaded data.
 */
public interface OffloadedLedgerHandle {

    default long lastAccessTimestamp() {
        return -1;
    }

    default int getPendingRead() {
        return 0;
    }

    /**
     * Returns the greatest entry id lower than or equal to {@code entryId} whose location in the offloaded data is
     * known from the persistent index of the offloaded ledger, so that reading it does not require scanning the
     * entries that precede it. The result must not depend on previous reads (e.g. on cached offsets), so that
     * searches probing these entries take the same path regardless of what was read before.
     *
     * <p>Searches that may choose which entry to read, such as a binary search by timestamp, use it to prefer
     * entries that are cheap to read. It must not block nor trigger any I/O.
     *
     * @return the entry id, or -1 if unknown
     */
    default long getIndexedEntryIdFloor(long entryId) {
        return -1;
    }

    /**
     * Returns the lowest entry id greater than or equal to {@code entryId} whose location in the offloaded data is
     * known from the persistent index of the offloaded ledger, see {@link #getIndexedEntryIdFloor(long)}.
     *
     * @return the entry id, or -1 if unknown
     */
    default long getIndexedEntryIdCeiling(long entryId) {
        return -1;
    }
}
