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
package org.apache.bookkeeper.mledger.impl.cache;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;

class ReadEntryUtils {

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry) {
        // ml is the original owner of a cache shared by managed-ledger name, not necessarily the read's caller.
        // This type check assumes read-only and shadow ledgers never share such a cache; otherwise a shadow
        // point read could bypass its last-confirmed bound.
        if (ml instanceof ReadOnlyManagedLedgerImpl && firstEntry == lastEntry) {
            // A shared cache may retain an older read-only instance whose inventory predates this ledger.
            // Published point positions must not depend on that inventory or an open handle's piggyback LAC.
            // Closed handles still enforce their final LAC; cursor/count frontiers are not advanced here.
            return handle.isClosed() ? handle.readAsync(firstEntry, lastEntry)
                    : handle.readUnconfirmedAsync(firstEntry, lastEntry);
        }
        if (ml.getOptionalLedgerInfo(handle.getId()).isEmpty()) {
            // Other reads of a handle outside this managed ledger's inventory are bounded by the handle's LAC.
            return handle.readAsync(firstEntry, lastEntry);
        }
        // Compare the entry range with the lastConfirmedEntry maintained by the managed ledger because the entry cache
        // of `ShadowManagedLedgerImpl` reads entries via `ReadOnlyLedgerHandle`, which never updates `lastAddConfirmed`
        final var lastConfirmedEntry = ml.getLastConfirmedEntry();
        if (lastConfirmedEntry == null) {
            return CompletableFuture.failedFuture(new ManagedLedgerException(
                    "LastConfirmedEntry is null when reading ledger " + handle.getId()));
        }
        if (handle.getId() > lastConfirmedEntry.getLedgerId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading ledger " + handle.getId()));
        }
        if (handle.getId() == lastConfirmedEntry.getLedgerId() && lastEntry > lastConfirmedEntry.getEntryId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading entry " + lastEntry));
        }
        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }
}
