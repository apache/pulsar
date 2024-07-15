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
package org.apache.bookkeeper.mledger.util;

import static com.google.common.base.Preconditions.checkState;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceStability.Evolving
public class ManagedLedgerUtils {

    public static boolean ledgerExists(ManagedLedger ml, long ledgerId) {
        return ml.getLedgersInfo().get(ledgerId) != null;
    }

    public static Long getNextValidLedger(ManagedLedger ml, long ledgerId) {
        return ml.getLedgersInfo().ceilingKey(ledgerId + 1);

    }

    public static Position getPreviousPosition(ManagedLedger ml, Position position) {
        if (position.getEntryId() > 0) {
            return PositionFactory.create(position.getLedgerId(), position.getEntryId() - 1);
        }

        // The previous position will be the last position of an earlier ledgers
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>
                headMap = ml.getLedgersInfo().headMap(position.getLedgerId(), false);

        final Map.Entry<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> firstEntry = headMap.firstEntry();
        if (firstEntry == null) {
            // There is no previous ledger, return an invalid position in the current ledger
            return PositionFactory.create(position.getLedgerId(), -1);
        }

        // We need to find the most recent non-empty ledger
        for (long ledgerId : headMap.descendingKeySet()) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo li = headMap.get(ledgerId);
            if (li != null && li.getEntries() > 0) {
                return PositionFactory.create(li.getLedgerId(), li.getEntries() - 1);
            }
        }

        // in case there are only empty ledgers, we return a position in the first one
        return PositionFactory.create(firstEntry.getKey(), -1);
    }

    public static Position getFirstPosition(ManagedLedger ml) {
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = ml.getLedgersInfo();
        Long ledgerId = ledgers.firstKey();
        if (ledgerId == null) {
            return null;
        }
        Position lastConfirmedEntry = ml.getLastConfirmedEntry();
        if (ledgerId > lastConfirmedEntry.getLedgerId()) {
            checkState(ledgers.get(ledgerId).getEntries() == 0);
            ledgerId = lastConfirmedEntry.getLedgerId();
        }
        return PositionFactory.create(ledgerId, -1);
    }

}
