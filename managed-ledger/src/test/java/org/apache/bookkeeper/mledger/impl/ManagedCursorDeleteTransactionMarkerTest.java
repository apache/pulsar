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
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.PulsarMarkers.MessageIdData;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ManagedCursorDeleteTransactionMarkerTest extends MockedBookKeeperTestCase {
    @Test(timeOut = 20000)
    public void managedCursorDeleteTransactionMarkerTest() throws Exception {
        ManagedLedger ledger = factory.open("txn_marker");

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getNumberOfActiveEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ManagedCursor cursor = ledger.openCursor("test");
        MessageIdData messageIdData = MessageIdData.newBuilder()
                .setLedgerId(1)
                .setEntryId(1)
                .build();
        Position position1 = ledger.addEntry(Markers
                .newTxnCommitMarker(1, 1, 1, messageIdData).array());
        Position position2 = ledger.addEntry(Markers
                .newTxnCommitMarker(1, 1, 1, messageIdData).array());
        cursor.asyncDelete(position1, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {

            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {

            }
        }, null);

        Thread.sleep(1000L);
        assertEquals(cursor.getManagedLedger().getLastConfirmedEntry(), position2);
    }
}