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

import static org.testng.Assert.assertEquals;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.CursorInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.MessageRangeInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void testGetManagedLedgerInfoWithClose() throws Exception {
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setMaxEntriesPerLedger(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("testGetManagedLedgerInfo", conf);
        ManagedCursor c1 = ledger.openCursor("c1");

        PositionImpl p1 = (PositionImpl) ledger.addEntry("entry1".getBytes());
        PositionImpl p2 = (PositionImpl) ledger.addEntry("entry2".getBytes());
        PositionImpl p3 = (PositionImpl) ledger.addEntry("entry3".getBytes());
        ledger.addEntry("entry4".getBytes());

        c1.delete(p2);
        c1.delete(p3);

        ledger.close();

        ManagedLedgerInfo info = factory.getManagedLedgerInfo("testGetManagedLedgerInfo");

        assertEquals(info.ledgers.size(), 4);

        assertEquals(info.ledgers.get(0).ledgerId, 3);
        assertEquals(info.ledgers.get(1).ledgerId, 5);
        assertEquals(info.ledgers.get(2).ledgerId, 6);
        assertEquals(info.ledgers.get(3).ledgerId, 7);

        assertEquals(info.cursors.size(), 1);

        CursorInfo cursorInfo = info.cursors.get("c1");
        assertEquals(cursorInfo.markDelete.ledgerId, 3);
        assertEquals(cursorInfo.markDelete.entryId, -1);

        assertEquals(cursorInfo.individualDeletedMessages.size(), 2);

        MessageRangeInfo mri = cursorInfo.individualDeletedMessages.get(0);
        assertEquals(mri.from.ledgerId, p2.getLedgerId());
        assertEquals(mri.from.entryId, -1);
        assertEquals(mri.to.ledgerId, p2.getLedgerId());
        assertEquals(mri.to.entryId, 0);
    }

}
