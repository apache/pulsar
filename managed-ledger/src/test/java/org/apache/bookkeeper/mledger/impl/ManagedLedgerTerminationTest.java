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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.NoMoreEntriesToReadException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedLedgerTerminationTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void terminateSimple() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        try {
            ledger.addEntry("entry-1".getBytes());
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateReopen() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());

        Position lastPosition = ledger.terminate();

        assertEquals(lastPosition, p0);

        ledger.close();

        ledger = factory.open("my_test_ledger");

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("Should have thrown exception");
        } catch (ManagedLedgerTerminatedException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateWithCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());

        List<Entry> entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p0);
        entries.forEach(Entry::release);

        Position lastPosition = ledger.terminate();
        assertEquals(lastPosition, p1);

        // Cursor can keep reading
        entries = c1.readEntries(1);
        assertEquals(entries.size(), 1);
        assertEquals(entries.get(0).getPosition(), p1);
        entries.forEach(Entry::release);
    }

    @Test(timeOut = 20000)
    public void terminateWithCursorReadOrWait() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());
        assertFalse(ledger.isTerminated());

        Position lastPosition = ledger.terminate();
        assertTrue(ledger.isTerminated());
        assertEquals(lastPosition, p1);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p0);
        assertEquals(entries.get(1).getPosition(), p1);
        entries.forEach(Entry::release);

        // Normal read will just return no entries
        assertEquals(c1.readEntries(10), Collections.emptyList());

        // Read or wait will fail
        try {
            c1.readEntriesOrWait(10);
            fail("Should have thrown exception");
        } catch (NoMoreEntriesToReadException e) {
            // Expected
        }
    }

    @Test(timeOut = 20000)
    public void terminateWithNonDurableCursor() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger");

        Position p0 = ledger.addEntry("entry-0".getBytes());
        Position p1 = ledger.addEntry("entry-1".getBytes());
        assertFalse(ledger.isTerminated());

        Position lastPosition = ledger.terminate();
        assertTrue(ledger.isTerminated());
        assertEquals(lastPosition, p1);

        ManagedCursor c1 = ledger.newNonDurableCursor(PositionImpl.earliest);

        List<Entry> entries = c1.readEntries(10);
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getPosition(), p0);
        assertEquals(entries.get(1).getPosition(), p1);
        entries.forEach(Entry::release);

        // Normal read will just return no entries
        assertEquals(c1.readEntries(10), Collections.emptyList());

        // Read or wait will fail
        try {
            c1.readEntriesOrWait(10);
            fail("Should have thrown exception");
        } catch (NoMoreEntriesToReadException e) {
            // Expected
        }
    }

}
