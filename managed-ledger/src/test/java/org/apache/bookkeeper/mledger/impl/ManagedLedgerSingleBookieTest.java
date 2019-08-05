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

import com.google.common.base.Charsets;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedLedgerSingleBookieTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    public ManagedLedgerSingleBookieTest() {
        // Just one bookie
        super(1);
    }

    @Test // (timeOut = 20000)
    public void simple() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger", config);

        assertEquals(ledger.getNumberOfEntries(), 0);
        assertEquals(ledger.getTotalSize(), 0);

        ledger.addEntry("dummy-entry-1".getBytes(Encoding));

        assertEquals(ledger.getNumberOfEntries(), 1);
        assertEquals(ledger.getTotalSize(), "dummy-entry-1".getBytes(Encoding).length);

        ManagedCursor cursor = ledger.openCursor("c1");

        assertFalse(cursor.hasMoreEntries());
        assertEquals(cursor.readEntries(100), new ArrayList<Entry>());

        ledger.addEntry("dummy-entry-2".getBytes(Encoding));

        assertTrue(cursor.hasMoreEntries());

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(e -> e.release());

        entries = cursor.readEntries(100);
        assertEquals(entries.size(), 0);
        entries.forEach(e -> e.release());

        ledger.close();
    }
}
