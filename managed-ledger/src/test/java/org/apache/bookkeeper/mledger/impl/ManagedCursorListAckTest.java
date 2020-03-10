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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedCursorListAckTest extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;

    @Test(timeOut = 20000)
    void testMultiPositionDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        ManagedCursor c1 = ledger.openCursor("c1");
        Position p0 = c1.getMarkDeletedPosition();
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes(Encoding));
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes(Encoding));
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes(Encoding));
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes(Encoding));
        Position p5 = ledger.addEntry("dummy-entry-5".getBytes(Encoding));
        Position p6 = ledger.addEntry("dummy-entry-6".getBytes(Encoding));
        Position p7 = ledger.addEntry("dummy-entry-7".getBytes(Encoding));

        assertEquals(c1.getNumberOfEntries(), 7);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 7);

        c1.delete(Lists.newArrayList(p2, p3, p5, p7));

        assertEquals(c1.getNumberOfEntries(), 3);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 3);
        assertEquals(c1.getMarkDeletedPosition(), p0);

        c1.delete(Lists.newArrayList(p1));

        assertEquals(c1.getNumberOfEntries(), 2);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 2);
        assertEquals(c1.getMarkDeletedPosition(), p3);

        c1.delete(Lists.newArrayList(p4, p6, p7));

        assertEquals(c1.getNumberOfEntries(), 0);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 0);
        assertEquals(c1.getMarkDeletedPosition(), p7);
    }

}
