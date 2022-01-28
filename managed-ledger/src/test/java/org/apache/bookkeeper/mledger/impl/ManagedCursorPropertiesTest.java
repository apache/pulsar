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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.testng.annotations.Test;

public class ManagedCursorPropertiesTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    void testPropertiesClose() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), Collections.emptyMap());

        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());
        ledger.addEntry("entry-4".getBytes());

        Map<String, Long> properties = new TreeMap<>();
        properties.put("a", 1L);
        properties.put("b", 2L);
        properties.put("c", 3L);
        c1.markDelete(p3, properties);

        assertEquals(c1.getProperties(), properties);

        Map<String, Long> properties2 = new TreeMap<>();
        properties2.put("a", 4L);
        properties2.put("b", 5L);
        properties2.put("c", 6L);
        c1.markDelete(p3, properties2);

        assertEquals(c1.getProperties(), properties2);

        ledger.close();

        // Reopen the managed ledger
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getProperties(), properties2);
    }

    @Test(timeOut = 20000)
    void testPropertiesRecoveryAfterCrash() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), Collections.emptyMap());

        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());
        ledger.addEntry("entry-4".getBytes());

        Map<String, Long> properties = new TreeMap<>();
        properties.put("a", 1L);
        properties.put("b", 2L);
        properties.put("c", 3L);
        c1.markDelete(p3, properties);

        // Create a new factory to force a managed ledger close and recovery
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        // Reopen the managed ledger
        ledger = factory2.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getProperties(), properties);

        factory2.shutdown();
    }

    @Test(timeOut = 20000)
    void testPropertiesOnDelete() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        ManagedCursor c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), Collections.emptyMap());

        ledger.addEntry("entry-1".getBytes());
        Position p2 = ledger.addEntry("entry-2".getBytes());
        Position p3 = ledger.addEntry("entry-3".getBytes());
        ledger.addEntry("entry-4".getBytes());

        Map<String, Long> properties = new TreeMap<>();
        properties.put("a", 1L);
        properties.put("b", 2L);
        properties.put("c", 3L);
        c1.markDelete(p2, properties);

        assertEquals(c1.getProperties(), properties);

        // Delete p3 and ensure the properties are carried over
        c1.markDelete(p3, properties);

        assertEquals(c1.getProperties(), properties);

        ledger.close();

        // Reopen the managed ledger
        ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getMarkDeletedPosition(), p3);
        assertEquals(c1.getProperties(), properties);
    }

    @Test
    void testPropertiesAtCreation() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger_at_creation", new ManagedLedgerConfig());


        Map<String, Long> properties = new TreeMap<>();
        properties.put("a", 1L);
        properties.put("b", 2L);
        properties.put("c", 3L);

        ManagedCursor c1 = ledger.openCursor("c1", InitialPosition.Latest, properties);
        assertEquals(c1.getProperties(), properties);

        ledger.addEntry("entry-1".getBytes());

        ledger.close();

        // Reopen the managed ledger
        ledger = factory.open("my_test_ledger_at_creation", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), properties);
    }

}
