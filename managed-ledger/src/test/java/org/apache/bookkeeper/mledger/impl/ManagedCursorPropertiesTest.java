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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.apache.bookkeeper.mledger.util.Futures.executeWithRetry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
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

        Map<String, String> cursorProperties = new TreeMap<>();
        cursorProperties.put("custom1", "one");
        cursorProperties.put("custom2", "two");

        ManagedCursor c1 = ledger.openCursor("c1", InitialPosition.Latest, Collections.emptyMap(), cursorProperties);

        assertEquals(c1.getProperties(), Collections.emptyMap());
        assertEquals(c1.getCursorProperties(), cursorProperties);

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
        assertEquals(c1.getCursorProperties(), cursorProperties);

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

        Map<String, String> cursorProperties = new TreeMap<>();
        cursorProperties.put("custom1", "one");
        cursorProperties.put("custom2", "two");

        ManagedCursor c1 = ledger.openCursor("c1", InitialPosition.Latest, properties, cursorProperties);
        assertEquals(c1.getProperties(), properties);
        assertEquals(c1.getCursorProperties(), cursorProperties);

        ledger.addEntry("entry-1".getBytes());

        ledger.close();

        // Reopen the managed ledger
        ledger = factory.open("my_test_ledger_at_creation", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), properties);
        assertEquals(c1.getCursorProperties(), cursorProperties);
    }

    @Test
    void testUpdateCursorProperties() throws Exception {
        ManagedLedger ledger = factory.open("testUpdateCursorProperties", new ManagedLedgerConfig());

        Map<String, Long> properties = new TreeMap<>();
        properties.put("a", 1L);

        Map<String, String> cursorProperties = new TreeMap<>();
        cursorProperties.put("custom1", "one");
        cursorProperties.put("custom2", "two");

        ManagedCursor c1 = ledger.openCursor("c1", InitialPosition.Latest, properties, cursorProperties);
        assertEquals(c1.getProperties(), properties);
        assertEquals(c1.getCursorProperties(), cursorProperties);

        ledger.addEntry("entry-1".getBytes());

        Map<String, String> cursorPropertiesUpdated = new TreeMap<>();
        cursorPropertiesUpdated.put("custom1", "three");
        cursorPropertiesUpdated.put("custom2", "four");

        c1.setCursorProperties(cursorPropertiesUpdated).get(10, TimeUnit.SECONDS);

        ledger.close();

        // Reopen the managed ledger
        ledger = factory.open("testUpdateCursorProperties", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), properties);
        assertEquals(c1.getCursorProperties(), cursorPropertiesUpdated);

        c1.putCursorProperty("custom3", "Five").get();
        cursorPropertiesUpdated.put("custom3", "Five");
        c1.removeCursorProperty("custom1").get();
        cursorPropertiesUpdated.remove("custom1");
        assertEquals(c1.getCursorProperties(), cursorPropertiesUpdated);

        // Create a new factory to force a managed ledger close and recovery
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        // Reopen the managed ledger
        ledger = factory2.open("testUpdateCursorProperties", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        assertEquals(c1.getProperties(), properties);
        assertEquals(c1.getCursorProperties(), cursorPropertiesUpdated);

        ledger.close();

        factory2.shutdown();

        // Create a new factory to force a managed ledger close and recovery
        ManagedLedgerFactory factory3 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        // Reopen the managed ledger
        ledger = factory3.open("testUpdateCursorProperties", new ManagedLedgerConfig());
        c1 = ledger.openCursor("c1");

        c1.putCursorProperty(CURSOR_INTERNAL_PROPERTY_PREFIX + "test", "test").get(10, TimeUnit.SECONDS);
        c1.putCursorProperty("custom4", "custom4").get(10, TimeUnit.SECONDS);
        c1.setCursorProperties(cursorPropertiesUpdated).get(10, TimeUnit.SECONDS);

        cursorPropertiesUpdated.put(CURSOR_INTERNAL_PROPERTY_PREFIX + "test", "test");

        try {
            c1.setCursorProperties(cursorPropertiesUpdated).get(10, TimeUnit.SECONDS);
            Assert.fail("Should fail");
        } catch (Exception e) {
            assertTrue(
                    FutureUtil.unwrapCompletionException(e).getMessage().contains("The property key can't start with"));
        }

        assertEquals(c1.getCursorProperties(), cursorPropertiesUpdated);

        ledger.close();

        factory3.shutdown();
    }

    @Test
    public void testUpdateCursorPropertiesConcurrent() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig());
        ManagedCursor c1 = ledger.openCursor("c1");

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");

        futures.add(executeWithRetry(() -> c1.setCursorProperties(map),
                ManagedLedgerException.BadVersionException.class, 3));

        futures.add(executeWithRetry(() -> c1.putCursorProperty("a", "2"),
                ManagedLedgerException.BadVersionException.class, 3));

        futures.add(executeWithRetry(() -> c1.removeCursorProperty("c"),
                ManagedLedgerException.BadVersionException.class, 3));

        for (CompletableFuture<Void> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

        assertEquals(c1.getCursorProperties().get("a"), "2");
        assertEquals(c1.getCursorProperties().get("b"), "2");
        assertNull(c1.getCursorProperties().get("c"));
    }
}
