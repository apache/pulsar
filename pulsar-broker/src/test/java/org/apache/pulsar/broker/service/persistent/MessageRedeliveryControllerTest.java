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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageRedeliveryControllerTest {
    @DataProvider(name = "allowOutOfOrderDelivery")
    public Object[][] dataProvider() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testAddAndRemove(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);

        assertEquals(controller.isPositionToStickyKeyHashInitialized(), !allowOutOfOrderDelivery);
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);

        controller.add(1, 1);
        controller.add(1, 2);

        assertFalse(controller.isEmpty());
        assertEquals(controller.size(), 2);
        assertNull(controller.getHash(1, 1));
        assertNull(controller.getHash(1, 2));
        assertEquals(controller.isPositionToStickyKeyHashInitialized(), !allowOutOfOrderDelivery);

        controller.remove(1, 1);
        controller.remove(1, 2);

        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);

        controller.add(2, 1, 100);
        controller.add(2, 2, 101);
        controller.add(2, 3, 101);

        assertFalse(controller.isEmpty());
        assertEquals(controller.size(), 3);
        assertTrue(controller.isPositionToStickyKeyHashInitialized());
        assertEquals(controller.getHash(2, 1), Long.valueOf(100));
        assertEquals(controller.getHash(2, 2), Long.valueOf(101));
        assertEquals(controller.getHash(2, 3), Long.valueOf(101));
        if (!allowOutOfOrderDelivery) {
            assertTrue(controller.containsStickyKeyHash(100));
            assertTrue(controller.containsStickyKeyHash(101));
        }

        controller.remove(2, 1);
        controller.remove(2, 2);

        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(2, 1));
        assertNull(controller.getHash(2, 2));
        assertEquals(controller.getHash(2, 3), Long.valueOf(101));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(100));
            assertTrue(controller.containsStickyKeyHash(101));
        }

        controller.clear();
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);
        assertNull(controller.getHash(2, 3));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(101));
        }

        controller.add(2, 2, 201);
        controller.add(1, 3, 100);
        controller.add(3, 1, 300);
        controller.add(2, 1, 200);
        controller.add(3, 2, 301);
        controller.add(1, 2, 101);
        controller.add(1, 1, 100);

        controller.removeAllUpTo(1, 3);
        assertEquals(controller.size(), 4);
        assertNull(controller.getHash(1, 1));
        assertNull(controller.getHash(1, 2));
        assertNull(controller.getHash(1, 3));
        assertEquals(controller.getHash(2, 1), Long.valueOf(200));
        assertEquals(controller.getHash(2, 2), Long.valueOf(201));
        assertEquals(controller.getHash(3, 1), Long.valueOf(300));
        assertEquals(controller.getHash(3, 2), Long.valueOf(301));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(100));
            assertFalse(controller.containsStickyKeyHash(101));
            assertTrue(controller.containsStickyKeyHash(200));
            assertTrue(controller.containsStickyKeyHash(201));
            assertTrue(controller.containsStickyKeyHash(300));
            assertTrue(controller.containsStickyKeyHash(301));
        }

        controller.removeAllUpTo(3, 1);
        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(2, 1));
        assertNull(controller.getHash(2, 2));
        assertNull(controller.getHash(3, 1));
        assertEquals(controller.getHash(3, 2), Long.valueOf(301));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(200));
            assertFalse(controller.containsStickyKeyHash(201));
            assertFalse(controller.containsStickyKeyHash(300));
            assertTrue(controller.containsStickyKeyHash(301));
        }

        controller.removeAllUpTo(5, 10);
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);
        assertNull(controller.getHash(3, 2));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(301));
        }
    }

    @Test(timeOut = 10000)
    public void testOutOfOrderSentinelHashDoesNotInitializePositionHashMap() {
        MessageRedeliveryController controller = new MessageRedeliveryController(true);

        controller.add(1, 1, STICKY_KEY_HASH_NOT_SET);

        assertFalse(controller.isPositionToStickyKeyHashInitialized());
        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(1, 1));

        controller.removeAllUpTo(1, 1);
        assertTrue(controller.isEmpty());
        assertFalse(controller.isPositionToStickyKeyHashInitialized());
    }

    @Test(timeOut = 10000)
    public void testClassicOutOfOrderDoesNotInitializePositionHashMap() {
        MessageRedeliveryController controller = new MessageRedeliveryController(true, true);

        controller.add(1, 1, 100);

        assertFalse(controller.isPositionToStickyKeyHashInitialized());
        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(1, 1));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testContainsStickyKeyHashes(boolean allowOutOfOrderDelivery) throws Exception {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.add(1, 1, 100);
        controller.add(1, 2, 101);
        controller.add(1, 3, 102);
        controller.add(2, 2, 103);
        controller.add(2, 1, 104);

        if (allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHashes(Set.of(100)));
            assertFalse(controller.containsStickyKeyHashes(Set.of(101, 102, 103)));
            assertFalse(controller.containsStickyKeyHashes(Set.of(104, 105)));
        } else {
            assertTrue(controller.containsStickyKeyHashes(Set.of(100)));
            assertTrue(controller.containsStickyKeyHashes(Set.of(101, 102, 103)));
            assertTrue(controller.containsStickyKeyHashes(Set.of(104, 105)));
        }

        assertFalse(controller.containsStickyKeyHashes(Set.of()));
        assertFalse(controller.containsStickyKeyHashes(Set.of(99)));
        assertFalse(controller.containsStickyKeyHashes(Set.of(105, 106)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testGetMessagesToReplayNow(boolean allowOutOfOrderDelivery) throws Exception {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.add(2, 2);
        controller.add(1, 3);
        controller.add(3, 1);
        controller.add(2, 1);
        controller.add(3, 2);
        controller.add(1, 2);
        controller.add(1, 1);

        if (allowOutOfOrderDelivery) {
            // The entries are sorted by ledger ID but not by entry ID
            Position[] actual1 = controller.getMessagesToReplayNow(3, item -> true).toArray(new Position[3]);
            Position[] expected1 = { PositionFactory.create(1, 1),
                    PositionFactory.create(1, 2), PositionFactory.create(1, 3) };
            assertEqualsNoOrder(actual1, expected1);
        } else {
            // The entries are completely sorted
            Set<Position> actual2 = controller.getMessagesToReplayNow(6, item -> true);
            Set<Position> expected2 = new TreeSet<>();
            expected2.add(PositionFactory.create(1, 1));
            expected2.add(PositionFactory.create(1, 2));
            expected2.add(PositionFactory.create(1, 3));
            expected2.add(PositionFactory.create(2, 1));
            expected2.add(PositionFactory.create(2, 2));
            expected2.add(PositionFactory.create(3, 1));
            assertEquals(actual2, expected2);
        }
    }
}
