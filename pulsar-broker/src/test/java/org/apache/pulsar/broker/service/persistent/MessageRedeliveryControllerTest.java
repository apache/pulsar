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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.TreeSet;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.pulsar.common.util.collections.LongPairSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageRedeliveryControllerTest {
    @DataProvider(name = "allowOutOfOrderDelivery")
    public Object[][] dataProvider() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testAddAndRemove(boolean allowOutOfOrderDelivery) throws Exception {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);

        Field messagesToRedeliverField = MessageRedeliveryController.class.getDeclaredField("messagesToRedeliver");
        messagesToRedeliverField.setAccessible(true);
        LongPairSet messagesToRedeliver = (LongPairSet) messagesToRedeliverField.get(controller);

        Field hashesToBeBlockedField = MessageRedeliveryController.class.getDeclaredField("hashesToBeBlocked");
        hashesToBeBlockedField.setAccessible(true);
        ConcurrentLongLongPairHashMap hashesToBeBlocked = (ConcurrentLongLongPairHashMap) hashesToBeBlockedField
                .get(controller);

        if (allowOutOfOrderDelivery) {
            assertNull(hashesToBeBlocked);
        } else {
            assertNotNull(hashesToBeBlocked);
        }

        assertTrue(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 0);
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 0);
        }

        assertTrue(controller.add(1, 1));
        assertTrue(controller.add(1, 2));
        assertFalse(controller.add(1, 1));

        assertFalse(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 2);
        assertTrue(messagesToRedeliver.contains(1, 1));
        assertTrue(messagesToRedeliver.contains(1, 2));
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 0);
            assertFalse(hashesToBeBlocked.containsKey(1, 1));
            assertFalse(hashesToBeBlocked.containsKey(1, 2));
        }

        assertTrue(controller.remove(1, 1));
        assertTrue(controller.remove(1, 2));
        assertFalse(controller.remove(1, 1));

        assertTrue(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 0);
        assertFalse(messagesToRedeliver.contains(1, 1));
        assertFalse(messagesToRedeliver.contains(1, 2));
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 0);
        }

        assertTrue(controller.add(2, 1, 100));
        assertTrue(controller.add(2, 2, 101));
        assertTrue(controller.add(2, 3, 101));
        assertFalse(controller.add(2, 1, 100));

        assertFalse(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 3);
        assertTrue(messagesToRedeliver.contains(2, 1));
        assertTrue(messagesToRedeliver.contains(2, 2));
        assertTrue(messagesToRedeliver.contains(2, 3));
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 3);
            assertEquals(hashesToBeBlocked.get(2, 1).first, 100);
            assertEquals(hashesToBeBlocked.get(2, 2).first, 101);
            assertEquals(hashesToBeBlocked.get(2, 3).first, 101);
        }

        controller.clear();
        assertTrue(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 0);
        assertTrue(messagesToRedeliver.isEmpty());
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 0);
            assertTrue(hashesToBeBlocked.isEmpty());
        }

        controller.add(2, 2, 201);
        controller.add(1, 3, 100);
        controller.add(3, 1, 300);
        controller.add(2, 1, 200);
        controller.add(3, 2, 301);
        controller.add(1, 2, 101);
        controller.add(1, 1, 100);

        controller.removeAllUpTo(1, 3);
        assertEquals(messagesToRedeliver.size(), 4);
        assertTrue(messagesToRedeliver.contains(2, 1));
        assertTrue(messagesToRedeliver.contains(2, 2));
        assertTrue(messagesToRedeliver.contains(3, 1));
        assertTrue(messagesToRedeliver.contains(3, 2));
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 4);
            assertEquals(hashesToBeBlocked.get(2, 1).first, 200);
            assertEquals(hashesToBeBlocked.get(2, 2).first, 201);
            assertEquals(hashesToBeBlocked.get(3, 1).first, 300);
            assertEquals(hashesToBeBlocked.get(3, 2).first, 301);
        }

        controller.removeAllUpTo(3, 1);
        assertEquals(messagesToRedeliver.size(), 1);
        assertTrue(messagesToRedeliver.contains(3, 2));
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 1);
            assertEquals(hashesToBeBlocked.get(3, 2).first, 301);
        }

        controller.removeAllUpTo(5, 10);
        assertTrue(controller.isEmpty());
        assertEquals(messagesToRedeliver.size(), 0);
        if (!allowOutOfOrderDelivery) {
            assertEquals(hashesToBeBlocked.size(), 0);
        }
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
            assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet(100)));
            assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet(101, 102, 103)));
            assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet(104, 105)));
        } else {
            assertTrue(controller.containsStickyKeyHashes(Sets.newHashSet(100)));
            assertTrue(controller.containsStickyKeyHashes(Sets.newHashSet(101, 102, 103)));
            assertTrue(controller.containsStickyKeyHashes(Sets.newHashSet(104, 105)));
        }

        assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet()));
        assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet(99)));
        assertFalse(controller.containsStickyKeyHashes(Sets.newHashSet(105, 106)));
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
            PositionImpl[] actual1 = controller.getMessagesToReplayNow(3).toArray(new PositionImpl[3]);
            PositionImpl[] expected1 = { PositionImpl.get(1, 1), PositionImpl.get(1, 2), PositionImpl.get(1, 3) };
            assertEqualsNoOrder(actual1, expected1);
        } else {
            // The entries are completely sorted
            Set<PositionImpl> actual2 = controller.getMessagesToReplayNow(6);
            Set<PositionImpl> expected2 = new TreeSet<>();
            expected2.add(PositionImpl.get(1, 1));
            expected2.add(PositionImpl.get(1, 2));
            expected2.add(PositionImpl.get(1, 3));
            expected2.add(PositionImpl.get(2, 1));
            expected2.add(PositionImpl.get(2, 2));
            expected2.add(PositionImpl.get(3, 1));
            assertEquals(actual2, expected2);
        }
    }
}
