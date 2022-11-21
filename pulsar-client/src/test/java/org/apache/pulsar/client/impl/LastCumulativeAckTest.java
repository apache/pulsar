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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.testng.annotations.Test;

public class LastCumulativeAckTest {

    @Test
    public void testUpdate() {
        final LastCumulativeAck lastCumulativeAck = new LastCumulativeAck();
        assertFalse(lastCumulativeAck.isFlushRequired());
        assertEquals(lastCumulativeAck.getMessageId(), LastCumulativeAck.DEFAULT_MESSAGE_ID);
        assertNull(lastCumulativeAck.getBitSetRecyclable());

        final MessageIdImpl messageId1 = new MessageIdImpl(0L, 1L, 10);
        final BitSetRecyclable bitSetRecyclable1 = BitSetRecyclable.create();
        bitSetRecyclable1.set(0, 3);
        lastCumulativeAck.update(messageId1, bitSetRecyclable1);
        assertTrue(lastCumulativeAck.isFlushRequired());
        assertSame(lastCumulativeAck.getMessageId(), messageId1);
        assertSame(lastCumulativeAck.getBitSetRecyclable(), bitSetRecyclable1);

        final MessageIdImpl messageId2 = new MessageIdImpl(0L, 2L, 8);
        lastCumulativeAck.update(messageId2, bitSetRecyclable1);
        // bitSetRecyclable1 is not recycled
        assertEquals(bitSetRecyclable1.toString(), "{0, 1, 2}");

        final BitSetRecyclable bitSetRecyclable2 = BitSetRecyclable.create();
        bitSetRecyclable2.set(0, 2);

        // `update()` only accepts a newer message ID, so this call here has no side effect
        lastCumulativeAck.update(messageId2, bitSetRecyclable2);
        assertSame(lastCumulativeAck.getBitSetRecyclable(), bitSetRecyclable1);

        final MessageIdImpl messageId3 = new MessageIdImpl(0L, 3L, 9);
        lastCumulativeAck.update(messageId3, bitSetRecyclable2);
        // bitSetRecyclable1 is recycled because it's replaced in `update`
        assertEquals(bitSetRecyclable1.toString(), "{}");
        assertSame(lastCumulativeAck.getMessageId(), messageId3);
        assertSame(lastCumulativeAck.getBitSetRecyclable(), bitSetRecyclable2);
        bitSetRecyclable2.recycle();
    }

    @Test
    public void testFlush() {
        final LastCumulativeAck lastCumulativeAck = new LastCumulativeAck();
        assertNull(lastCumulativeAck.flush());

        final MessageIdImpl messageId = new MessageIdImpl(0L, 1L, 3);
        final BitSetRecyclable bitSetRecyclable = BitSetRecyclable.create();
        bitSetRecyclable.set(0, 3);
        lastCumulativeAck.update(messageId, bitSetRecyclable);
        assertTrue(lastCumulativeAck.isFlushRequired());

        final LastCumulativeAck lastCumulativeAckToFlush = lastCumulativeAck.flush();
        assertFalse(lastCumulativeAck.isFlushRequired());
        assertSame(lastCumulativeAckToFlush.getMessageId(), messageId);
        assertNotSame(lastCumulativeAckToFlush.getBitSetRecyclable(), bitSetRecyclable);
        assertEquals(lastCumulativeAckToFlush.getBitSetRecyclable(), bitSetRecyclable);
    }

    @Test
    public void testCompareTo() {
        LastCumulativeAck lastCumulativeAck = new LastCumulativeAck();
        lastCumulativeAck.update(new MessageIdImpl(0L, 1L, -1), null);

        assertTrue(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 0L, -1)) > 0);
        assertEquals(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 1L, -1)), 0);
        assertTrue(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 2L, -1)) < 0);
        assertTrue(lastCumulativeAck.compareTo(new BatchMessageIdImpl(0L, 1L, -1, 0)) > 0);

        lastCumulativeAck = new LastCumulativeAck();
        lastCumulativeAck.update(new BatchMessageIdImpl(0L, 1L, -1, 1), null);

        assertTrue(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 0L, -1)) > 0);
        assertTrue(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 1L, -1)) < 0);
        assertTrue(lastCumulativeAck.compareTo(new MessageIdImpl(0L, 2L, -1)) < 0);
        assertTrue(lastCumulativeAck.compareTo(new BatchMessageIdImpl(0L, 1L, -1, 0)) > 0);
        assertTrue(lastCumulativeAck.compareTo(new BatchMessageIdImpl(0L, 1L, -1, 2)) < 0);
        assertEquals(lastCumulativeAck.compareTo(new BatchMessageIdImpl(0L, 1L, -1, 1)), 0);
    }
}
