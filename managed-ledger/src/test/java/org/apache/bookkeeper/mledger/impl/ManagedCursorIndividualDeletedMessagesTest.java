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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import java.lang.reflect.Method;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.testng.annotations.Test;

public class ManagedCursorIndividualDeletedMessagesTest {
    @Test(timeOut = 10000)
    void testRecoverIndividualDeletedMessages() throws Exception {
        BookKeeper bookkeeper = mock(BookKeeper.class);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setUnackedRangesOpenCacheSetEnabled(true);

        NavigableMap<Long, LedgerInfo> ledgersInfo = new ConcurrentSkipListMap<>();
        ledgersInfo.put(1L, createLedgerInfo(1, 100, 1024));
        ledgersInfo.put(3L, createLedgerInfo(3, 50, 512));
        ledgersInfo.put(5L, createLedgerInfo(5, 200, 2048));
        ledgersInfo.put(10L, createLedgerInfo(10, 2, 32));
        ledgersInfo.put(20L, createLedgerInfo(20, 10, 256));

        ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        doReturn(ledgersInfo).when(ledger).getLedgersInfo();

        ManagedCursorImpl cursor = spy(new ManagedCursorImpl(bookkeeper, config, ledger, "test-cursor"));
        LongPairRangeSet<PositionImpl> deletedMessages = cursor.getIndividuallyDeletedMessagesSet();

        Method recoverMethod = ManagedCursorImpl.class.getDeclaredMethod("recoverIndividualDeletedMessages",
                List.class);
        recoverMethod.setAccessible(true);

        // (1) [(1:5..1:10]]
        List<MessageRange> messageRangeList = Lists.newArrayList();
        messageRangeList.add(createMessageRange(1, 5, 1, 10));
        List<Range<PositionImpl>> expectedRangeList = Lists.newArrayList();
        expectedRangeList.add(createPositionRange(1, 5, 1, 10));
        recoverMethod.invoke(cursor, messageRangeList);
        assertEquals(deletedMessages.size(), 1);
        assertEquals(deletedMessages.asRanges(), expectedRangeList);

        // (2) [(1:10..3:0]]
        messageRangeList.clear();
        messageRangeList.add(createMessageRange(1, 10, 3, 0));
        expectedRangeList.clear();
        expectedRangeList.add(createPositionRange(1, 10, 1, 99));
        expectedRangeList.add(createPositionRange(3, -1, 3, 0));
        recoverMethod.invoke(cursor, messageRangeList);
        assertEquals(deletedMessages.size(), 2);
        assertEquals(deletedMessages.asRanges(), expectedRangeList);

        // (3) [(1:20..10:1],(20:2..20:9]]
        messageRangeList.clear();
        messageRangeList.add(createMessageRange(1, 20, 10, 1));
        messageRangeList.add(createMessageRange(20, 2, 20, 9));
        expectedRangeList.clear();
        expectedRangeList.add(createPositionRange(1, 20, 1, 99));
        expectedRangeList.add(createPositionRange(3, -1, 3, 49));
        expectedRangeList.add(createPositionRange(5, -1, 5, 199));
        expectedRangeList.add(createPositionRange(10, -1, 10, 1));
        expectedRangeList.add(createPositionRange(20, 2, 20, 9));
        recoverMethod.invoke(cursor, messageRangeList);
        assertEquals(deletedMessages.size(), 5);
        assertEquals(deletedMessages.asRanges(), expectedRangeList);
    }

    private static LedgerInfo createLedgerInfo(long ledgerId, long entries, long size) {
        return LedgerInfo.newBuilder().setLedgerId(ledgerId).setEntries(entries).setSize(size)
                .setTimestamp(System.currentTimeMillis()).build();
    }

    private static MessageRange createMessageRange(long lowerLedgerId, long lowerEntryId, long upperLedgerId,
            long upperEntryId) {
        NestedPositionInfo.Builder nestedPositionBuilder = NestedPositionInfo.newBuilder();
        MessageRange.Builder messageRangeBuilder = MessageRange.newBuilder();

        nestedPositionBuilder.setLedgerId(lowerLedgerId);
        nestedPositionBuilder.setEntryId(lowerEntryId);
        messageRangeBuilder.setLowerEndpoint(nestedPositionBuilder.build());

        nestedPositionBuilder.setLedgerId(upperLedgerId);
        nestedPositionBuilder.setEntryId(upperEntryId);
        messageRangeBuilder.setUpperEndpoint(nestedPositionBuilder.build());

        return messageRangeBuilder.build();
    }

    private static Range<PositionImpl> createPositionRange(long lowerLedgerId, long lowerEntryId, long upperLedgerId,
            long upperEntryId) {
        return Range.openClosed(new PositionImpl(lowerLedgerId, lowerEntryId),
                new PositionImpl(upperLedgerId, upperEntryId));
    }
}
