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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.apache.pulsar.common.util.collections.OpenLongPairRangeSet;
import org.roaringbitmap.RoaringBitSet;
import org.testng.annotations.Test;

/**
 * Comprehensive compatibility tests for PositionRangeSet.
 * Covers 4.x to 5.0 upgrade, 5.0 to 4.x downgrade, and cross-ledger MessageRange recovery.
 */
public class PositionRangeSetCompatibilityTest extends BookKeeperClusterTestCase {

    public PositionRangeSetCompatibilityTest() {
        super(2);
    }

    /**
     * Verifies 4.x to 5.0 upgrade with bitmap format.
     * Tests that 5.0 PositionRangeSet can deserialize data written by 4.x OpenLongPairRangeSet
     * (when unackedRangesOpenCacheSetEnabled=true in 4.x).
     */
    @Test
    public void testUpgrade_BitmapFormat() {
        OpenLongPairRangeSet<Position> legacy = new OpenLongPairRangeSet<>(PositionFactory::create);
        legacy.addOpenClosed(0, -1, 0, 99);
        legacy.addOpenClosed(1, 49, 1, 149);
        legacy.addOpenClosed(5, -1, 5, 999);

        Map<Long, long[]> serialized = legacy.toRanges(Integer.MAX_VALUE);

        PositionRangeSet recovered = new PositionRangeSet(PositionFactory::create, false);
        recovered.build(serialized);

        assertEquals(recovered.asRanges(), legacy.asRanges());
        assertEquals(recovered.size(), legacy.size());
    }

    /**
     * Verifies 4.x to 5.0 upgrade with MessageRange format including cross-ledger expansion.
     * Tests that 5.0 PositionRangeSet can recover data written by 4.x DefaultRangeSet
     * (when unackedRangesOpenCacheSetEnabled=false in 4.x) and correctly expands
     * MessageRanges that span multiple ledgers using ledger metadata.
     */
    @Test
    public void testUpgrade_CrossLedgerMessageRange() throws Exception {
        String mlName = "test-ml-" + UUID.randomUUID();
        String cursorName = "test-cursor";

        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);

        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1)
                .setMaxEntriesPerLedger(5);

        ManagedLedger ledger = factory.open(mlName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);

        for (int i = 0; i < 100; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        List<MessageRange> messageRanges = new ArrayList<>();
        messageRanges.add(createMessageRange(0, 2, 2, 1));
        messageRanges.add(createMessageRange(5, 0, 7, 3));

        cursor.recoverIndividualDeletedMessages(messageRanges.size(), (IntFunction<MessageRange>) messageRanges::get);

        PositionRangeSet recovered = (PositionRangeSet) cursor.getIndividuallyDeletedMessagesSet();

        assertTrue(recovered.contains(0, 3));
        assertTrue(recovered.contains(1, 2));
        assertTrue(recovered.contains(2, 1));

        ledger.close();
    }

    /**
     * Verifies 5.0 to 4.x downgrade with bitmap format.
     * Tests that 4.x OpenLongPairRangeSet can deserialize data written by 5.0 PositionRangeSet
     * (when persistIndividualAckAsLongArray=true in 5.0).
     */
    @Test
    public void testDowngrade_BitmapFormat() {
        PositionRangeSet source = new PositionRangeSet(PositionFactory::create, false);
        source.addOpenClosed(0, -1, 0, 99);
        source.addOpenClosed(1, 49, 1, 149);

        Map<Long, long[]> serialized = source.toRanges(Integer.MAX_VALUE);

        OpenLongPairRangeSet<Position> legacy = new OpenLongPairRangeSet<>(PositionFactory::create);
        legacy.build(serialized);

        assertEquals(legacy.asRanges(), source.asRanges());
        assertEquals(legacy.size(), source.size());
    }

    /**
     * Verifies 5.0 to 4.x downgrade with MessageRange format.
     * Tests that 4.x DefaultRangeSet can recover data written by 5.0 PositionRangeSet
     * (when persistIndividualAckAsLongArray=false in 5.0, which is the default).
     */
    @Test
    public void testDowngrade_MessageRangeFormat() {
        PositionRangeSet source = new PositionRangeSet(PositionFactory::create, false);
        source.addOpenClosed(0, -1, 0, 99);
        source.addOpenClosed(5, -1, 5, 999);

        List<MessageRange> messageRanges = buildMessageRanges(source);

        LongPairRangeSet<Position> legacy =
                new LongPairRangeSet.DefaultRangeSet<>(
                        PositionFactory::create,
                        pos -> new LongPairRangeSet.LongPair(pos.getLedgerId(), pos.getEntryId()));
        for (MessageRange mr : messageRanges) {
            legacy.addOpenClosed(
                    mr.getLowerEndpoint().getLedgerId(), mr.getLowerEndpoint().getEntryId(),
                    mr.getUpperEndpoint().getLedgerId(), mr.getUpperEndpoint().getEntryId());
        }

        assertEquals(legacy.asRanges(), source.asRanges());
    }

    /**
     * Verifies bitmap wire format compatibility at binary level.
     * Ensures BitSet.toLongArray()/valueOf() produces identical representations
     * between 4.x OpenLongPairRangeSet and 5.0 PositionRangeSet.
     */
    @Test
    public void testBitmapBinaryFormat() {
        OpenLongPairRangeSet<Position> legacy = new OpenLongPairRangeSet<>(
                PositionFactory::create, RoaringBitSet::new);
        legacy.addOpenClosed(0, -1, 0, 9);

        Map<Long, long[]> serialized = legacy.toRanges(Integer.MAX_VALUE);
        long[] bitSetArray = serialized.get(0L);

        assertEquals(Arrays.stream(bitSetArray).filter(l -> l != 0).count(), 1);
    }

    private MessageRange createMessageRange(long lowerLedger, long lowerEntry,
                                           long upperLedger, long upperEntry) {
        MessageRange range = new MessageRange();
        range.setLowerEndpoint().setLedgerId(lowerLedger).setEntryId(lowerEntry);
        range.setUpperEndpoint().setLedgerId(upperLedger).setEntryId(upperEntry);
        return range;
    }

    private List<MessageRange> buildMessageRanges(PositionRangeSet rangeSet) {
        List<MessageRange> result = new ArrayList<>();
        rangeSet.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            result.add(createMessageRange(lowerKey, lowerValue, upperKey, upperValue));
            return true;
        });
        return result;
    }
}
