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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.service.BrokerRandomReader.EntryResult;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.RandomReadInvisibleReason;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.Test;

public class RandomReaderBrokerTest {

    @Test
    public void testReadEntriesCoversRequestedPhysicalEntriesWithoutReadingPastInvisibleEntry() {
        EntryImpl visible0 = entry(3L, 0L, metadata(0L));
        EntryImpl delayed1 = entry(3L, 1L, metadata(1L).setDeliverAtTime(System.currentTimeMillis() + 60_000));
        EntryImpl visible2 = entry(3L, 2L, metadata(2L));
        EntryImpl beyondRequest3 = entry(3L, 3L, metadata(3L));

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position start = PositionFactory.create(3L, 0L);
        when(managedLedger.readEntries(start, 3))
                .thenReturn(CompletableFuture.completedFuture(List.of(visible0, delayed1, visible2)));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getManagedLedger()).thenReturn(managedLedger);
        when(topic.getName()).thenReturn("persistent://public/default/t1");
        when(topic.isDelayedDeliveryEnabled()).thenReturn(true);

        BrokerRandomReader reader = new BrokerRandomReader(7L, "rr", Map.of(), topic, mock(ServerCnx.class), true);

        List<EntryResult> results = reader.readEntries(start, 3, PositionFactory.create(3L, 10L)).join();
        assertEquals(results.size(), 3);
        assertTrue(results.get(0).isVisible());
        assertFalse(results.get(1).isVisible());
        assertEquals(results.get(1).invisibleReason(), RandomReadInvisibleReason.DELAYED_DELIVERY);
        assertTrue(results.get(2).isVisible());
        assertEquals(beyondRequest3.refCnt(), 1);

        results.forEach(EntryResult::release);
        beyondRequest3.release();
    }

    @Test
    public void testExceededMaxVisiblePositionReturnsInvisibleSlot() {
        EntryImpl visible0 = entry(3L, 0L, metadata(0L));
        EntryImpl overMax1 = entry(3L, 1L, metadata(1L));

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position start = PositionFactory.create(3L, 0L);
        when(managedLedger.readEntries(start, 2))
                .thenReturn(CompletableFuture.completedFuture(List.of(visible0, overMax1)));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getManagedLedger()).thenReturn(managedLedger);
        when(topic.getName()).thenReturn("persistent://public/default/t1");

        BrokerRandomReader reader = new BrokerRandomReader(7L, "rr", Map.of(), topic, mock(ServerCnx.class), true);

        List<EntryResult> results = reader.readEntries(start, 2, PositionFactory.create(3L, 0L)).join();
        assertEquals(results.size(), 2);
        assertTrue(results.get(0).isVisible());
        assertFalse(results.get(1).isVisible());
        assertEquals(results.get(1).invisibleReason(), RandomReadInvisibleReason.EXCEEDED_MAX_VISIBLE_POSITION);

        results.forEach(EntryResult::release);
    }

    @Test
    public void testReadEntriesStillCoversEntriesWhenStartExceedsMaxVisiblePosition() {
        EntryImpl overMax2 = entry(3L, 2L, metadata(2L));
        EntryImpl overMax3 = entry(3L, 3L, metadata(3L));

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position start = PositionFactory.create(3L, 2L);
        when(managedLedger.readEntries(start, 2))
                .thenReturn(CompletableFuture.completedFuture(List.of(overMax2, overMax3)));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getManagedLedger()).thenReturn(managedLedger);
        when(topic.getName()).thenReturn("persistent://public/default/t1");

        BrokerRandomReader reader = new BrokerRandomReader(7L, "rr", Map.of(), topic, mock(ServerCnx.class), true);

        List<EntryResult> results = reader.readEntries(start, 2, PositionFactory.create(3L, 1L)).join();
        assertEquals(results.size(), 2);
        assertFalse(results.get(0).isVisible());
        assertEquals(results.get(0).invisibleReason(), RandomReadInvisibleReason.EXCEEDED_MAX_VISIBLE_POSITION);
        assertFalse(results.get(1).isVisible());
        assertEquals(results.get(1).invisibleReason(), RandomReadInvisibleReason.EXCEEDED_MAX_VISIBLE_POSITION);

        results.forEach(EntryResult::release);
    }

    @Test
    public void testMarkerEntriesReturnSpecificInvisibleReasons() {
        EntryImpl serverOnlyMarker = entry(3L, 0L, Markers.newReplicatedSubscriptionsSnapshotRequest("sid", "us-west"));
        EntryImpl txnMarker = entry(3L, 1L, Markers.newTxnCommitMarker(1L, 2L, 3L));

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position start = PositionFactory.create(3L, 0L);
        when(managedLedger.readEntries(start, 2))
                .thenReturn(CompletableFuture.completedFuture(List.of(serverOnlyMarker, txnMarker)));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getManagedLedger()).thenReturn(managedLedger);
        when(topic.getName()).thenReturn("persistent://public/default/t1");

        BrokerRandomReader reader = new BrokerRandomReader(7L, "rr", Map.of(), topic, mock(ServerCnx.class), true);

        List<EntryResult> results = reader.readEntries(start, 2, PositionFactory.create(3L, 10L)).join();
        assertEquals(results.size(), 2);
        assertFalse(results.get(0).isVisible());
        assertEquals(results.get(0).invisibleReason(), RandomReadInvisibleReason.SERVER_ONLY_MARKER);
        assertFalse(results.get(1).isVisible());
        assertEquals(results.get(1).invisibleReason(), RandomReadInvisibleReason.TRANSACTION_MARKER);

        results.forEach(EntryResult::release);
    }

    @Test
    public void testReadEntriesReleasesAllEntriesWhenMetadataParsingFails() {
        EntryImpl visible0 = entry(3L, 0L, metadata(0L));
        EntryImpl invalid1 = EntryImpl.create(3L, 1L, Unpooled.wrappedBuffer(new byte[] {1, 2, 3}));
        EntryImpl unprocessed2 = entry(3L, 2L, metadata(2L));

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position start = PositionFactory.create(3L, 0L);
        when(managedLedger.readEntries(start, 3))
                .thenReturn(CompletableFuture.completedFuture(List.of(visible0, invalid1, unprocessed2)));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getManagedLedger()).thenReturn(managedLedger);
        when(topic.getName()).thenReturn("persistent://public/default/t1");

        BrokerRandomReader reader = new BrokerRandomReader(7L, "rr", Map.of(), topic, mock(ServerCnx.class), true);

        assertThrows(CompletionException.class,
                () -> reader.readEntries(start, 3, PositionFactory.create(3L, 10L)).join());
        assertEquals(visible0.refCnt(), 0);
        assertEquals(invalid1.refCnt(), 0);
        assertEquals(unprocessed2.refCnt(), 0);
    }

    private static EntryImpl entry(long ledgerId, long entryId, MessageMetadata metadata) {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] {1});
        ByteBuf data = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        payload.release();
        try {
            return EntryImpl.create(ledgerId, entryId, data);
        } finally {
            data.release();
        }
    }

    private static EntryImpl entry(long ledgerId, long entryId, ByteBuf data) {
        try {
            return EntryImpl.create(ledgerId, entryId, data);
        } finally {
            data.release();
        }
    }

    private static MessageMetadata metadata(long sequenceId) {
        return new MessageMetadata()
                .setProducerName("p")
                .setSequenceId(sequenceId)
                .setPublishTime(System.currentTimeMillis());
    }
}
