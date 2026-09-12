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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntryCacheDisabledTest {
    private ManagedLedgerImpl mockManagedLedger;
    private ManagedLedgerConfig managedLedgerConfig;
    private ReadHandle lh;

    @BeforeMethod
    public void setup() {
        mockManagedLedger = mock(ManagedLedgerImpl.class);
        when(mockManagedLedger.getName()).thenReturn("testManagedLedger");
        managedLedgerConfig = new ManagedLedgerConfig();
        when(mockManagedLedger.getConfig()).thenReturn(managedLedgerConfig);
        when(mockManagedLedger.getMbean()).thenReturn(mock(ManagedLedgerMBeanImpl.class));
        // a same-thread executor, so the read completes before the assertions run
        when(mockManagedLedger.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
        when(mockManagedLedger.getOptionalLedgerInfo(1L)).thenReturn(Optional.empty());
        ManagedLedgerFactoryImpl mockFactory = mock(ManagedLedgerFactoryImpl.class);
        when(mockFactory.getMbean()).thenReturn(mock(ManagedLedgerFactoryMBeanImpl.class));
        when(mockManagedLedger.getFactory()).thenReturn(mockFactory);
        lh = mock(ReadHandle.class);
        when(lh.getId()).thenReturn(1L);
    }

    private static ByteBuf serializeMessage(String producerName) {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(7)
                .setPublishTime(123456789L);
        ByteBuf payload = Unpooled.copiedBuffer("payload", StandardCharsets.UTF_8);
        try {
            // serializeMetadataAndPayload copies the payload instead of taking ownership of it
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }

    @Test
    public void testReadDoesNotParseMessageMetadataWhenTheEntriesArentPulsarMessages() throws Exception {
        EntryCacheDisabled entryCache = new EntryCacheDisabled(mockManagedLedger);

        // the transaction log and the pending ack store keep entries that are not Pulsar messages, so the read
        // must not try to parse message metadata out of them
        managedLedgerConfig.setPulsarMessageEntries(false);
        Entry entryWithoutParsing = readSingleEntry(entryCache, 0L);
        assertThat(entryWithoutParsing.getMessageMetadata()).isNull();
        entryWithoutParsing.release();

        // control: the same bytes read over the same path do get parsed when the entries are Pulsar messages, so
        // the assertion above can't pass merely because the payload happens to be unparseable
        managedLedgerConfig.setPulsarMessageEntries(true);
        Entry entryWithParsing = readSingleEntry(entryCache, 1L);
        assertThat(entryWithParsing.getMessageMetadata()).isNotNull();
        assertThat(entryWithParsing.getMessageMetadata().getProducerName()).isEqualTo("producer");
        entryWithParsing.release();
    }

    /**
     * Reads a single freshly serialized Pulsar message back through the cache.
     *
     * @apiNote the returned entry must be released by the caller
     */
    private Entry readSingleEntry(EntryCacheDisabled entryCache, long entryId) throws Exception {
        ByteBuf headersAndPayload = serializeMessage("producer");
        LedgerEntryImpl ledgerEntry =
                LedgerEntryImpl.create(1L, entryId, headersAndPayload.readableBytes(), headersAndPayload);
        LedgerEntries ledgerEntries = mock(LedgerEntries.class);
        when(ledgerEntries.iterator()).thenReturn(List.<LedgerEntry>of(ledgerEntry).iterator());
        when(lh.readAsync(entryId, entryId)).thenReturn(CompletableFuture.completedFuture(ledgerEntries));

        CompletableFuture<Entry> future = new CompletableFuture<>();
        entryCache.asyncReadEntry(lh, PositionFactory.create(1L, entryId), new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        Entry entry = future.get(10, TimeUnit.SECONDS);
        // the LedgerEntries mock doesn't release what it holds, so that reference is dropped explicitly instead
        ledgerEntry.close();
        return entry;
    }
}
