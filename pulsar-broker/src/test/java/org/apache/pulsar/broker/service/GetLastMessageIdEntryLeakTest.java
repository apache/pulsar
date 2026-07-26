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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class GetLastMessageIdEntryLeakTest extends ProducerConsumerBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Reproduces the ByteBuf leak in ServerCnx#getLargestBatchIndexWhenPossible: when
     * Commands.parseMessageMetadata throws on the entry read for the last position, the entry (and
     * its backing ByteBuf) must still be released.
     */
    @Test
    public void testEntryReleasedWhenParseMetadataThrows() throws Exception {
        final String topic = newTopicName();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producer.send("payload".getBytes());

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic).subscriptionName("sub").subscribe();

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();

        // A 2-byte, non-magic buffer: parseMessageMetadata reads it but fails at readUnsignedInt
        // (needs 4 bytes), which is exactly the corrupt-entry case that triggers the leak.
        ByteBuf corruptBuf = Unpooled.buffer(2);
        corruptBuf.writeShort(0x0000);

        // Build the entry eagerly so its retain happens now; then drop our own ref so the entry
        // "owns" the only ref and we can observe whether the broker releases it.
        Position lastPosition = ((ManagedLedgerImpl) persistentTopic.getManagedLedger()).getLastConfirmedEntry();
        EntryImpl corruptEntry =
                EntryImpl.create(lastPosition.getLedgerId(), lastPosition.getEntryId(), corruptBuf);
        corruptBuf.release();
        assertEquals(corruptBuf.refCnt(), 1);

        // Spy the real ManagedLedgerImpl so asyncReadEntry hands back our corrupt entry, while
        // getLastPosition() etc. still delegate to the real ledger.
        ManagedLedgerImpl spyLedger = spy((ManagedLedgerImpl) persistentTopic.getManagedLedger());
        doAnswer(inv -> {
            ReadEntryCallback callback = inv.getArgument(1);
            callback.readEntryComplete(corruptEntry, inv.getArgument(2));
            return null;
        }).when(spyLedger).asyncReadEntry(any(Position.class), any(ReadEntryCallback.class), any());

        FieldUtils.writeField(persistentTopic, "ledger", spyLedger, true);

        // The broker fails the request (MetadataError) - that's expected; the point is the buffer.
        assertThrows(Exception.class, consumer::getLastMessageId);

        // Before the fix: parseMessageMetadata throws, entry.release() is skipped -> refCnt stays 1.
        // After the fix: release runs in finally -> refCnt drops to 0.
        assertEquals(corruptBuf.refCnt(), 0, "entry's ByteBuf leaked when parseMessageMetadata threw");
    }

}
