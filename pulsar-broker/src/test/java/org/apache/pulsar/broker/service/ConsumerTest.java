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

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.client.api.MessageId.latest;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerTest {
    private Consumer consumer;
    private Subscription subscription;
    private ServerCnx cnx;
    private final ConsumerStatsImpl stats = new ConsumerStatsImpl();

    @BeforeMethod
    public void beforeMethod() {
        subscription = mock(Subscription.class);
        cnx = mock(ServerCnx.class);
        SocketAddress address = mock(SocketAddress.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);

        when(cnx.clientAddress()).thenReturn(address);
        when(subscription.getName()).thenReturn("subscription");
        when(subscription.getTopic()).thenReturn(topic);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);
        when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);

        consumer =
                new Consumer(subscription, Exclusive, "topic", 1, 0, "Cons1", true, cnx, "myrole-1", emptyMap(), false,
                        new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), latest, DEFAULT_CONSUMER_EPOCH);
    }

    @Test
    public void testGetMsgOutCounter() {
        stats.msgOutCounter = 1L;
        consumer.updateStats(stats);
        assertEquals(consumer.getMsgOutCounter(), 1L);
    }

    @Test
    public void testGetBytesOutCounter() {
        stats.bytesOutCounter = 1L;
        consumer.updateStats(stats);
        assertEquals(consumer.getBytesOutCounter(), 1L);
    }

    @Test
    public void testSendMessagesFinalizesPermitsAfterPendingAckAdmission() {
        Consumer sharedConsumer = new Consumer(subscription, Shared, "topic", 2, 0, "shared-consumer", false, cnx,
                "myrole-1", emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), latest,
                DEFAULT_CONSUMER_EPOCH);
        sharedConsumer.setPendingAcksAddHandler((ignored, ledgerId, entryId, stickyKeyHash) -> ledgerId != 2);
        sharedConsumer.flowPermits(100);

        Entry partialBatch = mock(Entry.class);
        when(partialBatch.getLedgerId()).thenReturn(1L);
        when(partialBatch.getEntryId()).thenReturn(1L);
        Entry rejectedBatch = mock(Entry.class);
        when(rejectedBatch.getLedgerId()).thenReturn(2L);
        when(rejectedBatch.getEntryId()).thenReturn(2L);
        Entry emptyPartialBatch = mock(Entry.class);
        when(emptyPartialBatch.getLedgerId()).thenReturn(3L);
        when(emptyPartialBatch.getEntryId()).thenReturn(3L);
        List<Entry> entries = new ArrayList<>(List.of(partialBatch, rejectedBatch, emptyPartialBatch));
        // A null slot represents an entry removed by dispatcher filtering before final send admission.
        entries.add(null);

        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        batchSizes.setBatchSize(0, 10);
        batchSizes.setBatchSize(1, 10);
        batchSizes.setBatchSize(2, 10);
        EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entries.size());
        batchIndexesAcks.setIndexesAcks(0, Pair.of(10, new long[] {0b100101L}));
        batchIndexesAcks.setIndexesAcks(2, Pair.of(10, new long[] {0L}));

        PulsarCommandSender commandSender = mock(PulsarCommandSender.class);
        when(cnx.getCommandSender()).thenReturn(commandSender);
        when(commandSender.sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(), any(), any(), any(),
                any(), any(), anyLong()))
                .thenReturn(ImmediateEventExecutor.INSTANCE.newSucceededFuture(null));

        try {
            SendMessagesResult sendResult = sharedConsumer.sendMessagesWithResult(
                    entries, batchSizes, batchIndexesAcks, 23, 0, 0, mock(RedeliveryTracker.class));

            assertEquals(sendResult.getTotalMessagePermits(), 3);
            assertEquals(sendResult.getMessagePermits(0), 3);
            assertEquals(sendResult.getMessagePermits(1), 0);
            assertEquals(sendResult.getMessagePermits(2), 0);
            assertEquals(sendResult.getMessagePermits(3), 0);
            assertEquals(sharedConsumer.getAvailablePermits(), 97);
            assertEquals(sharedConsumer.getUnackedMessages(), 3);
            assertNull(entries.get(1));
            assertNull(entries.get(2));
            verify(rejectedBatch).release();
            verify(emptyPartialBatch).release();
            ArgumentCaptor<SendMessagesResult> sendResultCaptor = ArgumentCaptor.forClass(SendMessagesResult.class);
            verify(commandSender).sendMessagesToConsumer(eq(2L), eq("topic"), eq(subscription), anyInt(), eq(entries),
                    eq(batchSizes), eq(batchIndexesAcks), sendResultCaptor.capture(), any(),
                    eq(DEFAULT_CONSUMER_EPOCH));
            assertSame(sendResultCaptor.getValue(), sendResult);
            assertSame(entries.get(0), partialBatch);
        } finally {
            batchSizes.recyle();
            batchIndexesAcks.recycle();
        }
    }

    @Test
    public void testFinalizedPermitsOutliveRecycledSenderInputs() {
        Consumer sharedConsumer = new Consumer(subscription, Shared, "topic", 2, 0, "shared-consumer", false, cnx,
                "myrole-1", emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), latest,
                DEFAULT_CONSUMER_EPOCH);
        sharedConsumer.setPendingAcksAddHandler((ignored, ledgerId, entryId, stickyKeyHash) -> true);
        sharedConsumer.flowPermits(100);

        Entry partialBatch = mock(Entry.class);
        when(partialBatch.getLedgerId()).thenReturn(1L);
        when(partialBatch.getEntryId()).thenReturn(1L);
        Entry completeBatch = mock(Entry.class);
        when(completeBatch.getLedgerId()).thenReturn(2L);
        when(completeBatch.getEntryId()).thenReturn(2L);
        List<Entry> entries = new ArrayList<>(List.of(partialBatch, completeBatch));

        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        batchSizes.setBatchSize(0, 10);
        batchSizes.setBatchSize(1, 4);
        EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(entries.size());
        batchIndexesAcks.setIndexesAcks(0, Pair.of(10, new long[] {0b100101L}));

        PulsarCommandSender commandSender = mock(PulsarCommandSender.class);
        when(cnx.getCommandSender()).thenReturn(commandSender);
        Promise<Void> pendingWrite = ImmediateEventExecutor.INSTANCE.newPromise();
        when(commandSender.sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(), any(), any(), any(),
                any(), any(), anyLong())).thenReturn(pendingWrite);

        SendMessagesResult sendResult;
        try {
            sendResult = sharedConsumer.sendMessagesWithResult(
                    entries, batchSizes, batchIndexesAcks, 14, 0, 0, mock(RedeliveryTracker.class));
        } finally {
            // The command sender owns and recycles these inputs before its asynchronous write completes.
            batchSizes.recyle();
            batchIndexesAcks.recycle();
        }

        EntryBatchSizes reusedBatchSizes = EntryBatchSizes.get(entries.size());
        EntryBatchIndexesAcks reusedBatchIndexesAcks = EntryBatchIndexesAcks.get(entries.size());
        try {
            // Mutate the recycled carriers while the write is still pending. The finalized result must be independent.
            reusedBatchSizes.setBatchSize(0, 1);
            reusedBatchSizes.setBatchSize(1, 1);
            reusedBatchIndexesAcks.setIndexesAcks(0, Pair.of(1, new long[] {1L}));

            assertEquals(sendResult.getMessagePermits(0), 3);
            assertEquals(sendResult.getMessagePermits(1), 4);
            assertEquals(sendResult.getTotalMessagePermits(), 7);
        } finally {
            pendingWrite.trySuccess(null);
            reusedBatchSizes.recyle();
            reusedBatchIndexesAcks.recycle();
        }
    }

    @Test
    public void testSendMessagesWriteFailureDisconnectsConsumer() throws BrokerServiceException {
        Consumer sharedConsumer = new Consumer(subscription, Shared, "topic", 2, 0, "shared-consumer", false, cnx,
                "myrole-1", emptyMap(), false, new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), latest,
                DEFAULT_CONSUMER_EPOCH);
        sharedConsumer.setPendingAcksAddHandler((ignored, ledgerId, entryId, stickyKeyHash) -> true);
        sharedConsumer.flowPermits(100);

        Entry entry = mock(Entry.class);
        when(entry.getLedgerId()).thenReturn(1L);
        when(entry.getEntryId()).thenReturn(1L);
        List<Entry> entries = new ArrayList<>(List.of(entry));
        EntryBatchSizes batchSizes = EntryBatchSizes.get(1);
        batchSizes.setBatchSize(0, 1);
        PulsarCommandSender commandSender = mock(PulsarCommandSender.class);
        when(cnx.getCommandSender()).thenReturn(commandSender);
        when(commandSender.sendMessagesToConsumer(anyLong(), anyString(), any(), anyInt(), any(), any(), any(),
                any(), any(), anyLong()))
                .thenReturn(ImmediateEventExecutor.INSTANCE.newFailedFuture(new IOException("write failed")));

        try {
            sharedConsumer.sendMessagesWithResult(
                    entries, batchSizes, null, 1, 0, 0, mock(RedeliveryTracker.class));

            verify(cnx).closeConsumer(sharedConsumer, Optional.empty());
            verify(subscription).removeConsumer(sharedConsumer, false);
            verify(cnx).removedConsumer(sharedConsumer);
        } finally {
            batchSizes.recyle();
        }
    }

    @Test
    public void testSendMessagesResultRejectsPartialFinalization() {
        SendMessagesResult sendResult = new SendMessagesResult(2);
        sendResult.setMessagePermits(0, Integer.MAX_VALUE);

        expectThrows(ArithmeticException.class, () -> sendResult.setMessagePermits(1, 1));
        assertEquals(sendResult.getMessagePermits(0), Integer.MAX_VALUE);
        assertEquals(sendResult.getMessagePermits(1), 0);
        assertEquals(sendResult.getTotalMessagePermits(), Integer.MAX_VALUE);
        expectThrows(IllegalStateException.class, () -> sendResult.setMessagePermits(0, 1));
        expectThrows(IllegalArgumentException.class, () -> new SendMessagesResult(1).setMessagePermits(0, 0));
    }
}
