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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.*;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

@Test(groups = "broker")
public class PersistentDispatcherMultipleConsumersTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedCursorImpl cursorMock;
    private Consumer consumerMock;
    private PersistentTopic topicMock;
    private PersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;

    private PersistentDispatcherMultipleConsumers persistentDispatcher;

    final String topicName = "persistent://public/default/testTopic";
    final String subscriptionName = "testSubscription";

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(100).when(configMock).getDispatcherMaxRoundRobinBatchSize();

        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();

        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(subscriptionName).when(cursorMock).getName();

        consumerMock = mock(Consumer.class);
        doReturn(1000).when(consumerMock).getAvailablePermits();
        doReturn(true).when(consumerMock).isWritable();

        subscriptionMock = mock(PersistentSubscription.class);

        persistentDispatcher = new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);
    }

    @Test
    public void testShouldRewindBeforeReadingOrReplayingForReplay() throws NoSuchFieldException, IllegalAccessException {
        List<Entry> readEntries = new ArrayList<>();
        readEntries.add(EntryImpl.create(1, 10, createMessage("message10", 10)));
        readEntries.add(EntryImpl.create(1, 11, createMessage("message11", 11)));
        readEntries.add(EntryImpl.create(1, 12, createMessage("message12", 12)));
        readEntries.add(EntryImpl.create(1, 13, createMessage("message13", 13)));
        readEntries.add(EntryImpl.create(1, 14, createMessage("message14", 14)));

        doAnswer(invocationOnMock -> {
            ((PersistentDispatcherMultipleConsumers) invocationOnMock.getArgument(2))
                    .readEntriesComplete(readEntries, PersistentDispatcherMultipleConsumers.ReadType.Normal);
            return null;
        }).doNothing().when(cursorMock).asyncReadEntriesOrWait(
                anyInt(), anyLong(), any(PersistentDispatcherMultipleConsumers.class),
                eq(PersistentDispatcherMultipleConsumers.ReadType.Normal), any());

        final Field havePendingReplayReadField =
                PersistentDispatcherMultipleConsumers.class.getDeclaredField("havePendingReplayRead");
        havePendingReplayReadField.setAccessible(true);
        havePendingReplayReadField.set(persistentDispatcher, true);

        try {
            // shouldRewindBeforeReadingOrReplaying changed to true.
            persistentDispatcher.addConsumer(consumerMock);
            persistentDispatcher.consumerFlow(consumerMock, 1000);
        } catch (Exception e) {
            fail("Failed to add mock consumer", e);
        }

        List<Entry> replayEntries = new ArrayList<>();
        replayEntries.add(EntryImpl.create(1, 10, createMessage("message10", 10)));

        try {
            // Replay message is ignored and cursor is rewound.
            // After that, normal message is read and sent to consumer.
            persistentDispatcher.readEntriesComplete(replayEntries, PersistentDispatcherMultipleConsumers.ReadType.Replay);
        } catch (Exception e) {
            fail("Failed to readEntriesComplete.", e);
        }

        ArgumentCaptor<List<Entry>> entriesCaptor = ArgumentCaptor.forClass(List.class);
        verify(consumerMock, times(1)).sendMessages(
                entriesCaptor.capture(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );

        Assert.assertEquals(entriesCaptor.getAllValues().get(0), readEntries);
    }

    private ByteBuf createMessage(String message, int sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }
}
