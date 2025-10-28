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
package org.apache.pulsar.broker.service.nonpersistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class NonPersistentStickyKeyDispatcherMultipleConsumersTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private NonPersistentTopic topicMock;
    private NonPersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;

    private NonPersistentStickyKeyDispatcherMultipleConsumers nonpersistentDispatcher;
    private StickyKeyConsumerSelector selector;

    final String topicName = "non-persistent://public/default/testTopic";

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(true).when(configMock).isSubscriptionKeySharedUseConsistentHashing();
        doReturn(1).when(configMock).getSubscriptionKeySharedConsistentHashingReplicaPoints();

        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        topicMock = mock(NonPersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();

        subscriptionMock = mock(NonPersistentSubscription.class);
        selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
        nonpersistentDispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(
            topicMock, subscriptionMock, selector);
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        nonpersistentDispatcher.close().get();
        Consumer consumer = mock(Consumer.class);
        nonpersistentDispatcher.addConsumer(consumer);
        verify(consumer, times(1)).disconnect();
        assertEquals(0, nonpersistentDispatcher.getConsumers().size());
        assertTrue(selector.getConsumerKeyHashRanges().isEmpty());
    }

    @Test(timeOut = 10000)
    public void testTotalAvailablePermitsWhenRemoveConsumer() throws Exception {
        final int permits = 10;
        // add 2 consumers to dispatcher
        Consumer consumerMock = mock(Consumer.class);
        when(consumerMock.getAvailablePermits()).thenReturn(permits);
        nonpersistentDispatcher.addConsumer(consumerMock);
        Consumer consumerMock2 = mock(Consumer.class);
        when(consumerMock2.getAvailablePermits()).thenReturn(permits);
        when(consumerMock2.isWritable()).thenReturn(true);
        nonpersistentDispatcher.addConsumer(consumerMock2);

        // add 10 flow permits to the dispatcher, so TOTAL_AVAILABLE_PERMITS_UPDATER will be 10
        nonpersistentDispatcher.consumerFlow(consumerMock2, permits);
        assertEquals(NonPersistentDispatcherMultipleConsumers
                        .TOTAL_AVAILABLE_PERMITS_UPDATER.get(nonpersistentDispatcher), 10);
        // add another 10 flow permits to the dispatcher, so TOTAL_AVAILABLE_PERMITS_UPDATER will be 20
        nonpersistentDispatcher.consumerFlow(consumerMock, permits);
        assertEquals(NonPersistentDispatcherMultipleConsumers.
                        TOTAL_AVAILABLE_PERMITS_UPDATER.get(nonpersistentDispatcher), 20);
        // remove one consumer, so the TOTAL_AVAILABLE_PERMITS_UPDATER of the dispatcher will be reduced to 10
        nonpersistentDispatcher.removeConsumer(consumerMock);
        assertEquals(NonPersistentDispatcherMultipleConsumers.
                TOTAL_AVAILABLE_PERMITS_UPDATER.get(nonpersistentDispatcher), 10);
    }

    @Test(timeOut = 10000)
    public void testSendMessage() throws BrokerServiceException {
        Consumer consumerMock = mock(Consumer.class);
        when(consumerMock.getAvailablePermits()).thenReturn(1000);
        when(consumerMock.isWritable()).thenReturn(true);
        nonpersistentDispatcher.addConsumer(consumerMock);

        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createMessage("message1", 1)));
        entries.add(EntryImpl.create(1, 2, createMessage("message2", 2)));
        doAnswer(invocationOnMock -> {
            ChannelPromise mockPromise = mock(ChannelPromise.class);
            List<Entry> receivedEntries = invocationOnMock.getArgument(0, List.class);
            for (int index = 1; index <= receivedEntries.size(); index++) {
                Entry entry = receivedEntries.get(index - 1);
                assertEquals(entry.getLedgerId(), 1);
                assertEquals(entry.getEntryId(), index);
                ByteBuf byteBuf = entry.getDataBuffer();
                MessageMetadata messageMetadata = Commands.parseMessageMetadata(byteBuf);
                assertEquals(byteBuf.toString(UTF_8), "message" + index);
            };
            return mockPromise;
        }).when(consumerMock).sendMessages(any(List.class), any(EntryBatchSizes.class), any(),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));
        try {
            nonpersistentDispatcher.sendMessages(entries);
        } catch (Exception e) {
            fail("Failed to sendMessages.", e);
        }
        verify(consumerMock, times(1)).sendMessages(any(List.class), any(EntryBatchSizes.class),
                eq(null), anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));
    }

    @Test(timeOut = 10000)
    public void testSendMessageRespectFlowControl() throws BrokerServiceException {
        Consumer consumerMock = mock(Consumer.class);
        nonpersistentDispatcher.addConsumer(consumerMock);

        List<Entry> entries = new ArrayList<>();
        entries.add(EntryImpl.create(1, 1, createMessage("message1", 1)));
        entries.add(EntryImpl.create(1, 2, createMessage("message2", 2)));
        doAnswer(invocationOnMock -> {
            ChannelPromise mockPromise = mock(ChannelPromise.class);
            List<Entry> receivedEntries = invocationOnMock.getArgument(0, List.class);
            for (int index = 1; index <= receivedEntries.size(); index++) {
                Entry entry = receivedEntries.get(index - 1);
                assertEquals(entry.getLedgerId(), 1);
                assertEquals(entry.getEntryId(), index);
                ByteBuf byteBuf = entry.getDataBuffer();
                MessageMetadata messageMetadata = Commands.parseMessageMetadata(byteBuf);
                assertEquals(byteBuf.toString(UTF_8), "message" + index);
            }
            return mockPromise;
        }).when(consumerMock).sendMessages(any(List.class), any(EntryBatchSizes.class), any(),
                anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));
        try {
            nonpersistentDispatcher.sendMessages(entries);
        } catch (Exception e) {
            fail("Failed to sendMessages.", e);
        }
        verify(consumerMock, times(0)).sendMessages(any(List.class), any(EntryBatchSizes.class),
                eq(null), anyInt(), anyLong(), anyLong(), any(RedeliveryTracker.class));
    }

    private ByteBuf createMessage(String message, int sequenceId) {
        return createMessage(message, sequenceId, "testKey");
    }

    private ByteBuf createMessage(String message, int sequenceId, String key) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKey(key)
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        return serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, Unpooled.copiedBuffer(message.getBytes(UTF_8)));
    }

}
