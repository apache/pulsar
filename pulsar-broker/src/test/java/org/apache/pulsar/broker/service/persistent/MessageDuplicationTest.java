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
import io.netty.channel.EventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Slf4j
@Test(groups = "broker")
public class MessageDuplicationTest {

    private static final int BROKER_DEDUPLICATION_ENTRIES_INTERVAL = 10;
    private static final int BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS = 10;
    private static final String REPLICATOR_PREFIX = "foo";

    @Test
    public void testIsDuplicate() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        PersistentTopic persistentTopic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        MessageDeduplication messageDeduplication = spy(new MessageDeduplication(pulsarService, persistentTopic, managedLedger));
        doReturn(true).when(messageDeduplication).isEnabled();

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        MessageDeduplication.MessageDupStatus status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);

        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        status = messageDeduplication.isDuplicate(publishContext2, byteBuf2);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // should expect unknown because highestSequencePersisted is empty
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.highestSequencedPersisted.put(producerName1, 0L);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // now that highestSequencedPersisted, message with seqId of zero can be classified as a dup
        assertEquals(status, MessageDeduplication.MessageDupStatus.Dup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.highestSequencedPushed.put(producerName1, 0L);
        messageDeduplication.highestSequencedPersisted.put(producerName1, 0L);
        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        publishContext1 = getPublishContext(producerName1, 4, 8);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
    }

    @Test
    public void testIsDuplicateWithFailure() {

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        MessageDeduplication messageDeduplication = spy(new MessageDeduplication(pulsarService, mock(PersistentTopic.class), managedLedger));
        doReturn(true).when(messageDeduplication).isEnabled();


        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            Runnable test = (Runnable) args[0];
            test.run();
            return null;
        }).when(eventLoopGroup).submit(any(Runnable.class));

        BrokerService brokerService = mock(BrokerService.class);
        doReturn(eventLoopGroup).when(brokerService).executor();
        doReturn(pulsarService).when(brokerService).pulsar();

        PersistentTopic persistentTopic = spy(new PersistentTopic("topic-1", brokerService, managedLedger, messageDeduplication));

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(new PositionImpl(0, 1), null, publishContext1);
        verify(managedLedger, times(1)).asyncAddEntry(any(ByteBuf.class), any(), any());
        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        persistentTopic.publishMessage(byteBuf2, publishContext2);
        persistentTopic.addComplete(new PositionImpl(0, 2), null, publishContext2);
        verify(managedLedger, times(2)).asyncAddEntry(any(ByteBuf.class), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(new PositionImpl(0, 3), null, publishContext1);
        verify(managedLedger, times(3)).asyncAddEntry(any(ByteBuf.class), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(new PositionImpl(0, 4), null, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // publish dup
        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));

        // publish message unknown dup status
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        // don't complete message
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // publish same message again
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), any(), any());
        verify(publishContext1, times(1)).completed(any(MessageDeduplication.MessageDupUnknownException.class), eq(-1L), eq(-1L));

        // complete seq 6 message eventually
        persistentTopic.addComplete(new PositionImpl(0, 5), null, publishContext1);

        // simulate failure
        byteBuf1 = getMessage(producerName1, 7);
        publishContext1 = getPublishContext(producerName1, 7);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), any(), any());

        persistentTopic.addFailed(new ManagedLedgerException("test"), publishContext1);
        // check highestSequencedPushed is reset
        assertEquals(messageDeduplication.highestSequencedPushed.size(), 2);
        assertEquals(messageDeduplication.highestSequencedPersisted.size(), 2);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName2);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        verify(messageDeduplication, times(1)).resetHighestSequenceIdPushed();

        // try dup
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), any(), any());
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);

        // try new message
        byteBuf1 = getMessage(producerName1, 8);
        publishContext1 = getPublishContext(producerName1, 8);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(7)).asyncAddEntry(any(ByteBuf.class), any(), any());
        persistentTopic.addComplete(new PositionImpl(0, 5), null, publishContext1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 8);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 8);

    }

    public ByteBuf getMessage(String producerName, long seqId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(seqId)
                .setPublishTime(System.currentTimeMillis());

        return serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, messageMetadata, io.netty.buffer.Unpooled.copiedBuffer(new byte[0]));
    }

    public Topic.PublishContext getPublishContext(String producerName, long seqId) {
        return spy(new Topic.PublishContext() {
            @Override
            public String getProducerName() {
                return producerName;
            }

            public long getSequenceId() {
                return seqId;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {

            }
        });
    }

    public Topic.PublishContext getPublishContext(String producerName, long seqId, long lastSequenceId) {
        return spy(new Topic.PublishContext() {
            @Override
            public String getProducerName() {
                return producerName;
            }

            @Override
            public long getSequenceId() {
                return seqId;
            }

            @Override
            public long getHighestSequenceId() {
                return lastSequenceId;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {

            }
        });
    }
}
