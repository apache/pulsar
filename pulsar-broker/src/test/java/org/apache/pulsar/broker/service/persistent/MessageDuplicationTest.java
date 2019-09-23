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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
public class MessageDuplicationTest {

    private final static int BROKER_DEDUPLICATION_ENTRIES_INTERVAL = 10;
    private final static int BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS = 10;
    private final static String REPLICATOR_PREFIX = "foo";

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
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        status = messageDeduplication.isDuplicate(publishContext2, byteBuf2);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // should expect unknown because highestSequencePersisted is empty
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.highestSequencedPersisted.put(producerName1, 0L);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // now that highestSequencedPersisted, message with seqId of zero can be classified as a dup
        assertEquals(status, MessageDeduplication.MessageDupStatus.Dup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
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
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        status = messageDeduplication.isDuplicate(publishContext2, byteBuf2);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // should expect unknown because highestSequencePersisted is empty
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.highestSequencedPersisted.put(producerName1, 0L);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        // now that highestSequencedPersisted, message with seqId of zero can be classified as a dup
        assertEquals(status, MessageDeduplication.MessageDupStatus.Dup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertTrue(lastSequenceIdPushed != null);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
    }


    public ByteBuf getMessage(String producerName, long seqId) {
        PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                .setProducerName(producerName).setSequenceId(seqId)
                .setPublishTime(System.currentTimeMillis()).build();

        ByteBuf byteBuf = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, messageMetadata, io.netty.buffer.Unpooled.copiedBuffer(new byte[0]));

        return byteBuf;
    }

    public Topic.PublishContext getPublishContext(String producerName, long seqId) {
        return new Topic.PublishContext() {
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
        };
    }
}
