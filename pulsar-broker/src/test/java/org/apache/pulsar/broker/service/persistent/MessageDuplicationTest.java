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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MessageDuplicationTest extends BrokerTestBase {

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
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class, pulsarService, persistentTopic, managedLedger);
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
    public void testInactiveProducerRemove() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(1);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class, pulsarService, topic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn(managedCursor).when(messageDeduplication).getManagedCursor();

        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);

        Field field = MessageDeduplication.class.getDeclaredField("inactiveProducers");
        field.setAccessible(true);
        Map<String, Long> inactiveProducers = (ConcurrentHashMap<String, Long>) field.get(messageDeduplication);

        String producerName1 = "test1";
        when(publishContext.getHighestSequenceId()).thenReturn(2L);
        when(publishContext.getSequenceId()).thenReturn(1L);
        when(publishContext.getProducerName()).thenReturn(producerName1);
        messageDeduplication.isDuplicate(publishContext, null);

        String producerName2 = "test2";
        when(publishContext.getProducerName()).thenReturn(producerName2);
        messageDeduplication.isDuplicate(publishContext, null);

        String producerName3 = "test3";
        when(publishContext.getProducerName()).thenReturn(producerName3);
        messageDeduplication.isDuplicate(publishContext, null);

        // All 3 are added to the inactiveProducers list
        messageDeduplication.producerRemoved(producerName1);
        messageDeduplication.producerRemoved(producerName2);
        messageDeduplication.producerRemoved(producerName3);

        // Try first purgeInactive, all producer not inactive.
        messageDeduplication.purgeInactiveProducers();
        assertEquals(inactiveProducers.size(), 3);

        doReturn(false).when(messageDeduplication).isEnabled();
        inactiveProducers.put(producerName2, System.currentTimeMillis() - 80000);
        inactiveProducers.put(producerName3, System.currentTimeMillis() - 80000);
        messageDeduplication.purgeInactiveProducers();
        assertFalse(inactiveProducers.containsKey(producerName2));
        assertFalse(inactiveProducers.containsKey(producerName3));
        doReturn(true).when(messageDeduplication).isEnabled();
        // Modify the inactive time of produce2 and produce3
        // messageDeduplication.purgeInactiveProducers() will remove producer2 and producer3
        inactiveProducers.put(producerName2, System.currentTimeMillis() - 70000);
        inactiveProducers.put(producerName3, System.currentTimeMillis() - 70000);
        // Try second purgeInactive, produce2 and produce3 is inactive.
        messageDeduplication.purgeInactiveProducers();
        assertFalse(inactiveProducers.containsKey(producerName2));
        assertFalse(inactiveProducers.containsKey(producerName3));
        final var highestSequencedPushed = messageDeduplication.highestSequencedPushed;

        assertEquals((long) highestSequencedPushed.get(producerName1), 2L);
        assertFalse(highestSequencedPushed.containsKey(producerName2));
        assertFalse(highestSequencedPushed.containsKey(producerName3));
    }

    @Test
    public void testIsDuplicateWithFailure() {

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        doReturn(mock(PulsarResources.class)).when(pulsarService).getPulsarResources();
        doReturn(mock(CompactionServiceFactory.class)).when(pulsarService).getCompactionServiceFactory();

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
        doReturn(pulsarService).when(brokerService).getPulsar();
        doReturn(new BacklogQuotaManager(pulsarService)).when(brokerService).getBacklogQuotaManager();
        doReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK).when(pulsarService).getMonotonicSnapshotClock();

        PersistentTopic persistentTopic = spyWithClassAndConstructorArgs(PersistentTopic.class, "topic-1", brokerService, managedLedger, messageDeduplication);

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 1), null, publishContext1);
        verify(managedLedger, times(1)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        persistentTopic.publishMessage(byteBuf2, publishContext2);
        persistentTopic.addComplete(PositionFactory.create(0, 2), null, publishContext2);
        verify(managedLedger, times(2)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 3), null, publishContext1);
        verify(managedLedger, times(3)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 4), null, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
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
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));

        // publish message unknown dup status
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        // don't complete message
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
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
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1)).completed(any(MessageDeduplication.MessageDupUnknownException.class), eq(-1L), eq(-1L));

        // complete seq 6 message eventually
        persistentTopic.addComplete(PositionFactory.create(0, 5), null, publishContext1);

        // simulate failure
        byteBuf1 = getMessage(producerName1, 7);
        publishContext1 = getPublishContext(producerName1, 7);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());

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
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);

        // try new message
        byteBuf1 = getMessage(producerName1, 8);
        publishContext1 = getPublishContext(producerName1, 8);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(7)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        persistentTopic.addComplete(PositionFactory.create(0, 5), null, publishContext1);
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

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(true);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMessageDeduplication() throws Exception {
        String topicName = "persistent://prop/ns-abc/testMessageDeduplication";
        String producerName = "test-producer";
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .producerName(producerName)
                .topic(topicName)
                .create();
        final PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().orElse(null);
        assertNotNull(persistentTopic);
        final MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
        assertFalse(messageDeduplication.getInactiveProducers().containsKey(producerName));
        producer.close();
        Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.getInactiveProducers().containsKey(producerName)));
        admin.topicPolicies().setDeduplicationStatus(topicName, false);
        Awaitility.await().untilAsserted(() -> {
                    final Boolean deduplicationStatus = admin.topicPolicies().getDeduplicationStatus(topicName);
                    Assert.assertNotNull(deduplicationStatus);
                    Assert.assertFalse(deduplicationStatus);
                });
        messageDeduplication.purgeInactiveProducers();
        assertTrue(messageDeduplication.getInactiveProducers().isEmpty());
    }


    @Test
    public void testMessageDeduplicationShouldNotWorkForSystemTopic() throws PulsarAdminException {
        final String localName = UUID.randomUUID().toString();
        final String namespace = "prop/ns-abc";
        final String prefix = "persistent://%s/".formatted(namespace);
        final String topic = prefix + localName;
        admin.topics().createNonPartitionedTopic(topic);

        // broker level policies
        final String eventSystemTopic = prefix + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        final Optional<Topic> optionalTopic = pulsar.getBrokerService().getTopic(eventSystemTopic, true).join();
        assertTrue(optionalTopic.isPresent());
        final Topic ptRef = optionalTopic.get();
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());

        // namespace level policies
        admin.namespaces().setDeduplicationStatus(namespace, true);
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());

        // topic level policies
        admin.topicPolicies().setDeduplicationStatus(eventSystemTopic, true);
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());
    }
}
