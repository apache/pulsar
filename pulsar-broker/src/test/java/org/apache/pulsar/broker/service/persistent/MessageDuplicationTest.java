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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.markDelete;
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION;
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
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
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, persistentTopic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        MessageDeduplication.MessageDupStatus status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        byteBuf1.release();

        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        status = messageDeduplication.isDuplicate(publishContext2, byteBuf2);
        byteBuf2.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
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
        byteBuf1.release();
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
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
    }

    @Test
    public void testConcurrentDuplicateCheckForSameProducer() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, mock(PersistentTopic.class), mock(ManagedLedger.class));
        doReturn(true).when(messageDeduplication).isEnabled();

        int threads = 32;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ByteBuf message = getMessage("producer", 1);
        List<Future<MessageDeduplication.MessageDupStatus>> results = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                Topic.PublishContext context = getPublishContext("producer", 1);
                results.add(executor.submit(() -> {
                    start.await();
                    return messageDeduplication.isDuplicate(context, message);
                }));
            }
            start.countDown();

            int accepted = 0;
            int unknown = 0;
            for (Future<MessageDeduplication.MessageDupStatus> result : results) {
                switch (result.get()) {
                    case NotDup -> accepted++;
                    case Unknown -> unknown++;
                    default -> throw new AssertionError("A sequence cannot be known persisted before completion");
                }
            }
            assertEquals(accepted, 1);
            assertEquals(unknown, threads - 1);
            assertEquals(messageDeduplication.highestSequencedPushed.get("producer").longValue(), 1L);
        } finally {
            message.release();
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
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
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, managedLedger);
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
    public void testInactiveGeoReplicationProducerKeepsDedupState() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(1);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn(PositionFactory.create(0, 0)).when(managedCursor).getMarkDeletedPosition();
        doReturn(managedCursor).when(messageDeduplication).getManagedCursor();

        Map<String, Long> inactiveProducers = messageDeduplication.getInactiveProducers();

        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        // Geo V2 dedup state tracks the last source position, so it must outlive producer inactivity cleanup.
        MessageDeduplication.ReplSourcePosition replSourcePosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        messageDeduplication.highestReplPositionPushed.put(replicatorProducerName, replSourcePosition);
        messageDeduplication.highestReplPositionPersisted.put(replicatorProducerName, replSourcePosition);

        messageDeduplication.producerRemoved(replicatorProducerName);
        inactiveProducers.put(replicatorProducerName, System.currentTimeMillis() - 70000);

        messageDeduplication.purgeInactiveProducers();

        assertFalse(inactiveProducers.containsKey(replicatorProducerName));
        assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName), replSourcePosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(replicatorProducerName), replSourcePosition);
    }

    @Test
    public void testGeoReplicationV2DeduplicationStateTransitions() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        AtomicReference<long[]> sourcePosition = new AtomicReference<>(new long[]{7L, 9L});
        Topic.PublishContext publishContext = getReplPublishContext(replicatorProducerName, sourcePosition);

        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Unknown);

        messageDeduplication.recordMessagePersisted(publishContext, PositionFactory.create(1L, 1L));
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);

        sourcePosition.set(new long[]{7L, 8L});
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);

        sourcePosition.set(new long[]{8L, 0L});
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Unknown);

        // Simulate an add failure: pushed state must roll back to the last persisted source position.
        messageDeduplication.resetHighestSequenceIdPushed();
        MessageDeduplication.ReplSourcePosition persistedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName), persistedPosition);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
    }

    @Test
    public void testGeoReplicationV2RequiresValidSourcePosition() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(replicatorProducerName).when(publishContext).getProducerName();
        doReturn(true).when(publishContext).supportsReplDedupByLidAndEid();
        Map<String, Object> properties = new HashMap<>();
        doAnswer(invocation -> {
            properties.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(publishContext).setProperty(any(), any());
        doAnswer(invocation -> properties.get(invocation.getArgument(0)))
                .when(publishContext).getProperty(any());

        for (String sourcePosition : new String[]{null, "invalid", "-1:9", "7:-1", "7:", ":9", "7:9:10",
                "9223372036854775808:9"}) {
            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName("app-producer")
                    .setReplicatedFrom("c1")
                    .setSequenceId(3L)
                    .setPublishTime(System.currentTimeMillis());
            if (sourcePosition != null) {
                metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue(sourcePosition);
            }
            ByteBuf entry = serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
            try {
                assertEquals(messageDeduplication.isDuplicate(publishContext, entry),
                        MessageDeduplication.MessageDupStatus.Unknown);
            } finally {
                entry.release();
            }
        }

        assertTrue(messageDeduplication.highestReplPositionPushed.isEmpty());
        assertTrue(messageDeduplication.highestReplPositionPersisted.isEmpty());
        assertTrue(messageDeduplication.highestSequencedPushed.isEmpty());
        assertTrue(messageDeduplication.highestSequencedPersisted.isEmpty());

        // Prove that this context carries a successfully parsed position through the V2 duplicate check.
        MessageMetadata validMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(3L)
                .setPublishTime(System.currentTimeMillis());
        validMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        ByteBuf validEntry = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, validMetadata, Unpooled.EMPTY_BUFFER);
        try {
            assertEquals(messageDeduplication.isDuplicate(publishContext, validEntry),
                    MessageDeduplication.MessageDupStatus.NotDup);
            assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName),
                    new MessageDeduplication.ReplSourcePosition(7L, 9L));
        } finally {
            validEntry.release();
        }
    }

    @Test
    public void testGeoReplicationWatermarksAreIndependentByReplicator() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        AtomicReference<long[]> sourcePosition = new AtomicReference<>(new long[]{7L, 9L});
        String firstReplicator = REPLICATOR_PREFIX + ".c1-->c3";
        String secondReplicator = REPLICATOR_PREFIX + ".c2-->c3";
        Topic.PublishContext firstContext = getReplPublishContext(firstReplicator, sourcePosition);
        Topic.PublishContext secondContext = getReplPublishContext(secondReplicator, sourcePosition);

        assertEquals(messageDeduplication.isDuplicateReplV2(firstContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(secondContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        messageDeduplication.recordMessagePersisted(firstContext, PositionFactory.create(1L, 1L));
        messageDeduplication.recordMessagePersisted(secondContext, PositionFactory.create(1L, 2L));

        assertEquals(messageDeduplication.isDuplicateReplV2(firstContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);
        assertEquals(messageDeduplication.isDuplicateReplV2(secondContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);
        assertEquals(messageDeduplication.highestReplPositionPersisted.size(), 2);
    }

    @DataProvider
    public Object[][] snapshotProducerLimits() {
        return new Object[][]{{0, 2, 2}, {1, 1, 0}, {1, 0, 1}, {1, 1, 1}, {1, 2, 3}, {3, 4, 4}};
    }

    @Test(dataProvider = "snapshotProducerLimits")
    public void testSnapshotStoresCompleteReplicationWatermarkPairsWithinProducerLimit(
            int limit, int normalProducerCount, int replicatorCount) {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(1);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(limit);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        ManagedCursor managedCursor = mock(ManagedCursor.class);
        messageDeduplication.setManagedCursor(managedCursor);

        AtomicReference<Map<String, Long>> snapshotRef = new AtomicReference<>();
        Position position = PositionFactory.create(1, 1);
        doAnswer(invocation -> {
            Map<String, Long> snapshot = invocation.getArgument(1);
            snapshotRef.set(new HashMap<>(snapshot));
            MarkDeleteCallback callback = invocation.getArgument(2);
            callback.markDeleteComplete(null);
            return null;
        }).when(managedCursor).asyncMarkDelete(eq(position), any(), any(), any());

        for (int i = 0; i < normalProducerCount; i++) {
            Topic.PublishContext publishContext = getPublishContext("normal-producer-" + i, 5L);
            messageDeduplication.recordMessagePersistedNormal(publishContext, position);
        }
        for (int i = 0; i < replicatorCount; i++) {
            String producerName = REPLICATOR_PREFIX + ".source-" + i + "-->target";
            Topic.PublishContext publishContext = getReplPublishContext(producerName,
                    new AtomicReference<>(new long[]{7L + i, 9L}));
            messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        }

        Map<String, Long> snapshot = snapshotRef.get();
        assertNotNull(snapshot);
        int normalEntries = 0;
        for (int i = 0; i < normalProducerCount; i++) {
            String producerName = "normal-producer-" + i;
            if (snapshot.containsKey(producerName)) {
                assertEquals(snapshot.get(producerName), Long.valueOf(5L));
                normalEntries++;
            }
        }
        int replicationPairs = 0;
        for (int i = 0; i < replicatorCount; i++) {
            String producerName = REPLICATOR_PREFIX + ".source-" + i + "-->target";
            assertEquals(snapshot.containsKey(producerName + "_LID"), snapshot.containsKey(producerName + "_EID"));
            if (snapshot.containsKey(producerName + "_LID")) {
                assertEquals(snapshot.get(producerName + "_LID"), Long.valueOf(7L + i));
                assertEquals(snapshot.get(producerName + "_EID"), Long.valueOf(9L));
                replicationPairs++;
            }
        }
        assertEquals(normalEntries, Math.min(limit, normalProducerCount));
        assertEquals(replicationPairs, Math.min(limit, replicatorCount));
        assertEquals(snapshot.size(), normalEntries + 2 * replicationPairs);
    }

    @Test
    public void testSnapshotRetainsLegacyKeysWithoutOverwritingReplicationWatermark() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(1);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(2);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        ManagedCursor managedCursor = mock(ManagedCursor.class);
        messageDeduplication.setManagedCursor(managedCursor);

        AtomicReference<Map<String, Long>> snapshotRef = new AtomicReference<>();
        Position position = PositionFactory.create(1, 1);
        doAnswer(invocation -> {
            Map<String, Long> snapshot = invocation.getArgument(1);
            snapshotRef.set(new HashMap<>(snapshot));
            MarkDeleteCallback callback = invocation.getArgument(2);
            callback.markDeleteComplete(null);
            return null;
        }).when(managedCursor).asyncMarkDelete(eq(position), any(), any(), any());

        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        String legacyProducerName = REPLICATOR_PREFIX + "-application_LID";
        Map<String, Long> oldSnapshot = Map.of(
                "normal-producer", 5L,
                legacyProducerName, 11L,
                replicatorProducerName + "_LID", 7L,
                replicatorProducerName + "_EID", 9L);
        messageDeduplication.recoverDeduplicationStateFromSnapshot(oldSnapshot);

        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(replicatorProducerName).when(publishContext).getProducerName();
        doReturn(new long[]{8L, 0L}).when(publishContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);

        messageDeduplication.recordMessagePersistedRepl(publishContext, position);

        Map<String, Long> snapshot = snapshotRef.get();
        assertNotNull(snapshot);
        assertEquals(snapshot, Map.of(
                "normal-producer", 5L,
                legacyProducerName, 11L,
                replicatorProducerName + "_LID", 8L,
                replicatorProducerName + "_EID", 0L));
    }

    @Test
    public void testOldReplicationSnapshotAliasesDoNotConsumeNormalProducerLimit() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setBrokerDeduplicationEntriesInterval(1);
        configuration.setBrokerDeduplicationMaxNumberOfProducers(1);
        configuration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(configuration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication deduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        ManagedCursor cursor = mock(ManagedCursor.class);
        deduplication.setManagedCursor(cursor);
        Position position = PositionFactory.create(1, 1);
        AtomicReference<Map<String, Long>> snapshotRef = new AtomicReference<>();
        doAnswer(invocation -> {
            snapshotRef.set(new HashMap<>(invocation.getArgument(1)));
            MarkDeleteCallback callback = invocation.getArgument(2);
            callback.markDeleteComplete(null);
            return null;
        }).when(cursor).asyncMarkDelete(eq(position), any(), any(), any());

        String firstReplicator = REPLICATOR_PREFIX + ".c0-->c2";
        String secondReplicator = REPLICATOR_PREFIX + ".c1-->c2";
        // This snapshot was written before the broker's producer limit was reduced to one.
        deduplication.recoverDeduplicationStateFromSnapshot(Map.of(
                firstReplicator + "_LID", 7L, firstReplicator + "_EID", 9L,
                secondReplicator + "_LID", 8L, secondReplicator + "_EID", 0L));
        deduplication.recordMessagePersistedNormal(getPublishContext("ordinary", 11L, 11L), position);

        Map<String, Long> snapshot = snapshotRef.get();
        assertNotNull(snapshot);
        assertEquals(snapshot.get("ordinary"), Long.valueOf(11L),
                "Historical position aliases must not evict the only ordinary producer");
        assertEquals(snapshot.size(), 3);
        assertEquals(snapshot.containsKey(firstReplicator + "_LID"),
                snapshot.containsKey(firstReplicator + "_EID"));
        assertEquals(snapshot.containsKey(secondReplicator + "_LID"),
                snapshot.containsKey(secondReplicator + "_EID"));
    }

    @Test
    public void testRecoverOldSnapshotWithNormalAndReplicationState() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));

        String completeReplicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        String incompleteReplicatorProducerName = REPLICATOR_PREFIX + ".c3-->c2";
        String entryOnlyProducerName = REPLICATOR_PREFIX + ".c4-->c2";
        Map<String, Long> oldSnapshot = new HashMap<>();
        oldSnapshot.put("normal-producer", 5L);
        oldSnapshot.put(completeReplicatorProducerName + "_LID", 7L);
        oldSnapshot.put(completeReplicatorProducerName + "_EID", 9L);
        oldSnapshot.put(incompleteReplicatorProducerName + "_LID", 11L);
        oldSnapshot.put(entryOnlyProducerName + "_EID", 12L);

        messageDeduplication.recoverDeduplicationStateFromSnapshot(oldSnapshot);

        assertEquals(messageDeduplication.highestSequencedPushed, oldSnapshot);
        assertEquals(messageDeduplication.highestSequencedPersisted, oldSnapshot);
        MessageDeduplication.ReplSourcePosition expectedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(completeReplicatorProducerName),
                expectedPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(completeReplicatorProducerName),
                expectedPosition);
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(incompleteReplicatorProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(incompleteReplicatorProducerName));
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(entryOnlyProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(entryOnlyProducerName));
    }

    @Test
    public void testReplayRecoversGeoReplicationWatermarkAcrossLedgers() {
        String topicName = "persistent://prop/ns/tp";
        String clusterName = "c2";

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName(clusterName);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn(topicName).when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";

        MessageMetadata firstMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(1L)
                .setPublishTime(System.currentTimeMillis());
        firstMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        messageDeduplication.recoverReplWatermarkFromMetadata(firstMetadata);

        MessageMetadata nextMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(2L)
                .setPublishTime(System.currentTimeMillis());
        nextMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:10");
        messageDeduplication.recoverReplWatermarkFromMetadata(nextMetadata);

        MessageDeduplication.ReplSourcePosition nextPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 10L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), nextPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), nextPosition);
        assertFalse(messageDeduplication.highestSequencedPushed.containsKey(geoProducerName));
        assertFalse(messageDeduplication.highestSequencedPersisted.containsKey(geoProducerName));

        MessageMetadata laterMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(3L)
                .setPublishTime(System.currentTimeMillis());
        laterMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("8:0");
        messageDeduplication.recoverReplWatermarkFromMetadata(laterMetadata);

        MessageDeduplication.ReplSourcePosition laterPosition =
                new MessageDeduplication.ReplSourcePosition(8L, 0L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), laterPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), laterPosition);
    }

    @Test
    public void testPersistedGeoReplicationWatermarkAcrossLedgers() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(geoProducerName).when(publishContext).getProducerName();
        doReturn(new long[]{7L, 9L}, new long[]{7L, 10L}, new long[]{8L, 0L})
                .when(publishContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);
        Position position = PositionFactory.create(1L, 1L);

        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        MessageDeduplication.ReplSourcePosition nextPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 10L);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), nextPosition);

        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        MessageDeduplication.ReplSourcePosition laterPosition =
                new MessageDeduplication.ReplSourcePosition(8L, 0L);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), laterPosition);
    }

    @Test
    public void testPersistedWatermarksDoNotRegressWhenCallbacksCompleteOutOfOrder() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        Position laterPosition = PositionFactory.create(2L, 0L);
        Position earlierPosition = PositionFactory.create(1L, 9L);

        Topic.PublishContext normalContext = mock(Topic.PublishContext.class);
        doReturn("app-producer").when(normalContext).getProducerName();
        doReturn(10L, 9L).when(normalContext).getSequenceId();
        messageDeduplication.recordMessagePersistedNormal(normalContext, laterPosition);
        messageDeduplication.recordMessagePersistedNormal(normalContext, earlierPosition);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer"), Long.valueOf(10L));

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext replicationContext = mock(Topic.PublishContext.class);
        doReturn(geoProducerName).when(replicationContext).getProducerName();
        doReturn(new long[]{8L, 0L}, new long[]{7L, 99L})
                .when(replicationContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);
        messageDeduplication.recordMessagePersistedRepl(replicationContext, laterPosition);
        messageDeduplication.recordMessagePersistedRepl(replicationContext, earlierPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName),
                new MessageDeduplication.ReplSourcePosition(8L, 0L));
    }

    @Test
    public void testReplayRecoversMixedV1V2AndInvalidReplicationEntries() {
        String topicName = "persistent://prop/ns/tp";
        String clusterName = "c2";

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName(clusterName);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn(topicName).when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));

        MessageMetadata v1Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(7L)
                .setHighestSequenceId(9L)
                .setPublishTime(System.currentTimeMillis());
        messageDeduplication.recoverDeduplicationStateFromMetadata(v1Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").longValue(), 9L);

        MessageMetadata v2Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(10L)
                .setPublishTime(System.currentTimeMillis());
        v2Metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        messageDeduplication.recoverDeduplicationStateFromMetadata(v2Metadata);
        // Source-position properties also occur in V1 records, so preserve the original sequence state.
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").longValue(), 10L);

        // A producer name can be reused with lower sequence ids after its deduplication state is purged.
        MessageMetadata reusedV1Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(8L)
                .setPublishTime(System.currentTimeMillis());
        messageDeduplication.recoverDeduplicationStateFromMetadata(reusedV1Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").longValue(), 8L);

        MessageMetadata invalidV2Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(11L)
                .setPublishTime(System.currentTimeMillis());
        invalidV2Metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("invalid");
        messageDeduplication.recoverDeduplicationStateFromMetadata(invalidV2Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").longValue(), 11L);

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        MessageDeduplication.ReplSourcePosition expectedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), expectedPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), expectedPosition);
    }

    @Test
    public void testReplayRecoversSequenceIdAfterInactiveProducerNameIsReused() throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        int originalInactivityTimeout = configuration.getBrokerDeduplicationProducerInactivityTimeoutMinutes();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(0);
        try {
            String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String producerName = "reused-producer";
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.isEnabled()));
            Position snapshotPosition = messageDeduplication.getManagedCursor().getMarkDeletedPosition();

            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                producer.newMessage().sequenceId(100).value("previous incarnation").send();
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), 100L);
            }
            Awaitility.await().untilAsserted(() -> {
                messageDeduplication.purgeInactiveProducers();
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), -1L);
            });
            configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(originalInactivityTimeout);

            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                assertEquals(producer.getLastSequenceId(), -1L);
                producer.newMessage().sequenceId(0).value("new incarnation").send();
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), 0L);
            }
            // Both incarnations must be replayed, rather than loading their state from a newer snapshot.
            assertEquals(messageDeduplication.getManagedCursor().getMarkDeletedPosition(), snapshotPosition);

            admin.topics().unload(topicName);
            PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication recoveredDeduplication = reloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(recoveredDeduplication.isEnabled()));
            assertEquals(recoveredDeduplication.getLastPublishedSequenceId(producerName), 0L);

            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                assertEquals(producer.getLastSequenceId(), 0L);
                Position beforePublish = reloadedTopic.getManagedLedger().getLastConfirmedEntry();
                producer.newMessage().sequenceId(1).value("next message").send();
                Position afterPublish = reloadedTopic.getManagedLedger().getLastConfirmedEntry();
                assertTrue(afterPublish.compareTo(beforePublish) > 0);
                producer.newMessage().sequenceId(0).value("duplicate").send();
                assertEquals(reloadedTopic.getManagedLedger().getLastConfirmedEntry(), afterPublish);
            }
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
            configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(originalInactivityTimeout);
        }
    }

    @DataProvider
    public Object[][] replicationPositionSuffixes() {
        return new Object[][]{{"_LID"}, {"_EID"}};
    }

    @Test(dataProvider = "replicationPositionSuffixes")
    public void testSnapshotRetainsProducerNameEndingWithReplicationPositionSuffix(String suffix) throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(1);
        try (PulsarClient legacyClient = newLegacyDeduplicationClient()) {
            String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String producerName = configuration.getReplicatorPrefix() + "-application" + suffix;
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.isEnabled()));

            try (Producer<String> producer = legacyClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                        .sequenceId(7).value("message").send();
                Position messagePosition = PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId());
                Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.getManagedCursor()
                        .getMarkDeletedPosition().compareTo(messagePosition) >= 0));
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), 7L);
            }

            admin.topics().unload(topicName);
            PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication recoveredDeduplication = reloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(recoveredDeduplication.isEnabled()));
            assertEquals(recoveredDeduplication.getLastPublishedSequenceId(producerName), 7L);

            try (Producer<String> producer = legacyClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                assertEquals(producer.getLastSequenceId(), 7L);
                Position beforePublish = reloadedTopic.getManagedLedger().getLastConfirmedEntry();
                producer.newMessage().sequenceId(7).value("duplicate").send();
                assertEquals(reloadedTopic.getManagedLedger().getLastConfirmedEntry(), beforePublish);
            }
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
        }
    }

    @DataProvider
    public Object[][] legacyReplicationProducerNames() {
        return new Object[][]{{"-application"}, {"-application_LID"}, {"-application_EID"}};
    }

    @Test(dataProvider = "legacyReplicationProducerNames")
    public void testInactiveLegacyProducerWithReplicationPrefixCanReuseName(String nameSuffix) throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        int originalInactivityTimeout = configuration.getBrokerDeduplicationProducerInactivityTimeoutMinutes();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        try (PulsarClient legacyClient = newLegacyDeduplicationClient()) {
            String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String producerName = configuration.getReplicatorPrefix() + nameSuffix;
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication.isEnabled()));

            try (Producer<String> producer = legacyClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                producer.newMessage().sequenceId(100).value("previous incarnation").send();
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), 100L);
            }
            Awaitility.await().until(() -> !messageDeduplication.getInactiveProducers().isEmpty());
            assertTrue(messageDeduplication.getInactiveProducers().containsKey(producerName));
            long inactiveSince = messageDeduplication.getInactiveProducers().get(producerName);
            Awaitility.await().until(() -> System.currentTimeMillis() > inactiveSince);
            configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(0);
            messageDeduplication.purgeInactiveProducers();
            assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), -1L);
            configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(originalInactivityTimeout);

            try (Producer<String> producer = legacyClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                assertEquals(producer.getLastSequenceId(), -1L);
                Position beforePublish = persistentTopic.getManagedLedger().getLastConfirmedEntry();
                producer.newMessage().sequenceId(0).value("new incarnation").send();
                assertTrue(persistentTopic.getManagedLedger().getLastConfirmedEntry().compareTo(beforePublish) > 0);
                assertEquals(messageDeduplication.getLastPublishedSequenceId(producerName), 0L);
            }
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
            configuration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(originalInactivityTimeout);
        }
    }

    private PulsarClient newLegacyDeduplicationClient() throws Exception {
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        // Older clients do not advertise the V2 feature; these producer names use normal sequence IDs.
        return InjectedClientCnxClientBuilder.create(clientBuilder,
                (clientConfiguration, eventLoopGroup) -> new ClientCnx(
                        InstrumentProvider.NOOP, clientConfiguration, eventLoopGroup) {
                    @Override
                    protected ByteBuf buildConnectCommand(AuthData authData) throws Exception {
                        BaseCommand command = Commands.newConnectWithoutSerialize(authMethodName(), authData,
                                this.protocolVersion, clientVersion, proxyToTargetBrokerAddress,
                                null, null, null, null, null);
                        command.getConnect().getFeatureFlags().setSupportsReplDedupByLidAndEid(false);
                        return Commands.serializeWithSize(command);
                    }
                });
    }

    @DataProvider
    public Object[][] legacyV1SourcePositionProperty() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "legacyV1SourcePositionProperty")
    public void testReplayRecoversLegacyV1ReplicationSequenceIdAfterUnload(boolean includeSourcePosition)
            throws Exception {
        String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topicName);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).join().orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(persistentTopic.getMessageDeduplication().isEnabled()));

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(7L)
                .setHighestSequenceId(9L)
                .setPublishTime(System.currentTimeMillis());
        if (includeSourcePosition) {
            // Modern sources append this property even when an older target negotiates V1.
            metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        }
        ByteBuf entry = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
        try {
            persistentTopic.getManagedLedger().addEntry(ByteBufUtil.getBytes(entry));
        } finally {
            entry.release();
        }

        admin.topics().unload(topicName);
        PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).join().orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(reloadedTopic.getMessageDeduplication().isEnabled()));

        MessageDeduplication messageDeduplication = reloadedTopic.getMessageDeduplication();
        assertEquals(messageDeduplication.highestSequencedPushed.get("app-producer"), Long.valueOf(9L));
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer"), Long.valueOf(9L));
        if (!includeSourcePosition) {
            assertTrue(messageDeduplication.highestReplPositionPushed.isEmpty());
            assertTrue(messageDeduplication.highestReplPositionPersisted.isEmpty());
        }

        Topic.PublishContext replayContext = mock(Topic.PublishContext.class);
        doReturn(false).when(replayContext).supportsReplDedupByLidAndEid();
        doReturn(pulsar.getConfiguration().getReplicatorPrefix() + ".c1-->c2")
                .when(replayContext).getProducerName();
        doReturn(7L).when(replayContext).getSequenceId();
        doReturn(9L).when(replayContext).getHighestSequenceId();
        doReturn("app-producer").when(replayContext).getOriginalProducerName();
        doReturn(7L).when(replayContext).getOriginalSequenceId();
        doReturn(9L).when(replayContext).getOriginalHighestSequenceId();
        ByteBuf replayedEntry = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
        try {
            assertEquals(messageDeduplication.isDuplicate(replayContext, replayedEntry),
                    MessageDeduplication.MessageDupStatus.Dup);
        } finally {
            replayedEntry.release();
        }
    }

    @Test
    public void testLegacyV1SequenceSurvivesReplaySnapshotAndSecondReloadWithLimitOne() throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        int originalMaxProducers = configuration.getBrokerDeduplicationMaxNumberOfProducers();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        configuration.setBrokerDeduplicationMaxNumberOfProducers(1);
        try {
            String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String producerName = "app-producer";
            String replicatorProducerName = configuration.getReplicatorPrefix() + ".c1-->"
                    + configuration.getClusterName();
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic initialTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication initialDeduplication = initialTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(initialDeduplication.isEnabled()));

            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName(producerName)
                    .setReplicatedFrom("c1")
                    .setSequenceId(7L)
                    .setHighestSequenceId(9L)
                    .setPublishTime(System.currentTimeMillis());
            metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
            Position persistedPosition = publishLegacyReplicatedMessage(initialTopic, replicatorProducerName, metadata);
            assertEquals(initialDeduplication.getLastPublishedSequenceId(producerName), 9L);
            assertTrue(initialDeduplication.highestReplPositionPersisted.isEmpty());
            assertTrue(initialDeduplication.getManagedCursor().getMarkDeletedPosition()
                    .compareTo(persistedPosition) < 0);

            // The new deduplication instance snapshots the replayed V1 entry, including its possible V2 position.
            configuration.setBrokerDeduplicationEntriesInterval(1);
            admin.topics().unload(topicName);
            PersistentTopic firstReloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication firstRecoveredDeduplication = firstReloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(firstRecoveredDeduplication.isEnabled()));
            assertTrue(firstRecoveredDeduplication.getManagedCursor().getMarkDeletedPosition()
                    .compareTo(persistedPosition) >= 0);
            assertEquals(firstRecoveredDeduplication.getManagedCursor().getProperties(), Map.of(
                    producerName, 9L,
                    replicatorProducerName + "_LID", 7L,
                    replicatorProducerName + "_EID", 9L));

            // Recovery must now use the snapshot, because the deduplication cursor has passed the original entry.
            admin.topics().unload(topicName);
            PersistentTopic secondReloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).join().orElseThrow();
            MessageDeduplication secondRecoveredDeduplication = secondReloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(secondRecoveredDeduplication.isEnabled()));
            assertEquals(secondRecoveredDeduplication.getLastPublishedSequenceId(producerName), 9L);
            assertEquals(secondRecoveredDeduplication.highestReplPositionPersisted.get(replicatorProducerName),
                    new MessageDeduplication.ReplSourcePosition(7L, 9L));

            Position beforeRetry = secondReloadedTopic.getManagedLedger().getLastConfirmedEntry();
            Position retryPosition = publishLegacyReplicatedMessage(secondReloadedTopic, replicatorProducerName,
                    metadata);
            assertEquals(retryPosition, PositionFactory.create(-1L, -1L));
            assertEquals(secondReloadedTopic.getManagedLedger().getLastConfirmedEntry(), beforeRetry);
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
            configuration.setBrokerDeduplicationMaxNumberOfProducers(originalMaxProducers);
        }
    }

    @Test
    public void testSnapshotPersistenceFailureCanRetryAndRecoverBothStateTypes() throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        int originalMaxProducers = configuration.getBrokerDeduplicationMaxNumberOfProducers();
        double originalMarkDeleteRateLimit = configuration.getManagedLedgerDefaultMarkDeleteRateLimit();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(1);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        configuration.setBrokerDeduplicationMaxNumberOfProducers(1);
        configuration.setManagedLedgerDefaultMarkDeleteRateLimit(0);
        try {
            String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String producerName = "app-producer";
            String replicatorProducerName = configuration.getReplicatorPrefix() + ".c1-->"
                    + configuration.getClusterName();
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageDeduplication deduplication = topic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(deduplication.isEnabled()));
            ManagedCursor cursor = deduplication.getManagedCursor();
            MessageMetadata metadata = new MessageMetadata().setProducerName(producerName).setSequenceId(0)
                    .setPublishTime(System.currentTimeMillis());
            Position initialPosition = publishMessage(topic, producerName, metadata, false);
            Awaitility.await().untilAsserted(() -> {
                deduplication.takeSnapshot();
                assertEquals(cursor.getPersistentMarkDeletedPosition(), initialPosition);
            });
            assertTrue(cursor.getStats().getPersistLedgerSucceed() > 0);

            metadata.setSequenceId(9);
            publishMessage(topic, producerName, metadata, false);
            MessageMetadata replicatedMetadata = new MessageMetadata().setProducerName("remote-producer")
                    .setReplicatedFrom("c1").setSequenceId(5).setPublishTime(System.currentTimeMillis());
            replicatedMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
            Position failedSnapshotPosition = publishMessage(topic, replicatorProducerName, replicatedMetadata, true);
            long ledgerErrors = cursor.getStats().getPersistLedgerErrors();
            long metadataErrors = cursor.getStats().getPersistZookeeperErrors();
            // A cursor first falls back to the metadata store after a BookKeeper write failure.
            // Fail that fallback too, so the snapshot failure reaches MessageDeduplication.
            String cursorPath = "/managed-ledgers/" + topic.getManagedLedger().getName() + "/" + cursor.getName();
            mockZooKeeper.failConditional(KeeperException.Code.SYSTEMERROR,
                    (operation, path) -> operation == MockZooKeeper.Op.SET && path.equals(cursorPath));
            // Close only this cursor's ledger so unrelated background writes cannot consume the fault.
            pulsarTestContext.getMockBookKeeper().getLedgerMap()
                    .get(((ManagedCursorImpl) cursor).getCursorLedger()).close();
            Awaitility.await().untilAsserted(() -> {
                deduplication.takeSnapshot();
                assertEquals(cursor.getStats().getPersistLedgerErrors(), ledgerErrors + 1);
                assertEquals(cursor.getStats().getPersistZookeeperErrors(), metadataErrors + 1);
            });
            assertEquals(cursor.getMarkDeletedPosition(), failedSnapshotPosition);
            assertEquals(cursor.getPersistentMarkDeletedPosition(), initialPosition,
                    "The failed snapshot must not have reached durable storage");

            // New data must be able to trigger a fresh snapshot after the one-shot storage faults clear.
            metadata.setSequenceId(10);
            publishMessage(topic, producerName, metadata, false);
            replicatedMetadata.setSequenceId(6);
            replicatedMetadata.getPropertyAt(0).setValue("8:0");
            Position retrySnapshotPosition = publishMessage(topic, replicatorProducerName, replicatedMetadata, true);
            Map<String, Long> expectedSnapshot = Map.of(producerName, 10L,
                    replicatorProducerName + "_LID", 8L, replicatorProducerName + "_EID", 0L);
            Awaitility.await().untilAsserted(() -> {
                deduplication.takeSnapshot();
                assertEquals(cursor.getPersistentMarkDeletedPosition(), retrySnapshotPosition);
                assertEquals(cursor.getProperties(), expectedSnapshot);
            });

            // Require persistence before unloading: closing a dirty cursor can itself repair a failed write.
            admin.topics().unload(topicName);
            PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(topicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageDeduplication recovered = reloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(recovered.isEnabled()));
            assertEquals(recovered.getManagedCursor().getProperties(), expectedSnapshot);
            assertFalse(recovered.getManagedCursor().hasMoreEntries());
            assertEquals(recovered.getLastPublishedSequenceId(producerName), 10L);
            assertEquals(recovered.highestReplPositionPushed.get(replicatorProducerName),
                    new MessageDeduplication.ReplSourcePosition(8, 0));
            assertEquals(recovered.highestReplPositionPersisted.get(replicatorProducerName),
                    new MessageDeduplication.ReplSourcePosition(8, 0));
            Position beforeRetry = reloadedTopic.getManagedLedger().getLastConfirmedEntry();
            assertEquals(publishMessage(reloadedTopic, producerName, metadata, false),
                    PositionFactory.create(-1L, -1L));
            assertEquals(publishMessage(reloadedTopic, replicatorProducerName, replicatedMetadata, true),
                    PositionFactory.create(-1L, -1L));
            assertEquals(reloadedTopic.getManagedLedger().getLastConfirmedEntry(), beforeRetry);
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
            configuration.setBrokerDeduplicationMaxNumberOfProducers(originalMaxProducers);
            configuration.setManagedLedgerDefaultMarkDeleteRateLimit(originalMarkDeleteRateLimit);
        }
    }

    @DataProvider
    public Object[][] legacyShadowSnapshotWatermarks() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "legacyShadowSnapshotWatermarks")
    public void testShadowRecoversLegacySnapshotWithoutReplay(boolean hasWrongWatermark) throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        try {
            String sourceTopicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
            String shadowTopicName = sourceTopicName + "-shadow";
            String producerName = "original-producer";
            String shadowProducerName = ShadowReplicator.getShadowProducerName(configuration.getReplicatorPrefix(),
                    sourceTopicName, shadowTopicName);
            admin.topics().createNonPartitionedTopic(sourceTopicName);
            admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
            admin.topics().setShadowTopics(sourceTopicName, List.of(shadowTopicName));
            PersistentTopic sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(sourceTopicName, false).join().orElseThrow();
            PersistentTopic shadowTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(shadowTopicName, false).join().orElseThrow();
            MessageDeduplication initialDeduplication = shadowTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> {
                assertTrue(initialDeduplication.isEnabled());
                assertNotNull(sourceTopic.getShadowReplicators().get(shadowTopicName));
                ShadowReplicator replicator =
                        (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
                assertEquals(replicator.getState().toString(), "Started");
            });

            Position sourcePosition;
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopicName)
                    .producerName(producerName).enableBatching(false).create()) {
                MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                        .sequenceId(7).value("persisted source message").send();
                sourcePosition = PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId());
            }
            MessageDeduplication.ReplSourcePosition expectedPosition = new MessageDeduplication.ReplSourcePosition(
                    sourcePosition.getLedgerId(), sourcePosition.getEntryId());
            Awaitility.await().untilAsserted(() -> assertEquals(
                    initialDeduplication.highestReplPositionPersisted.get(shadowProducerName), expectedPosition));
            // Stop live replication so the historical snapshot remains the only recovery input.
            admin.topics().removeShadowTopics(sourceTopicName);
            sourceTopic.removeShadowReplicator(shadowTopicName).get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertTrue(sourceTopic.getShadowReplicators().isEmpty()));

            Map<String, Long> legacySnapshot = new HashMap<>();
            legacySnapshot.put(producerName, 7L);
            if (hasWrongWatermark) {
                // Old recovery could mistake a Geo source position for this Shadow source's physical position.
                // Use a larger value to catch recovery that incorrectly keeps the maximum of the two.
                legacySnapshot.put(shadowProducerName + "_LID", sourcePosition.getLedgerId() + 100L);
                legacySnapshot.put(shadowProducerName + "_EID", sourcePosition.getEntryId() + 100L);
            }
            ManagedCursor cursor = initialDeduplication.getManagedCursor();
            assertEquals(shadowTopic.getManagedLedger().getLastConfirmedEntry(), sourcePosition);
            assertTrue(cursor.getMarkDeletedPosition().compareTo(sourcePosition) < 0);
            markDelete(cursor, sourcePosition, legacySnapshot).get(10, TimeUnit.SECONDS);
            assertEquals(cursor.getProperties(), legacySnapshot);

            admin.topics().unload(shadowTopicName);
            PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(shadowTopicName, false).join().orElseThrow();
            MessageDeduplication recoveredDeduplication = reloadedTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(recoveredDeduplication.isEnabled()));
            assertEquals(recoveredDeduplication.getManagedCursor().getMarkDeletedPosition(), sourcePosition);
            assertEquals(reloadedTopic.getManagedLedger().getLastConfirmedEntry(), sourcePosition);
            assertFalse(recoveredDeduplication.getManagedCursor().hasMoreEntries(),
                    "Recovery must use the snapshot boundary because no source entries remain to replay");
            assertEquals(recoveredDeduplication.highestReplPositionPushed,
                    Map.of(shadowProducerName, expectedPosition));
            assertEquals(recoveredDeduplication.highestReplPositionPersisted,
                    Map.of(shadowProducerName, expectedPosition));
            assertEquals(recoveredDeduplication.getLastPublishedSequenceId(producerName), 7L);

            MessageMetadata retriedMetadata = new MessageMetadata().setProducerName(producerName)
                    .setSequenceId(7L).setPublishTime(System.currentTimeMillis());
            Position retryPosition = publishLegacyReplicatedMessage(reloadedTopic, shadowProducerName, retriedMetadata);
            assertEquals(retryPosition, PositionFactory.create(-1L, -1L));
            assertEquals(reloadedTopic.getManagedLedger().getLastConfirmedEntry(), sourcePosition);
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
        }
    }

    @Test
    public void testEmptyShadowDoesNotRecoverInvalidSourcePosition() throws Exception {
        String sourceTopicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        String shadowTopicName = sourceTopicName + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        PersistentTopic shadowTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(shadowTopicName, false).join().orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(shadowTopic.getMessageDeduplication().isEnabled()));
        assertTrue(shadowTopic.getManagedLedger().getLastConfirmedEntry().getEntryId() < 0);

        admin.topics().unload(shadowTopicName);
        PersistentTopic reloadedTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(shadowTopicName, false).join().orElseThrow();
        MessageDeduplication recoveredDeduplication = reloadedTopic.getMessageDeduplication();
        Awaitility.await().untilAsserted(() -> assertTrue(recoveredDeduplication.isEnabled()));
        assertFalse(recoveredDeduplication.getManagedCursor().hasMoreEntries());
        assertTrue(recoveredDeduplication.highestReplPositionPushed.isEmpty());
        assertTrue(recoveredDeduplication.highestReplPositionPersisted.isEmpty());
    }

    private Position publishLegacyReplicatedMessage(PersistentTopic topic, String replicatorProducerName,
                                                    MessageMetadata metadata) throws Exception {
        return publishMessage(topic, replicatorProducerName, metadata, false);
    }

    private Position publishMessage(PersistentTopic topic, String producerName, MessageMetadata metadata,
                                    boolean supportsReplDedupByLidAndEid) throws Exception {
        CompletableFuture<Position> completed = new CompletableFuture<>();
        ByteBuf entry = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
        try {
            topic.publishMessage(entry, new Topic.PublishContext() {
                private final Map<String, Object> properties = new HashMap<>();

                @Override
                public String getProducerName() {
                    return producerName;
                }

                @Override
                public long getSequenceId() {
                    return metadata.getSequenceId();
                }

                @Override
                public long getHighestSequenceId() {
                    return metadata.getHighestSequenceId();
                }

                @Override
                public String getOriginalProducerName() {
                    return metadata.getProducerName();
                }

                @Override
                public long getOriginalSequenceId() {
                    return metadata.getSequenceId();
                }

                @Override
                public long getOriginalHighestSequenceId() {
                    return metadata.getHighestSequenceId();
                }

                @Override
                public boolean supportsReplDedupByLidAndEid() {
                    return supportsReplDedupByLidAndEid;
                }

                @Override
                public Object getProperty(String key) {
                    return properties.get(key);
                }

                @Override
                public void setProperty(String key, Object value) {
                    properties.put(key, value);
                }

                @Override
                public void completed(Exception exception, long ledgerId, long entryId) {
                    if (exception == null) {
                        completed.complete(PositionFactory.create(ledgerId, entryId));
                    } else {
                        completed.completeExceptionally(exception);
                    }
                }
            });
            return completed.get(10, TimeUnit.SECONDS);
        } finally {
            entry.release();
        }
    }

    @Test
    public void testReplayRecoverGeoReplicationWatermarkSkipsMarkers() {
        String topicName = "persistent://prop/ns/tp";

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName("c2");
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn(topicName).when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(1L)
                .setMarkerType(MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE)
                .setPublishTime(System.currentTimeMillis());
        metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");

        messageDeduplication.recoverDeduplicationStateFromMetadata(metadata);

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(geoProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(geoProducerName));
        assertEquals(messageDeduplication.highestSequencedPushed.get("app-producer").longValue(), 1L);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").longValue(), 1L);
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
        MessageDeduplication messageDeduplication = spy(new MessageDeduplication(pulsarService,
                mock(PersistentTopic.class), managedLedger));
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
        doReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK).when(pulsarService).getMonotonicClock();

        PersistentTopic persistentTopic = spyWithClassAndConstructorArgs(PersistentTopic.class,
                "topic-1", brokerService, managedLedger, messageDeduplication);

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
        byteBuf2.release();

        lastSequenceIdPushed = messageDeduplication.highestSequencedPersisted.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        byteBuf1.release();

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
        byteBuf1.release();

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
        byteBuf1.release();

        // publish dup
        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        byteBuf1.release();

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
        byteBuf1.release();

        // publish same message again
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1))
                .completed(any(MessageDeduplication.MessageDupUnknownException.class), eq(-1L), eq(-1L));

        // complete seq 6 message eventually
        persistentTopic.addComplete(PositionFactory.create(0, 5), null, publishContext1);
        byteBuf1.release();

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
        byteBuf1.release();

        // try dup
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        byteBuf1.release();

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
        byteBuf1.release();

    }

    public ByteBuf getMessage(String producerName, long seqId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(seqId)
                .setPublishTime(System.currentTimeMillis());

        ByteBuf payload = Unpooled.copiedBuffer(new byte[0]);
        ByteBuf byteBuf = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, messageMetadata, payload);
        payload.release();
        return byteBuf;
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

    private MessageDeduplication newEnabledMessageDeduplication() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, mock(ManagedLedger.class));
        doReturn(true).when(messageDeduplication).isEnabled();
        return messageDeduplication;
    }

    private Topic.PublishContext getReplPublishContext(String producerName, AtomicReference<long[]> sourcePosition) {
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(producerName).when(publishContext).getProducerName();
        doReturn(true).when(publishContext).supportsReplDedupByLidAndEid();
        doAnswer(invocation -> MSG_PROP_REPL_SOURCE_POSITION.equals(invocation.getArgument(0))
                ? sourcePosition.get() : null).when(publishContext).getProperty(any());
        return publishContext;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(true);
        this.conf.setEnableShadowTopics(true);
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
        Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication
                .getInactiveProducers().containsKey(producerName)));
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
