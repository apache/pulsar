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

import static org.apache.pulsar.broker.service.persistent.BrokerServicePersistInternalMethodInvoker.ensureNoBacklogByInflightTask;
import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class ShadowReplicatorTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        admin.tenants().createTenant("prop1",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop1/ns-source");
        admin.namespaces().createNamespace("prop1/ns-shadow");
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testShadowReplication() throws Exception {
        String sourceTopicName = "persistent://prop1/ns-source/source-topic";
        String shadowTopicName = "persistent://prop1/ns-shadow/shadow-topic";
        String shadowTopicName2 = "persistent://prop1/ns-shadow/shadow-topic-2";

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName2, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName, shadowTopicName2));

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName).create();

        @Cleanup
        Consumer<byte[]> shadowConsumer =
                pulsarClient.newConsumer().topic(shadowTopicName).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        @Cleanup
        Consumer<byte[]> shadowConsumer2 =
                pulsarClient.newConsumer().topic(shadowTopicName2).subscriptionName("shadow-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();

        Awaitility.await().untilAsserted(()->Assert.assertEquals(sourceTopic.getShadowReplicators().size(), 2));

        ShadowReplicator
                replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(String.valueOf(replicator.getState()), "Started"));

        @Cleanup
        Consumer<byte[]> sourceConsumer =
                pulsarClient.newConsumer().topic(sourceTopicName).subscriptionName("source-sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        byte[] data = "test-shadow-topic".getBytes(StandardCharsets.UTF_8);
        MessageId sourceMessageId = producer.newMessage()
                .sequenceId(1)
                .key("K")
                .property("PK", "PV")
                .eventTime(123)
                .value(data)
                .send();

        Message<byte[]> sourceMessage = sourceConsumer.receive();
        Assert.assertEquals(sourceMessage.getMessageId(), sourceMessageId);

        //Wait until msg is replicated to shadow topic.
        Awaitility.await().until(() -> {
            replicator.msgOut.calculateRate();
            return replicator.msgOut.getCount() >= 1;
        });
        ensureNoBacklogByInflightTask(replicator);

        PersistentTopic shadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicName).get().get();
        Assert.assertNotNull(shadowTopic);

        Message<byte[]> shadowMessage = shadowConsumer.receive(5, TimeUnit.SECONDS);

        Assert.assertEquals(shadowMessage.getData(), sourceMessage.getData());
        Assert.assertEquals(shadowMessage.getSequenceId(), sourceMessage.getSequenceId());
        Assert.assertEquals(shadowMessage.getEventTime(), sourceMessage.getEventTime());
        Assert.assertEquals(shadowMessage.getProperties(), sourceMessage.getProperties());
        Assert.assertEquals(shadowMessage.getKey(), sourceMessage.getKey());
        Assert.assertEquals(shadowMessage.getOrderingKey(), sourceMessage.getOrderingKey());
        Assert.assertEquals(shadowMessage.getSchemaVersion(), sourceMessage.getSchemaVersion());
        Assert.assertEquals(shadowMessage.getPublishTime(), sourceMessage.getPublishTime());
        Assert.assertEquals(shadowMessage.getBrokerPublishTime(), sourceMessage.getBrokerPublishTime());
        Assert.assertEquals(shadowMessage.getIndex(), sourceMessage.getIndex());

        Assert.assertEquals(replicator.stats.getBytesOutCount(), 0);

        Assert.assertEquals(shadowMessage.getMessageId(), sourceMessage.getMessageId());
    }

    @DataProvider
    public Object[][] sourcePositionProperties() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "sourcePositionProperties")
    public void testShadowDeduplicationRecoveryFromSourceLedger(boolean inheritedSourcePosition) throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        String namespace = BrokerTestUtil.newUniqueName("prop1/shadow-replay");
        String sourceTopicName = "persistent://" + namespace + "/source";
        String shadowTopicName = "persistent://" + namespace + "/shadow";
        String producerName = "ordinary-source-producer";
        String inheritedPosition = Long.MAX_VALUE + ":99";
        Map<String, String> sourceProperties = inheritedSourcePosition
                ? Map.of(MSG_PROP_REPL_SOURCE_POSITION, inheritedPosition) : Map.of();
        try {
            admin.namespaces().createNamespace(namespace);
            admin.namespaces().setDeduplicationStatus(namespace, true);
            admin.topics().createNonPartitionedTopic(sourceTopicName);
            admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
            PersistentTopic shadowTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(shadowTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageDeduplication deduplication = shadowTopic.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(deduplication.isEnabled()));
            Position initialSnapshotPosition = deduplication.getManagedCursor().getMarkDeletedPosition();
            admin.topics().setShadowTopics(sourceTopicName, List.of(shadowTopicName));

            MessageIdImpl olderMessageId;
            Position markerPosition;
            try (Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName)
                    .producerName(producerName).enableBatching(false).create()) {
                waitForStartedShadowReplicator(sourceTopicName, shadowTopicName);
                producer.newMessage().sequenceId(7).properties(sourceProperties).value(new byte[]{1}).send();
                PersistentTopic initialSourceTopic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(sourceTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
                markerPosition = publishSourceMarker(initialSourceTopic, configuration.getClusterName());
                olderMessageId = (MessageIdImpl) producer.newMessage().sequenceId(8).properties(sourceProperties)
                        .value(new byte[]{2}).send();
            }
            String shadowProducerName = ShadowReplicator.getShadowProducerName(configuration.getReplicatorPrefix(),
                    sourceTopicName, shadowTopicName);
            MessageDeduplication.ReplSourcePosition olderPosition = new MessageDeduplication.ReplSourcePosition(
                    olderMessageId.getLedgerId(), olderMessageId.getEntryId());
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                    deduplication.highestReplPositionPersisted.get(shadowProducerName), olderPosition));

            PersistentTopic sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(sourceTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageMetadata originalMetadata = readSourceMetadata(sourceTopic, olderMessageId);
            MessageIdImpl markerMessageId = new MessageIdImpl(markerPosition.getLedgerId(), markerPosition.getEntryId(),
                    -1);
            MessageMetadata markerMetadata = readSourceMetadata(sourceTopic, markerMessageId);
            Assert.assertEquals(markerMetadata.getMarkerType(),
                    MarkerType.REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST_VALUE);
            Assert.assertFalse(originalMetadata.hasReplicatedFrom(),
                    "Shadow replication must not rewrite metadata in the shared source ledger");
            Assert.assertEquals(originalMetadata.getPropertiesList().stream()
                            .filter(property -> property.getKey().equals(MSG_PROP_REPL_SOURCE_POSITION))
                            .map(property -> property.getValue()).toList(),
                    inheritedSourcePosition ? List.of(inheritedPosition) : List.of());

            // A new source ledger has a lower entry id; recovery must compare the whole position.
            admin.topics().unload(sourceTopicName);
            MessageIdImpl newestMessageId;
            waitForStartedShadowReplicator(sourceTopicName, shadowTopicName);
            PersistentTopic reloadedSourceTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(sourceTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            if (inheritedSourcePosition) {
                String geoProducerName = configuration.getReplicatorPrefix() + ".upstream-->"
                        + configuration.getClusterName();
                newestMessageId = publishGeoSourceMessage(reloadedSourceTopic, geoProducerName, producerName,
                        9, inheritedPosition, new byte[]{3});
            } else {
                try (Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName)
                        .producerName(producerName).enableBatching(false).create()) {
                    newestMessageId = (MessageIdImpl) producer.newMessage().sequenceId(9)
                            .value(new byte[]{3}).send();
                }
            }
            assertTrue(newestMessageId.getLedgerId() > olderMessageId.getLedgerId());
            assertTrue(newestMessageId.getEntryId() < olderMessageId.getEntryId());
            MessageDeduplication.ReplSourcePosition expectedPosition = new MessageDeduplication.ReplSourcePosition(
                    newestMessageId.getLedgerId(), newestMessageId.getEntryId());
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                    deduplication.highestReplPositionPersisted.get(shadowProducerName), expectedPosition));
            MessageMetadata newestSourceMetadata = readSourceMetadata(reloadedSourceTopic, newestMessageId);
            if (inheritedSourcePosition) {
                Assert.assertEquals(newestSourceMetadata.getReplicatedFrom(), "upstream");
                Assert.assertEquals(newestSourceMetadata.getPropertiesList().stream()
                                .filter(property -> property.getKey().equals(MSG_PROP_REPL_SOURCE_POSITION))
                                .map(property -> property.getValue()).toList(), List.of(inheritedPosition),
                        "The shared source ledger must retain its upstream coordinate");
            }
            Assert.assertEquals(deduplication.getManagedCursor().getMarkDeletedPosition(), initialSnapshotPosition);
            assertTrue(deduplication.getManagedCursor().getProperties().isEmpty());

            // Stop live replication so reconnects cannot repair missing replay state before assertions.
            admin.topics().removeShadowTopics(sourceTopicName);
            sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(sourceTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            sourceTopic.removeShadowReplicator(shadowTopicName).get(10, TimeUnit.SECONDS);
            PersistentTopic stoppedSourceTopic = sourceTopic;
            Awaitility.await().untilAsserted(() -> assertTrue(stoppedSourceTopic.getShadowReplicators().isEmpty()));

            // The first reload must replay the source ledger; snapshot the recovered state before the second reload.
            configuration.setBrokerDeduplicationEntriesInterval(1);
            admin.topics().unload(shadowTopicName);
            PersistentTopic firstReloadedShadow = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(shadowTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageDeduplication recovered = firstReloadedShadow.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(recovered.isEnabled()));
            Assert.assertEquals(recovered.highestReplPositionPushed.get(shadowProducerName), expectedPosition);
            Assert.assertEquals(recovered.highestReplPositionPersisted.get(shadowProducerName), expectedPosition,
                    "Shadow replay must recover the source entry position from the shared ledger");
            Position newestPosition = PositionFactory.create(newestMessageId.getLedgerId(),
                    newestMessageId.getEntryId());
            Assert.assertEquals(recovered.getManagedCursor().getMarkDeletedPosition(), newestPosition);
            Assert.assertEquals(recovered.getManagedCursor().getProperties().get(shadowProducerName + "_LID"),
                    Long.valueOf(newestMessageId.getLedgerId()));
            Assert.assertEquals(recovered.getManagedCursor().getProperties().get(shadowProducerName + "_EID"),
                    Long.valueOf(newestMessageId.getEntryId()));
            Assert.assertEquals(retryShadowMessage(firstReloadedShadow, shadowProducerName, olderMessageId,
                    originalMetadata, false), PositionFactory.create(-1L, -1L));
            if (inheritedSourcePosition) {
                Assert.assertEquals(retryShadowMessage(firstReloadedShadow, shadowProducerName, newestMessageId,
                        newestSourceMetadata, false), PositionFactory.create(-1L, -1L));
            }
            Assert.assertEquals(firstReloadedShadow.getManagedLedger().getLastConfirmedEntry(), newestPosition);

            admin.topics().unload(shadowTopicName);
            PersistentTopic secondReloadedShadow = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(shadowTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
            MessageDeduplication restored = secondReloadedShadow.getMessageDeduplication();
            Awaitility.await().untilAsserted(() -> assertTrue(restored.isEnabled()));
            Assert.assertEquals(restored.highestReplPositionPushed.get(shadowProducerName), expectedPosition);
            Assert.assertEquals(restored.highestReplPositionPersisted.get(shadowProducerName), expectedPosition);
            Assert.assertEquals(retryShadowMessage(secondReloadedShadow, shadowProducerName, olderMessageId,
                    originalMetadata, false), PositionFactory.create(-1L, -1L));
            Assert.assertEquals(secondReloadedShadow.getManagedLedger().getLastConfirmedEntry(), newestPosition);

            // Markers bypass deduplication, but retrying an older marker must not move the shadow ledger backwards.
            Assert.assertEquals(retryShadowMessage(secondReloadedShadow, shadowProducerName, markerMessageId,
                    markerMetadata, true), markerPosition);
            Assert.assertEquals(secondReloadedShadow.getManagedLedger().getLastConfirmedEntry(), newestPosition);
            Assert.assertEquals(retryShadowMessage(secondReloadedShadow, shadowProducerName, olderMessageId,
                    originalMetadata, false), PositionFactory.create(-1L, -1L));
            Assert.assertEquals(secondReloadedShadow.getManagedLedger().getLastConfirmedEntry(), newestPosition);

            // Subscribe after the retries so prefetched entries cannot hide a regressed readable boundary.
            try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(shadowTopicName)
                    .subscriptionName("recovered-shadow-visibility").receiverQueueSize(1)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
                boolean newestMessageReceived = false;
                for (int i = 0; i < 3; i++) {
                    Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                    Assert.assertNotNull(message,
                            "The latest source entry must remain visible after an older marker retry");
                    try {
                        consumer.acknowledge(message);
                        if (message.getMessageId().equals(newestMessageId)) {
                            Assert.assertEquals(message.getData(), new byte[]{3});
                            newestMessageReceived = true;
                            break;
                        }
                    } finally {
                        message.release();
                    }
                }
                assertTrue(newestMessageReceived,
                        "The consumer must reach the newest source entry without another write");
            }

            // Recovery must still allow a genuinely new entry through the real shadow replicator.
            admin.topics().setShadowTopics(sourceTopicName, List.of(shadowTopicName));
            MessageIdImpl newMessageId;
            try (Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopicName)
                    .producerName(producerName).enableBatching(false).create()) {
                waitForStartedShadowReplicator(sourceTopicName, shadowTopicName);
                newMessageId = (MessageIdImpl) producer.newMessage().sequenceId(10).properties(sourceProperties)
                        .value(new byte[]{4}).send();
            }
            MessageDeduplication.ReplSourcePosition newPosition = new MessageDeduplication.ReplSourcePosition(
                    newMessageId.getLedgerId(), newMessageId.getEntryId());
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                    restored.highestReplPositionPersisted.get(shadowProducerName), newPosition));
            Assert.assertEquals(secondReloadedShadow.getManagedLedger().getLastConfirmedEntry(),
                    PositionFactory.create(newMessageId.getLedgerId(), newMessageId.getEntryId()));
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
        }
    }

    @Test
    public void testPartitionedShadowDeduplicationRecoveryBeforeSnapshot() throws Exception {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        Integer originalSnapshotInterval = configuration.getBrokerDeduplicationSnapshotIntervalSeconds();
        int originalEntriesInterval = configuration.getBrokerDeduplicationEntriesInterval();
        configuration.setBrokerDeduplicationSnapshotIntervalSeconds(0);
        configuration.setBrokerDeduplicationEntriesInterval(Integer.MAX_VALUE);
        String namespace = BrokerTestUtil.newUniqueName("prop1/partitioned-shadow-replay");
        String sourceTopicName = "persistent://" + namespace + "/source";
        String shadowTopicName = "persistent://" + namespace + "/shadow";
        String sourceProducerName = "partitioned-source-producer";
        record ShadowPartition(String sourceTopic, String shadowTopic, String producerName,
                               MessageIdImpl oldestMessageId, MessageIdImpl newestMessageId,
                               MessageMetadata oldestMetadata) {
        }
        List<ShadowPartition> partitions = new ArrayList<>();
        try {
            admin.namespaces().createNamespace(namespace);
            admin.namespaces().setDeduplicationStatus(namespace, true);
            admin.topics().createPartitionedTopic(sourceTopicName, 2);
            // Partition metadata creation does not open the source ledgers that shadow cursors need.
            for (int partition = 0; partition < 2; partition++) {
                String sourcePartition = TopicName.get(sourceTopicName).getPartition(partition).toString();
                pulsar.getBrokerService().getTopic(sourcePartition, false).get(10, TimeUnit.SECONDS).orElseThrow();
            }
            admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
            admin.topics().setShadowTopics(sourceTopicName, List.of(shadowTopicName));
            for (int partition = 0; partition < 2; partition++) {
                String sourcePartition = TopicName.get(sourceTopicName).getPartition(partition).toString();
                String shadowPartition = TopicName.get(shadowTopicName).getPartition(partition).toString();
                PersistentTopic shadowTopic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(shadowPartition, false).get(10, TimeUnit.SECONDS).orElseThrow();
                MessageDeduplication deduplication = shadowTopic.getMessageDeduplication();
                Awaitility.await().untilAsserted(() -> assertTrue(deduplication.isEnabled()));
                Position initialSnapshotPosition = deduplication.getManagedCursor().getMarkDeletedPosition();
                MessageIdImpl oldestMessageId = null;
                MessageIdImpl newestMessageId = null;
                try (Producer<byte[]> producer = pulsarClient.newProducer().topic(sourcePartition)
                        .producerName(sourceProducerName).enableBatching(false).create()) {
                    waitForStartedShadowReplicator(sourcePartition, shadowTopicName);
                    // Different entry counts make accidental reuse of another partition's watermark observable.
                    for (int message = 0; message < partition + 2; message++) {
                        newestMessageId = (MessageIdImpl) producer.newMessage().sequenceId(message)
                                .value(new byte[]{(byte) partition, (byte) message}).send();
                        if (oldestMessageId == null) {
                            oldestMessageId = newestMessageId;
                        }
                    }
                }
                String shadowProducerName = ShadowReplicator.getShadowProducerName(configuration.getReplicatorPrefix(),
                        sourcePartition, shadowPartition);
                MessageDeduplication.ReplSourcePosition expectedPosition = new MessageDeduplication.ReplSourcePosition(
                        newestMessageId.getLedgerId(), newestMessageId.getEntryId());
                Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                        deduplication.highestReplPositionPersisted, Map.of(shadowProducerName, expectedPosition)));
                Assert.assertEquals(deduplication.getManagedCursor().getMarkDeletedPosition(), initialSnapshotPosition);
                assertTrue(deduplication.getManagedCursor().getProperties().isEmpty());
                PersistentTopic sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(sourcePartition, false).get(10, TimeUnit.SECONDS).orElseThrow();
                partitions.add(new ShadowPartition(sourcePartition, shadowPartition, shadowProducerName,
                        oldestMessageId, newestMessageId, readSourceMetadata(sourceTopic, oldestMessageId)));
            }
            Assert.assertNotEquals(partitions.get(0).producerName(), partitions.get(1).producerName());
            Assert.assertNotEquals(partitions.get(0).newestMessageId().getLedgerId(),
                    partitions.get(1).newestMessageId().getLedgerId());

            // Stop both live producers so neither partition's reconnect can conceal missing replay state.
            admin.topics().removeShadowTopics(sourceTopicName);
            for (ShadowPartition partition : partitions) {
                PersistentTopic sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(partition.sourceTopic(), false).get(10, TimeUnit.SECONDS).orElseThrow();
                sourceTopic.removeShadowReplicator(shadowTopicName).get(10, TimeUnit.SECONDS);
                Awaitility.await().untilAsserted(() -> assertTrue(sourceTopic.getShadowReplicators().isEmpty()));
            }
            configuration.setBrokerDeduplicationEntriesInterval(1);
            admin.topics().unload(shadowTopicName);
            for (ShadowPartition partition : partitions) {
                PersistentTopic shadowTopic = (PersistentTopic) pulsar.getBrokerService()
                        .getTopic(partition.shadowTopic(), false).get(10, TimeUnit.SECONDS).orElseThrow();
                MessageDeduplication recovered = shadowTopic.getMessageDeduplication();
                Awaitility.await().untilAsserted(() -> assertTrue(recovered.isEnabled()));
                Assert.assertEquals(shadowTopic.getShadowSourceTopic().orElseThrow().toString(),
                        partition.sourceTopic());
                MessageIdImpl newestMessageId = partition.newestMessageId();
                MessageDeduplication.ReplSourcePosition expectedPosition = new MessageDeduplication.ReplSourcePosition(
                        newestMessageId.getLedgerId(), newestMessageId.getEntryId());
                Assert.assertEquals(recovered.highestReplPositionPersisted,
                        Map.of(partition.producerName(), expectedPosition));
                Assert.assertEquals(recovered.highestReplPositionPushed,
                        Map.of(partition.producerName(), expectedPosition));
                Position newestPosition = PositionFactory.create(newestMessageId.getLedgerId(),
                        newestMessageId.getEntryId());
                Assert.assertEquals(recovered.getManagedCursor().getMarkDeletedPosition(), newestPosition);
                Assert.assertEquals(recovered.getManagedCursor().getProperties().get(partition.producerName() + "_LID"),
                        Long.valueOf(newestMessageId.getLedgerId()));
                Assert.assertEquals(recovered.getManagedCursor().getProperties().get(partition.producerName() + "_EID"),
                        Long.valueOf(newestMessageId.getEntryId()));
                Assert.assertEquals(retryShadowMessage(shadowTopic, partition.producerName(),
                        partition.oldestMessageId(), partition.oldestMetadata(), false),
                        PositionFactory.create(-1L, -1L));
                Assert.assertEquals(shadowTopic.getManagedLedger().getLastConfirmedEntry(), newestPosition);
            }

            admin.topics().setShadowTopics(sourceTopicName, List.of(shadowTopicName));
            for (int index = 0; index < partitions.size(); index++) {
                ShadowPartition partition = partitions.get(index);
                try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(partition.shadowTopic())
                        .subscriptionName("partitioned-recovered-shadow").receiverQueueSize(1)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
                     Producer<byte[]> producer = pulsarClient.newProducer().topic(partition.sourceTopic())
                             .producerName(sourceProducerName).enableBatching(false).create()) {
                    waitForStartedShadowReplicator(partition.sourceTopic(), shadowTopicName);
                    byte[] newData = new byte[]{(byte) index, 100};
                    MessageIdImpl newMessageId =
                            (MessageIdImpl) producer.newMessage().sequenceId(100).value(newData).send();
                    for (int message = 0; message < index + 3; message++) {
                        Message<byte[]> received = consumer.receive(10, TimeUnit.SECONDS);
                        Assert.assertNotNull(received, "Each recovered partition must expose old and new entries");
                        try {
                            Assert.assertEquals(received.getData(), message < index + 2
                                    ? new byte[]{(byte) index, (byte) message} : newData);
                            MessageIdImpl receivedMessageId = (MessageIdImpl) received.getMessageId();
                            Assert.assertEquals(receivedMessageId.getPartitionIndex(), index);
                            if (message == index + 2) {
                                Assert.assertEquals(receivedMessageId.getLedgerId(), newMessageId.getLedgerId());
                                Assert.assertEquals(receivedMessageId.getEntryId(), newMessageId.getEntryId());
                            }
                            consumer.acknowledge(received);
                        } finally {
                            received.release();
                        }
                    }
                }
            }
        } finally {
            configuration.setBrokerDeduplicationSnapshotIntervalSeconds(originalSnapshotInterval);
            configuration.setBrokerDeduplicationEntriesInterval(originalEntriesInterval);
        }
    }

    private void waitForStartedShadowReplicator(String sourceTopicName, String shadowTopicName) throws Exception {
        PersistentTopic sourceTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(sourceTopicName, false).get(10, TimeUnit.SECONDS).orElseThrow();
        Awaitility.await().untilAsserted(() -> {
            ShadowReplicator replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
            Assert.assertNotNull(replicator);
            Assert.assertEquals(String.valueOf(replicator.getState()), "Started");
        });
    }

    private static Position publishSourceMarker(PersistentTopic sourceTopic, String sourceCluster) throws Exception {
        CompletableFuture<Position> completed = new CompletableFuture<>();
        ByteBuf marker = Markers.newReplicatedSubscriptionsSnapshotRequest("shadow-recovery-snapshot", sourceCluster);
        try {
            sourceTopic.publishMessage(marker, new Topic.PublishContext() {
                @Override
                public String getProducerName() {
                    return "pulsar.marker";
                }

                @Override
                public boolean isMarkerMessage() {
                    return true;
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
            marker.release();
        }
    }

    private static MessageMetadata readSourceMetadata(PersistentTopic topic, MessageIdImpl messageId) throws Exception {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        topic.asyncReadEntry(PositionFactory.create(messageId.getLedgerId(), messageId.getEntryId()),
                new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        result.complete(entry);
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        result.completeExceptionally(exception);
                    }
                }, null);
        Entry entry = result.get(10, TimeUnit.SECONDS);
        try {
            return new MessageMetadata().copyFrom(Commands.parseMessageMetadata(entry.getDataBuffer()));
        } finally {
            entry.release();
        }
    }

    private static MessageIdImpl publishGeoSourceMessage(PersistentTopic sourceTopic, String geoProducerName,
                                                          String originalProducerName, long sequenceId,
                                                          String sourcePosition, byte[] payload) throws Exception {
        MessageMetadata metadata = new MessageMetadata().setProducerName(originalProducerName)
                .setReplicatedFrom("upstream").setSequenceId(sequenceId)
                .setPublishTime(System.currentTimeMillis());
        metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue(sourcePosition);
        ByteBuf payloadBuffer = Unpooled.wrappedBuffer(payload);
        ByteBuf entry;
        try {
            entry = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payloadBuffer);
        } finally {
            payloadBuffer.release();
        }
        CompletableFuture<MessageIdImpl> completed = new CompletableFuture<>();
        try {
            sourceTopic.publishMessage(entry, new Topic.PublishContext() {
                private final Map<String, Object> properties = new HashMap<>();

                @Override
                public String getProducerName() {
                    return geoProducerName;
                }

                @Override
                public long getSequenceId() {
                    return sequenceId;
                }

                @Override
                public boolean supportsReplDedupByLidAndEid() {
                    return true;
                }

                @Override
                public void setProperty(String name, Object value) {
                    properties.put(name, value);
                }

                @Override
                public Object getProperty(String name) {
                    return properties.get(name);
                }

                @Override
                public void completed(Exception exception, long ledgerId, long entryId) {
                    if (exception == null) {
                        completed.complete(new MessageIdImpl(ledgerId, entryId, -1));
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

    private static Position retryShadowMessage(PersistentTopic shadowTopic, String shadowProducerName,
                                              MessageIdImpl messageId, MessageMetadata originalMetadata,
                                              boolean marker) throws Exception {
        CompletableFuture<Position> completed = new CompletableFuture<>();
        MessageMetadata metadata = new MessageMetadata().setProducerName(originalMetadata.getProducerName())
                .setSequenceId(originalMetadata.getSequenceId()).setPublishTime(originalMetadata.getPublishTime());
        metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION)
                .setValue(messageId.getLedgerId() + ":" + messageId.getEntryId());
        if (marker) {
            metadata.setMarkerType(originalMetadata.getMarkerType());
        }
        class ShadowPublishContext implements Topic.PublishContext, Position {
            private final Map<String, Object> properties = new HashMap<>();

            @Override
            public long getLedgerId() {
                return messageId.getLedgerId();
            }

            @Override
            public long getEntryId() {
                return messageId.getEntryId();
            }

            @Override
            public String getProducerName() {
                return shadowProducerName;
            }

            @Override
            public boolean supportsReplDedupByLidAndEid() {
                return true;
            }

            @Override
            public boolean isMarkerMessage() {
                return marker;
            }

            @Override
            public void setProperty(String name, Object value) {
                properties.put(name, value);
            }

            @Override
            public Object getProperty(String name) {
                return properties.get(name);
            }

            @Override
            public void completed(Exception exception, long ledgerId, long entryId) {
                if (exception == null) {
                    completed.complete(PositionFactory.create(ledgerId, entryId));
                } else {
                    completed.completeExceptionally(exception);
                }
            }
        }
        ByteBuf entry = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
        try {
            shadowTopic.publishMessage(entry, new ShadowPublishContext());
            return completed.get(10, TimeUnit.SECONDS);
        } finally {
            entry.release();
        }
    }

    @Test
    public void testShadowReplicatorReleasesSourceEntryBuffer() throws Exception {
        String sourceTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-source/source-topic");
        String shadowTopicName = BrokerTestUtil.newUniqueName("persistent://prop1/ns-shadow/shadow-topic");

        admin.topics().createNonPartitionedTopic(sourceTopicName);
        admin.topics().createShadowTopic(shadowTopicName, sourceTopicName);
        admin.topics().setShadowTopics(sourceTopicName, Lists.newArrayList(shadowTopicName));

        PersistentTopic sourceTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopicName).get().get();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(sourceTopic.getShadowReplicators().size(), 1));
        ShadowReplicator replicator = (ShadowReplicator) sourceTopic.getShadowReplicators().get(shadowTopicName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(String.valueOf(replicator.getState()), "Started"));

        Entry entry = createEntry(1, 0, "ref-count-check", 1);
        ByteBuf entryBuffer = entry.getDataBuffer();
        Assert.assertEquals(entryBuffer.refCnt(), 1);

        List<Entry> entries = Lists.newArrayList(entry);
        PersistentReplicator.InFlightTask inFlightTask =
                new PersistentReplicator.InFlightTask(
                        entry.getPosition(), entries.size(), replicator.getReplicatorId());
        inFlightTask.setEntries(entries);
        Assert.assertTrue(replicator.replicateEntries(entries, inFlightTask));

        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(inFlightTask.isDone());
            Assert.assertEquals(entryBuffer.refCnt(), 0);
        });
    }

    private static PersistentReplicator getAnyShadowReplicator(TopicName topicName, PulsarService pulsar) {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName.toString(), false).join().get();
        Awaitility.await().until(() -> !persistentTopic.getShadowReplicators().isEmpty());
        return (PersistentReplicator) persistentTopic.getShadowReplicators().values().iterator().next();
    }

    private static void waitReplicateFinish(TopicName topicName, PulsarAdmin admin){
        Awaitility.await().untilAsserted(() -> {
            for (Map.Entry<String, ? extends ReplicatorStats> subStats :
                    admin.topics().getStats(topicName.toString(), true, false, false).getReplication().entrySet()){
                assertTrue(subStats.getValue().getReplicationBacklog() == 0, "replication task finished");
            }
        });
    }

    @Test
    public void testCounterOfPendingMessagesCorrect() throws Exception {
        TopicName sourceTopicName = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://prop1/ns-source/source-topic"));
        TopicName shadowTopicName = TopicName
                .get(BrokerTestUtil.newUniqueName("persistent://prop1/ns-shadow/shadow-topic"));

        admin.topics().createNonPartitionedTopic(sourceTopicName.toString());
        admin.topics().createShadowTopic(shadowTopicName.toString(), sourceTopicName.toString());
        admin.topics().setShadowTopics(sourceTopicName.toString(), Lists.newArrayList(shadowTopicName.toString()));

        // Init replicator and send many messages.
        final String subName = "my-sub";
        @Cleanup
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(sourceTopicName.toString())
                .subscriptionName(subName)
                .receiverQueueSize(10)
                .subscribe();
        @Cleanup
        Producer<Schemas.PersonOne> producer = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(sourceTopicName.toString())
                .enableBatching(false)
                .create();
        for (int i = 0; i < 20; i++) {
            producer.send(new Schemas.PersonOne(i));
        }

        // Verify "inflight replication tasks" are correct.
        PersistentReplicator replicator = getAnyShadowReplicator(sourceTopicName, pulsar);
        waitReplicateFinish(sourceTopicName, admin);
        ensureNoBacklogByInflightTask(replicator);
    }

    private Entry createEntry(long ledgerId, long entryId, String message, long sequenceId) {
        ByteBuf headersAndPayload = createMessage(message, sequenceId);
        Entry entry = EntryImpl.create(ledgerId, entryId, headersAndPayload);
        headersAndPayload.release();
        return entry;
    }

    private ByteBuf createMessage(String message, long sequenceId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPublishTime(System.currentTimeMillis());
        ByteBuf payload = Unpooled.copiedBuffer(message.getBytes(StandardCharsets.UTF_8));
        ByteBuf headersAndPayload = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
        payload.release();
        return headersAndPayload;
    }

}
