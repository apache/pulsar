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

import static org.apache.bookkeeper.mledger.ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.Lists;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.CustomLog;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
public class ShadowTopicTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setEnableShadowTopics(true);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected String newTopicName() {
        return BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/shadow-test");
    }

    @Test
    public void testNonPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        //1. test shadow topic setting in topic creation.
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        PersistentTopic brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
    }

    @Test
    public void testShadowSourcePropertyCannotBeRemoved() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        Assert.assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.topics().removeProperties(shadowTopic, PROPERTY_SOURCE_TOPIC_KEY));
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        String partitionedSourceTopic = newTopicName();
        String partitionedShadowTopic = partitionedSourceTopic + "-shadow";
        admin.topics().createPartitionedTopic(partitionedSourceTopic, 2);
        admin.topics().createShadowTopic(partitionedShadowTopic, partitionedSourceTopic);
        Assert.assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.topics().removeProperties(partitionedShadowTopic, PROPERTY_SOURCE_TOPIC_KEY));
        Assert.assertEquals(admin.topics().getShadowSource(partitionedShadowTopic), partitionedSourceTopic);
    }

    @Test
    public void testPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        String sourceTopicPartition = TopicName.get(sourceTopic).getPartition(0).toString();
        String shadowTopicPartition = TopicName.get(shadowTopic).getPartition(0).toString();

        //1. test shadow topic setting in topic creation.
        admin.topics().createPartitionedTopic(sourceTopic, 2);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        pulsarClient.newProducer().topic(shadowTopic).create().close(); //trigger loading partitions.

        PersistentTopic brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopicPartition);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());

        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopicPartition);
    }

    @Test
    public void testPartitionedShadowDeletionWithoutStoredSourcePropertyKeepsSourceLedgers() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        String sourceTopicPartition = TopicName.get(sourceTopic).getPartition(0).toString();
        String shadowTopicPartition = TopicName.get(shadowTopic).getPartition(0).toString();
        admin.topics().createPartitionedTopic(sourceTopic, 1);
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        for (int i = 0; i < 10; i++) {
            producer.send("msg-" + i);
        }
        Set<Long> sourceLedgerIds = admin.topics().getInternalStats(sourceTopicPartition).ledgers.stream()
                .map(ledger -> ledger.ledgerId).collect(Collectors.toSet());
        Assert.assertFalse(sourceLedgerIds.isEmpty());

        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        pulsar.getBrokerService().getTopic(shadowTopicPartition, false).get(10, TimeUnit.SECONDS).get();
        admin.topics().unload(shadowTopicPartition);

        // Store the partition metadata as written by earlier versions, where the shadow source was only kept in
        // the partitioned topic metadata.
        String shadowManagedLedgerName = TopicName.get(shadowTopicPartition).getPersistenceNamingEncoding();
        removeStoredSourceProperty(shadowManagedLedgerName);
        org.apache.bookkeeper.mledger.ManagedLedgerInfo storedInfo =
                pulsar.getDefaultManagedLedgerFactory().getManagedLedgerInfo(shadowManagedLedgerName);
        Assert.assertTrue(storedInfo.properties == null
                || !storedInfo.properties.containsKey(PROPERTY_SOURCE_TOPIC_KEY));
        Assert.assertTrue(storedInfo.ledgers.stream().map(ledger -> ledger.ledgerId).collect(Collectors.toSet())
                .containsAll(sourceLedgerIds));

        // Shadow topics are disabled, so the partition is deleted without being loaded.
        pulsar.getConfiguration().setEnableShadowTopics(false);
        try {
            admin.topics().deletePartitionedTopic(shadowTopic);
        } finally {
            pulsar.getConfiguration().setEnableShadowTopics(true);
        }
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopicPartition).isEmpty());

        Awaitility.await().during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertTrue(pulsarTestContext.getMockBookKeeper().getLedgers().containsAll(sourceLedgerIds),
                        "Ledgers " + sourceLedgerIds + " should remain in "
                                + pulsarTestContext.getMockBookKeeper().getLedgers()));
    }

    @Test
    public void testShadowSourcePropertyCannotBeUpdatedToNull() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        String sourceTopicPartition = TopicName.get(sourceTopic).getPartition(0).toString();
        String shadowTopicPartition = TopicName.get(shadowTopic).getPartition(0).toString();
        admin.topics().createPartitionedTopic(sourceTopic, 1);
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        for (int i = 0; i < 10; i++) {
            producer.send("msg-" + i);
        }
        Set<Long> sourceLedgerIds = admin.topics().getInternalStats(sourceTopicPartition).ledgers.stream()
                .map(ledger -> ledger.ledgerId).collect(Collectors.toSet());
        Assert.assertFalse(sourceLedgerIds.isEmpty());

        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        pulsar.getBrokerService().getTopic(shadowTopicPartition, false).get(10, TimeUnit.SECONDS).get();
        admin.topics().unload(shadowTopicPartition);
        // Partition metadata as written by earlier versions, without the shadow source.
        removeStoredSourceProperty(TopicName.get(shadowTopicPartition).getPersistenceNamingEncoding());

        Assert.assertEquals(updatePropertiesWithNullSource(shadowTopic), 412);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        String nonPartitionedSourceTopic = newTopicName();
        String nonPartitionedShadowTopic = nonPartitionedSourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(nonPartitionedSourceTopic);
        admin.topics().createShadowTopic(nonPartitionedShadowTopic, nonPartitionedSourceTopic);
        Assert.assertEquals(updatePropertiesWithNullSource(nonPartitionedShadowTopic), 412);
        Assert.assertEquals(admin.topics().getShadowSource(nonPartitionedShadowTopic), nonPartitionedSourceTopic);

        // Shadow topics are disabled, so the partition is deleted without being loaded.
        pulsar.getConfiguration().setEnableShadowTopics(false);
        try {
            admin.topics().deletePartitionedTopic(shadowTopic);
        } finally {
            pulsar.getConfiguration().setEnableShadowTopics(true);
        }
        Awaitility.await().during(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertTrue(pulsarTestContext.getMockBookKeeper().getLedgers().containsAll(sourceLedgerIds),
                        "Ledgers " + sourceLedgerIds + " should remain in "
                                + pulsarTestContext.getMockBookKeeper().getLedgers()));
    }

    /**
     * Sends the request directly, since the admin client leaves out properties with a null value.
     */
    private int updatePropertiesWithNullSource(String topic) throws Exception {
        TopicName topicName = TopicName.get(topic);
        URI uri = URI.create(pulsar.getWebServiceAddress() + "/admin/v2/persistent/" + topicName.getTenant() + "/"
                + topicName.getNamespacePortion() + "/" + topicName.getEncodedLocalName() + "/properties");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"" + PROPERTY_SOURCE_TOPIC_KEY + "\":null}"))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    @Test
    public void testShadowTopicSubscriptionDeletionDeletesPendingAckStoreLedgers() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().createSubscription(shadowTopic, "sub-1", MessageId.earliest);
        admin.topics().createSubscription(shadowTopic, "sub-2", MessageId.earliest);
        Set<Long> pendingAckLedgerIds1 = createClosedPendingAckStore(shadowTopic, "sub-1");
        Set<Long> pendingAckLedgerIds2 = createClosedPendingAckStore(shadowTopic, "sub-2");

        pulsar.getConfiguration().setTransactionCoordinatorEnabled(true);
        try {
            admin.topics().deleteSubscription(shadowTopic, "sub-1");
            assertPendingAckStoreDeleted(shadowTopic, "sub-1", pendingAckLedgerIds1);

            admin.topics().delete(shadowTopic, true);
            assertPendingAckStoreDeleted(shadowTopic, "sub-2", pendingAckLedgerIds2);
        } finally {
            pulsar.getConfiguration().setTransactionCoordinatorEnabled(false);
        }
    }

    @Test
    public void testShadowTopicSubscriptionDeletionUsesTopicOffloaderForPendingAckStore() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().createSubscription(shadowTopic, "sub", MessageId.earliest);

        Map<Long, UUID> offloads = new ConcurrentHashMap<>();
        Set<Long> deletedOffloads = ConcurrentHashMap.newKeySet();
        LedgerOffloader topicOffloader = mock(LedgerOffloader.class);
        when(topicOffloader.getOffloadDriverName()).thenReturn("topic-offloader");
        when(topicOffloader.getOffloadPolicies()).thenReturn(null);
        when(topicOffloader.isAppendable()).thenReturn(true);
        when(topicOffloader.offload(any(), any(), any())).thenAnswer(invocation -> {
            ReadHandle ledger = invocation.getArgument(0);
            offloads.put(ledger.getId(), invocation.getArgument(1));
            return CompletableFuture.completedFuture(null);
        });
        when(topicOffloader.deleteOffloaded(anyLong(), any(), any())).thenAnswer(invocation -> {
            deletedOffloads.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        // A pending ack store created with the config of the topic, which has a topic specific offloader
        String pendingAckStoreName = TopicName.get(MLPendingAckStore.getTransactionPendingAckStoreSuffix(shadowTopic,
                "sub")).getPersistenceNamingEncoding();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setLedgerOffloader(topicOffloader);
        ManagedLedger pendingAckStore = pulsar.getDefaultManagedLedgerFactory().open(pendingAckStoreName, config);
        for (int i = 0; i < 5; i++) {
            pendingAckStore.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        pendingAckStore.offloadPrefix(pendingAckStore.getLastConfirmedEntry());
        Set<Long> offloadedLedgerIds = Set.copyOf(offloads.keySet());
        Assert.assertFalse(offloadedLedgerIds.isEmpty());
        pendingAckStore.close();
        Set<Long> ledgerIds = pulsar.getDefaultManagedLedgerFactory().getManagedLedgerInfo(pendingAckStoreName)
                .ledgers.stream().map(ledger -> ledger.ledgerId).collect(Collectors.toSet());

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(shadowTopic).get();
        topic.getManagedLedger().getConfig().setLedgerOffloader(topicOffloader);

        pulsar.getConfiguration().setTransactionCoordinatorEnabled(true);
        try {
            admin.topics().deleteSubscription(shadowTopic, "sub");
            assertPendingAckStoreDeleted(shadowTopic, "sub", ledgerIds);
            Assert.assertEquals(deletedOffloads, offloadedLedgerIds);
        } finally {
            pulsar.getConfiguration().setTransactionCoordinatorEnabled(false);
        }
    }

    private Set<Long> createClosedPendingAckStore(String topic, String subscription) throws Exception {
        String pendingAckStoreName = TopicName.get(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic,
                subscription)).getPersistenceNamingEncoding();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        ManagedLedger pendingAckStore = pulsar.getDefaultManagedLedgerFactory().open(pendingAckStoreName, config);
        for (int i = 0; i < 5; i++) {
            pendingAckStore.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
        }
        pendingAckStore.close();
        Set<Long> ledgerIds = pulsar.getDefaultManagedLedgerFactory().getManagedLedgerInfo(pendingAckStoreName)
                .ledgers.stream().map(ledger -> ledger.ledgerId).collect(Collectors.toSet());
        Assert.assertFalse(ledgerIds.isEmpty());
        Assert.assertTrue(pulsarTestContext.getMockBookKeeper().getLedgers().containsAll(ledgerIds));
        return ledgerIds;
    }

    private void assertPendingAckStoreDeleted(String topic, String subscription, Set<Long> ledgerIds) {
        String pendingAckStoreName = TopicName.get(MLPendingAckStore.getTransactionPendingAckStoreSuffix(topic,
                subscription)).getPersistenceNamingEncoding();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertFalse(pulsar.getDefaultManagedLedgerFactory().asyncExists(pendingAckStoreName).get());
            Set<Long> remaining = new HashSet<>(pulsarTestContext.getMockBookKeeper().getLedgers());
            remaining.retainAll(ledgerIds);
            Assert.assertTrue(remaining.isEmpty(), "Ledgers " + remaining + " should be deleted");
        });
    }

    private void removeStoredSourceProperty(String shadowManagedLedgerName) throws Exception {
        MetaStore store = ((ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory()).getMetaStore();
        CompletableFuture<Void> updateFuture = new CompletableFuture<>();
        store.getManagedLedgerInfo(shadowManagedLedgerName, false, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                ManagedLedgerInfo unmarkedInfo = new ManagedLedgerInfo();
                unmarkedInfo.addAllLedgerInfos(mlInfo.getLedgerInfosList());
                store.asyncUpdateLedgerIds(shadowManagedLedgerName, unmarkedInfo, stat,
                        new MetaStore.MetaStoreCallback<>() {
                            @Override
                            public void operationComplete(Void result, Stat stat) {
                                updateFuture.complete(null);
                            }

                            @Override
                            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                updateFuture.completeExceptionally(e);
                            }
                        });
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                updateFuture.completeExceptionally(e);
            }
        });
        updateFuture.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testPartitionedShadowTopicProduceAndConsume() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createPartitionedTopic(sourceTopic, 3);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);

        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic).subscriptionName("test")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("msg-" + i);
        }

        Set<String> set = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive();
            set.add(msg.getValue());
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(set.contains("msg-" + i));
        }
    }

    @Test
    public void testShadowTopicNotWritable() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(shadowTopic).create();
        Assert.expectThrows(PulsarClientException.NotAllowedException.class, () -> producer.send(new byte[]{1, 2, 3}));
    }

    private void awaitUntilShadowReplicatorReady(String sourceTopic, String shadowTopic) {
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic sourcePersistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopic).get().get();
            ShadowReplicator
                    replicator = (ShadowReplicator) sourcePersistentTopic.getShadowReplicators().get(shadowTopic);
            Assert.assertNotNull(replicator);
            Assert.assertEquals(String.valueOf(replicator.getState()), "Started");
        });
    }

    @Test
    public void testShadowTopicConsuming() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);

        @Cleanup Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopic).create();
        @Cleanup Consumer<byte[]> consumer =
                pulsarClient.newConsumer().topic(shadowTopic).subscriptionName("sub").subscribe();
        byte[] content = "Hello Shadow Topic".getBytes(StandardCharsets.UTF_8);
        MessageId id = producer.send(content);
        log.info().attr("id", id).log("msg send to source topic, id");
        Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);
    }

    @Test
    public void testShadowTopicConsumingWithStringSchema() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);

        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        @Cleanup Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        String content = "Hello Shadow Topic";
        MessageId id = producer.send(content);
        Message<String> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);

        for (int i = 0; i < 10; i++) {
            producer.send(content + i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(consumer.receive().getValue(), content + i);
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    private static class Point {
        int x;
        int y;
    }

    @Test
    public void testShadowTopicConsumingWithJsonSchema() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));
        awaitUntilShadowReplicatorReady(sourceTopic, shadowTopic);

        @Cleanup Producer<Point> producer =
                pulsarClient.newProducer(Schema.JSON(Point.class)).topic(sourceTopic).create();
        @Cleanup Consumer<Point> consumer =
                pulsarClient.newConsumer(Schema.JSON(Point.class)).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        Point content = new Point(1, 2);
        MessageId id = producer.send(content);
        Message<Point> msg = consumer.receive();
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);
    }

    @Test
    public void testConsumeShadowMessageWithoutCache() throws Exception {
        String sourceTopic = newTopicName();
        String shadowTopic = sourceTopic + "-shadow";
        admin.topics().createNonPartitionedTopic(sourceTopic);
        @Cleanup Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        String content = "Hello Shadow Topic";
        MessageId id = producer.send(content);
        for (int i = 0; i < 10; i++) {
            producer.send(content + i);
        }

        // Unload the source topic to trigger a ledger rollover. The ShadowManagedLedgerImpl
        // reads entries from the source's BookKeeper ledgers via metadata watch. Without the
        // shadow replicator enabled, it can only discover entries in closed ledgers (the
        // metadata for open ledgers shows entries=0). Unloading forces the current ledger
        // to close so the shadow topic can see all entries.
        admin.topics().unload(sourceTopic);

        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        @Cleanup Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic).subscriptionName("sub")
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();

        Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
        Assert.assertNotNull(msg, "Should have received a message from shadow topic");
        Assert.assertEquals(msg.getMessageId(), id);
        Assert.assertEquals(msg.getValue(), content);

        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Should have received message " + i + " from shadow topic");
            Assert.assertEquals(msg.getValue(), content + i);
        }
    }
}
