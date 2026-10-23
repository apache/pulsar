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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionFencedException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicMigratedException;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated.ResourceType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NonPersistentTopicTest extends BrokerTestBase {

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
        return Mockito.spy(brokerService);
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAccumulativeStats() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/aTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // stats are at zero before any activity
        TopicStats stats = topic.getStats(false, false, false);
        assertEquals(stats.getBytesInCounter(), 0);
        assertEquals(stats.getMsgInCounter(), 0);
        assertEquals(stats.getBytesOutCounter(), 0);
        assertEquals(stats.getMsgOutCounter(), 0);

        producer.newMessage().value("test").eventTime(5).send();

        Message<String> msg = consumer1.receive();
        assertNotNull(msg);
        msg = consumer2.receive();
        assertNotNull(msg);

        // send/receive result in non-zero stats
        TopicStats statsBeforeUnsubscribe = topic.getStats(false, false, false);
        assertTrue(statsBeforeUnsubscribe.getBytesInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getBytesOutCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgOutCounter() > 0);

        consumer1.unsubscribe();
        consumer2.unsubscribe();
        producer.close();
        topic.getProducers().values().forEach(topic::removeProducer);
        assertEquals(topic.getProducers().size(), 0);

        // consumer unsubscribe/producer removal does not result in stats loss
        TopicStats statsAfterUnsubscribe = topic.getStats(false, false, false);
        assertEquals(statsAfterUnsubscribe.getBytesInCounter(), statsBeforeUnsubscribe.getBytesInCounter());
        assertEquals(statsAfterUnsubscribe.getMsgInCounter(), statsBeforeUnsubscribe.getMsgInCounter());
        assertEquals(statsAfterUnsubscribe.getBytesOutCounter(), statsBeforeUnsubscribe.getBytesOutCounter());
        assertEquals(statsAfterUnsubscribe.getMsgOutCounter(), statsBeforeUnsubscribe.getMsgOutCounter());
    }

    @Test
    public void testCreateNonExistentPartitions() throws PulsarAdminException {
        final String topicName = "non-persistent://prop/ns-abc/testCreateNonExistentPartitions";
        admin.topics().createPartitionedTopic(topicName, 4);
        TopicName partition = TopicName.get(topicName).getPartition(4);
        assertThrows(PulsarClientException.NotFoundException.class, () -> {
            @Cleanup
            Producer<byte[]> ignored = pulsarClient.newProducer()
                    .topic(partition.toString())
                    .create();
        });
        assertEquals(admin.topics().getPartitionedTopicMetadata(topicName).partitions, 4);
    }


    /**
     * Regression test for the migration-redirect race in {@code NonPersistentTopic.internalSubscribe}
     * (introduced by PR #26051, which turned a blocking, ordered migration redirect into a fire-and-forget
     * async one). When the topic is migrated, the migration check must complete <i>before</i>
     * {@code addConsumerToSubscription}, and the consumer must NOT be attached to the subscription on the
     * old cluster. The previous code ran {@code getMigratedClusterUrlAsync().thenAccept(consumer::topicMigrated)}
     * concurrently with {@code addConsumerToSubscription}, so the consumer could be added before the redirect
     * and disconnect ran. The migration check must fail the subscribe before attaching a consumer, so that
     * the subscribe failure path releases the usage count and ServerCnx can redirect the request.
     */
    @Test
    public void testSubscribeOnMigratedTopicSkipsAddingConsumer() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/migration-race-" + UUID.randomUUID();
        final String subName = "migration-sub";

        // Materialize the real topic on the broker and acquire namespace-bundle ownership via a client lookup.
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        NonPersistentTopic realTopic =
                (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // Mark the local cluster as migrated so AbstractTopic.getMigratedClusterUrlAsync() (used by
        // the subscribe migration check) resolves to a present migrated-cluster URL.
        ClusterUrl migratedUrl = new ClusterUrl("http://migrated:8080", "https://migrated:8443",
                "pulsar://migrated:6650", "pulsar+ssl://migrated:6651");
        admin.clusters().updateClusterMigration(conf.getClusterName(), true, migratedUrl);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                AbstractTopic.getMigratedClusterUrlAsync(pulsar, topicName).get().isPresent());

        // Spy the topic and force isMigrated() so the subscription is considered migrated.
        NonPersistentTopic spyTopic = Mockito.spy(realTopic);
        Mockito.doReturn(true).when(spyTopic).isMigrated();

        // Pre-install a spy subscription so we can assert addConsumer is never invoked when migrated.
        NonPersistentSubscription spySubscription =
                Mockito.spy(new NonPersistentSubscription(spyTopic, subName, Collections.emptyMap()));
        spyTopic.getSubscriptions().put(subName, spySubscription);

        // An active transport connection that records the migration redirect.
        PulsarCommandSender commandSender = Mockito.mock(PulsarCommandSender.class);
        TransportCnx cnx = Mockito.mock(TransportCnx.class);
        Mockito.doReturn(true).when(cnx).isActive();
        Mockito.doReturn(true).when(cnx).isBatchMessageCompatibleVersion();
        Mockito.doReturn("test-role").when(cnx).getAuthRole();
        Mockito.doReturn(pulsar.getBrokerService()).when(cnx).getBrokerService();
        Mockito.doReturn(commandSender).when(cnx).getCommandSender();

        SubscriptionOption option = SubscriptionOption.builder()
                .cnx(cnx)
                .subscriptionName(subName)
                .consumerId(1L)
                .subType(CommandSubscribe.SubType.Shared)
                .priorityLevel(0)
                .consumerName("consumer-1")
                .isDurable(false)
                .startMessageId(null)
                .metadata(Collections.emptyMap())
                .readCompacted(false)
                .initialPosition(CommandSubscribe.InitialPosition.Latest)
                .startMessageRollbackDurationSec(0)
                .replicatedSubscriptionStateArg(false)
                .keySharedMeta(null)
                .subscriptionProperties(Optional.empty())
                .build();

        long usageBefore = spyTopic.currentUsageCount();

        assertThatThrownBy(() -> spyTopic.subscribe(option).get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TopicMigratedException.class);

        // ServerCnx handles the failed subscribe and sends the redirect; the topic must not disconnect a consumer.
        Mockito.verify(commandSender, Mockito.never()).sendTopicMigrated(Mockito.any(), Mockito.anyLong(),
                Mockito.any(), Mockito.any());

        // The core regression assertion: a migrated topic must never attach the consumer to the
        // subscription. The buggy code added it before the async redirect/disconnect could run.
        Mockito.verify(spySubscription, Mockito.never()).addConsumer(Mockito.any());
        assertTrue(spySubscription.getConsumers().isEmpty());

        // The subscribe failure must balance handleConsumerAdded's increment without dispatcher removal.
        assertEquals(spyTopic.currentUsageCount(), usageBefore);
    }

    @DataProvider
    public Object[][] migrationSubscriptionTypes() {
        return new Object[][] {
                {SubscriptionType.Exclusive}, {SubscriptionType.Failover},
                {SubscriptionType.Shared}, {SubscriptionType.Key_Shared}
        };
    }

    @Test(dataProvider = "migrationSubscriptionTypes")
    public void testMigratedSubscribeReleasesUsageCount(SubscriptionType subscriptionType) throws Exception {
        String topicName = "non-persistent://prop/ns-abc/migration-cleanup-" + UUID.randomUUID();
        String subName = "migration-sub";
        @Cleanup
        Consumer<byte[]> existingConsumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subName).subscriptionType(subscriptionType).subscribe();
        NonPersistentTopic topic =
                (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).orElseThrow();
        NonPersistentSubscription subscription = topic.getSubscriptions().get(subName);
        BrokerService brokerService = pulsar.getBrokerService();
        PulsarResources resources = Mockito.spy(pulsar.getPulsarResources());
        Mockito.doReturn(resources).when(pulsar).getPulsarResources();
        assertEquals(topic.currentUsageCount(), 1L);

        // Let the new client finish lookup, but delay its subscribe ownership check until migration starts.
        CompletableFuture<Void> resumeSubscribe = new CompletableFuture<>();
        CountDownLatch subscribeStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            CompletableFuture<Void> ownership = (CompletableFuture<Void>) invocation.callRealMethod();
            return ownership.thenCompose(__ -> {
                subscribeStarted.countDown();
                return resumeSubscribe;
            });
        }).when(brokerService).checkTopicNsOwnership(topicName);
        // Observe only the pending subscribe's client; the existing consumer also receives a migration notification.
        CompletableFuture<CommandTopicMigrated> migrationRedirect = new CompletableFuture<>();
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl());
        @Cleanup
        PulsarClient reconnectingClient = InjectedClientCnxClientBuilder.create(clientBuilder,
                (clientConf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, clientConf, eventLoopGroup) {
                    @Override
                    protected void handleTopicMigrated(CommandTopicMigrated command) {
                        super.handleTopicMigrated(command);
                        migrationRedirect.complete(new CommandTopicMigrated().copyFrom(command));
                    }
                });
        reconnectingClient.newConsumer().topic(topicName).subscriptionName(subName)
                .subscriptionType(subscriptionType).subscribeAsync();
        assertTrue(subscribeStarted.await(10, TimeUnit.SECONDS));

        ClusterUrl migratedUrl = new ClusterUrl("http://migrated:8080", null, "pulsar://migrated:6650", null);
        admin.clusters().updateClusterMigration(conf.getClusterName(), true, migratedUrl);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                AbstractTopic.getMigratedClusterUrlAsync(pulsar, topicName).get().isPresent());
        CompletableFuture<Void> migration;
        try {
            // Keep the existing dispatcher in the subscription while the new subscribe observes migration.
            // The migration flag is set before checkClusterMigration disconnects the existing consumer.
            synchronized (subscription.getDispatcher()) {
                migration = CompletableFuture.completedFuture(null)
                        .thenComposeAsync(__ -> topic.checkClusterMigration(), pulsar.getExecutor());
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(topic::isMigrated);
                CountDownLatch migrationPolicyRead = new CountDownLatch(1);
                Mockito.doAnswer(invocation -> {
                    migrationPolicyRead.countDown();
                    return invocation.callRealMethod();
                }).when(resources).getClusterResources();
                pulsar.getExecutor().execute(() -> resumeSubscribe.complete(null));
                assertTrue(migrationPolicyRead.await(10, TimeUnit.SECONDS),
                        "The pending subscribe must reach its migration check with the existing subscription");
                assertTrue(topic.getSubscriptions().get(subName) == subscription);
                assertEquals(subscription.getConsumers().size(), 1,
                        "The old consumer must still be attached when the pending subscribe checks migration");
            }
            migration.get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(topic.currentUsageCount(), 0L,
                            "Both the existing consumer and the redirected subscribe must release their usage"));
            assertTrue(subscription.getConsumers().isEmpty());

            CommandTopicMigrated redirect = migrationRedirect.get(10, TimeUnit.SECONDS);
            assertEquals(redirect.getResourceType(), ResourceType.Consumer);
            assertEquals(redirect.getBrokerServiceUrl(), migratedUrl.getBrokerServiceUrl());
            assertFalse(redirect.hasBrokerServiceUrlTls());
            admin.topics().delete(topicName, true);
        } finally {
            resumeSubscribe.complete(null);
            Mockito.doCallRealMethod().when(brokerService).checkTopicNsOwnership(topicName);
            Mockito.doCallRealMethod().when(resources).getClusterResources();
            Mockito.doCallRealMethod().when(pulsar).getPulsarResources();
        }
    }

    @Test
    public void testSubscriptionsOnNonPersistentTopic() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/topic_" + UUID.randomUUID();
        final String exclusiveSubName = "exclusive";
        final String failoverSubName = "failover";
        final String sharedSubName = "shared";
        final String keySharedSubName = "key_shared";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        producer.send("This is a message");
        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        NonPersistentTopic mockTopic = Mockito.spy(topic);
        pulsar.getBrokerService().getTopics().put(topicName, CompletableFuture.completedFuture(Optional.of(mockTopic)));
        Mockito
                .doAnswer(inv -> {
                    SubscriptionOption option = inv.getArgument(0);
                    if (option.isDurable()) {
                        return CompletableFuture.failedFuture(
                                new IllegalArgumentException("isDurable cannot be true when subscribe "
                                        + "on non-persistent topic"));
                    }
                    return inv.callRealMethod();
                }).when(mockTopic).subscribe(Mockito.any());

        @Cleanup
        Consumer<String> exclusiveConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(exclusiveSubName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        @Cleanup
        Consumer<String> failoverConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(failoverSubName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> failoverConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(failoverSubName)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> sharedConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(sharedSubName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> sharedConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(sharedSubName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        @Cleanup
        Consumer<String> keySharedConsumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(keySharedSubName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();
        @Cleanup
        Consumer<String> keySharedConsumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(keySharedSubName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe();

        final var subscriptionMap = mockTopic.getSubscriptions();
        assertEquals(subscriptionMap.size(), 4);

        // Check exclusive subscription
        NonPersistentSubscription exclusiveSub = subscriptionMap.get(exclusiveSubName);
        assertNotNull(exclusiveSub);
        exclusiveConsumer.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(exclusiveSubName) == null);

        // Check failover subscription
        NonPersistentSubscription failoverSub = subscriptionMap.get(failoverSubName);
        assertNotNull(failoverSub);
        failoverConsumer1.close();
        failoverSub = subscriptionMap.get(failoverSubName);
        assertNotNull(failoverSub);
        failoverConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(failoverSubName) == null);

        // Check shared subscription
        NonPersistentSubscription sharedSub = subscriptionMap.get(sharedSubName);
        assertNotNull(sharedSub);
        sharedConsumer1.close();
        sharedSub = subscriptionMap.get(sharedSubName);
        assertNotNull(sharedSub);
        sharedConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(sharedSubName) == null);

        // Check KeyShared subscription
        NonPersistentSubscription keySharedSub = subscriptionMap.get(keySharedSubName);
        assertNotNull(keySharedSub);
        keySharedConsumer1.close();
        keySharedSub = subscriptionMap.get(keySharedSubName);
        assertNotNull(keySharedSub);
        keySharedConsumer2.close();
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> subscriptionMap.get(keySharedSubName) == null);
    }


    @Test
    public void testReconnectWhileSubscriptionRemovalIsPending() throws Exception {
        String topicName = "non-persistent://prop/ns-abc/reconnect-" + UUID.randomUUID();
        String subName = "sub";
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName).enableBatching(false).create();
        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topicName).orElseThrow();
        NonPersistentTopic spyTopic = Mockito.spy(topic);
        pulsar.getBrokerService().getTopics().put(topicName,
                CompletableFuture.completedFuture(Optional.of(spyTopic)));
        NonPersistentSubscription subscription = Mockito.spy(
                new NonPersistentSubscription(spyTopic, subName, Collections.emptyMap()));
        spyTopic.getSubscriptions().put(subName, subscription);

        CompletableFuture<Void> removalQueued = new CompletableFuture<>();
        CompletableFuture<Void> allowRemoval = new CompletableFuture<>();
        Mockito.doAnswer(inv -> {
            CompletableFuture<Void> removal = allowRemoval.thenCompose(__ -> topic.unsubscribe(subName));
            removalQueued.complete(null);
            return removal;
        }).when(spyTopic).unsubscribe(subName);

        try {
            @Cleanup
            Consumer<String> first = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName(subName).subscriptionMode(SubscriptionMode.NonDurable).subscribe();
            first.close();
            removalQueued.get(10, TimeUnit.SECONDS);

            // Make the reconnect reach the old subscription before the queued removal can execute.
            CompletableFuture<Throwable> addAttempt = new CompletableFuture<>();
            Mockito.doAnswer(inv -> {
                @SuppressWarnings("unchecked")
                CompletableFuture<Void> result = (CompletableFuture<Void>) inv.callRealMethod();
                result.whenComplete((__, error) -> addAttempt.complete(error));
                return result;
            }).when(subscription).addConsumer(Mockito.any());
            CompletableFuture<Consumer<String>> reconnect = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName(subName).subscriptionMode(SubscriptionMode.NonDurable).subscribeAsync();

            assertTrue(addAttempt.get(10, TimeUnit.SECONDS) instanceof SubscriptionFencedException);
            assertTrue(subscription.getConsumers().isEmpty());

            allowRemoval.complete(null);
            @Cleanup
            Consumer<String> second = reconnect.get(30, TimeUnit.SECONDS);
            assertTrue(second.isConnected());
            assertNotSame(spyTopic.getSubscription(subName), subscription);
            assertEquals(admin.topics().getStats(topicName).getSubscriptions().get(subName).getConsumers().size(), 1);

            producer.send("after-reconnect");
            Message<String> message = second.receive(10, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(message.getValue(), "after-reconnect");
        } finally {
            allowRemoval.complete(null);
        }
    }

    @Test
    public void testRemoveProducerOnNonPersistentTopic() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/topic_" + UUID.randomUUID();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        NonPersistentTopic topic = (NonPersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        Field field = AbstractTopic.class.getDeclaredField("userCreatedProducerCount");
        field.setAccessible(true);
        int userCreatedProducerCount = (int) field.get(topic);
        assertEquals(userCreatedProducerCount, 1);

        producer.close();
        userCreatedProducerCount = (int) field.get(topic);
        assertEquals(userCreatedProducerCount, 0);
    }
}
