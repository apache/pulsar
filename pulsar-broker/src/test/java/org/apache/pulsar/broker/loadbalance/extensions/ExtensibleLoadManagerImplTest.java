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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelTest.overrideTableView;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Bandwidth;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.MsgRate;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Topics;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import static org.apache.pulsar.broker.namespace.NamespaceService.getHeartbeatNamespace;
import static org.apache.pulsar.broker.namespace.NamespaceService.getHeartbeatNamespaceV2;
import static org.apache.pulsar.broker.namespace.NamespaceService.getSLAMonitorNamespace;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.reporter.BrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceBundleSplitListener;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.awaitility.Awaitility;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link ExtensibleLoadManagerImpl}.
 */
@Slf4j
@Test(groups = "flaky")
@SuppressWarnings("unchecked")
public class ExtensibleLoadManagerImplTest extends ExtensibleLoadManagerImplBaseTest {

    public ExtensibleLoadManagerImplTest() {
        super("public/test");
    }

    @Test
    public void testAssignInternalTopic() throws Exception {
        Optional<BrokerLookupData> brokerLookupData1 = primaryLoadManager.assign(
                Optional.of(TopicName.get(ServiceUnitStateChannelImpl.TOPIC)),
                getBundleAsync(pulsar1, TopicName.get(ServiceUnitStateChannelImpl.TOPIC)).get(),
                LookupOptions.builder().build()).get();
        Optional<BrokerLookupData> brokerLookupData2 = secondaryLoadManager.assign(
                Optional.of(TopicName.get(ServiceUnitStateChannelImpl.TOPIC)),
                getBundleAsync(pulsar1, TopicName.get(ServiceUnitStateChannelImpl.TOPIC)).get(),
                LookupOptions.builder().build()).get();
        assertEquals(brokerLookupData1, brokerLookupData2);
        assertTrue(brokerLookupData1.isPresent());

        LeaderElectionService leaderElectionService = (LeaderElectionService)
                FieldUtils.readField(channel1, "leaderElectionService", true);
        Optional<LeaderBroker> currentLeader = leaderElectionService.getCurrentLeader();
        assertTrue(currentLeader.isPresent());
        assertEquals(brokerLookupData1.get().getWebServiceUrlTls(), currentLeader.get().getServiceUrl());
    }

    @Test
    public void testAssign() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-assign");
        TopicName topicName = topicAndBundle.getLeft();
        NamespaceBundle bundle = topicAndBundle.getRight();
        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle,
                LookupOptions.builder().build()).get();
        assertTrue(brokerLookupData.isPresent());
        log.info("Assign the bundle {} to {}", bundle, brokerLookupData);
        // Should get owner info from channel.
        Optional<BrokerLookupData> brokerLookupData1 = secondaryLoadManager.assign(Optional.empty(), bundle,
                LookupOptions.builder().build()).get();
        assertEquals(brokerLookupData, brokerLookupData1);

        Optional<LookupResult> lookupResult = pulsar2.getNamespaceService()
                .getBrokerServiceUrlAsync(topicName, LookupOptions.builder().build()).get();
        assertTrue(lookupResult.isPresent());
        assertEquals(lookupResult.get().getLookupData().getHttpUrl(), brokerLookupData.get().getWebServiceUrl());

        Optional<URL> webServiceUrl = pulsar2.getNamespaceService()
                .getWebServiceUrl(bundle, LookupOptions.builder().requestHttps(false).build());
        assertTrue(webServiceUrl.isPresent());
        assertEquals(webServiceUrl.get().toString(), brokerLookupData.get().getWebServiceUrl());
    }

    @Test
    public void testLookupOptions() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle =
                getBundleIsNotOwnByChangeEventTopic("test-lookup-options");
        TopicName topicName = topicAndBundle.getLeft();
        NamespaceBundle bundle = topicAndBundle.getRight();

        admin.topics().createPartitionedTopic(topicName.toString(), 1);

        // Test LookupOptions.readOnly = true when the bundle is not owned by any broker.
        Optional<URL> webServiceUrlReadOnlyTrue = pulsar1.getNamespaceService()
                .getWebServiceUrl(bundle, LookupOptions.builder().readOnly(true).requestHttps(false).build());
        assertTrue(webServiceUrlReadOnlyTrue.isEmpty());

        // Test LookupOptions.readOnly = false and the bundle assign to some broker.
        Optional<URL> webServiceUrlReadOnlyFalse = pulsar1.getNamespaceService()
                .getWebServiceUrl(bundle, LookupOptions.builder().readOnly(false).requestHttps(false).build());
        assertTrue(webServiceUrlReadOnlyFalse.isPresent());

        // Test LookupOptions.requestHttps = true
        Optional<URL> webServiceUrlHttps = pulsar2.getNamespaceService()
                .getWebServiceUrl(bundle, LookupOptions.builder().requestHttps(true).build());
        assertTrue(webServiceUrlHttps.isPresent());
        assertTrue(webServiceUrlHttps.get().toString().startsWith("https"));

        // TODO: Support LookupOptions.loadTopicsInBundle = true

        // Test LookupOptions.advertisedListenerName = internal but the broker do not have internal listener.
        try {
            pulsar2.getNamespaceService()
                    .getWebServiceUrl(bundle, LookupOptions.builder().advertisedListenerName("internal").build());
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("the broker do not have internal listener"));
        }
    }

    @Test
    public void testCheckOwnershipAsync() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-check-ownership");
        NamespaceBundle bundle = topicAndBundle.getRight();
        // 1. The bundle is never assigned.
        retryStrategically((test) -> {
            try {
                return !primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get()
                        && !secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get();
            } catch (Exception e) {
                return false;
            }
        }, 5, 200);
        assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());

        // 2. Assign the bundle to a broker.
        Optional<BrokerLookupData> lookupData = primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build()).get();
        assertTrue(lookupData.isPresent());
        if (lookupData.get().getPulsarServiceUrl().equals(pulsar1.getBrokerServiceUrl())) {
            assertTrue(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        } else {
            assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertTrue(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        }

    }

    @Test
    public void testFilter() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-filter");
        NamespaceBundle bundle = topicAndBundle.getRight();

        doReturn(List.of(new BrokerFilter() {
            @Override
            public String name() {
                return "Mock broker filter";
            }

            @Override
            public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                                ServiceUnitId serviceUnit,
                                                                                LoadManagerContext context) {
                brokers.remove(pulsar1.getBrokerId());
                return CompletableFuture.completedFuture(brokers);
            }

        })).when(primaryLoadManager).getBrokerFilterPipeline();

        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build()).get();
        assertTrue(brokerLookupData.isPresent());
        assertEquals(brokerLookupData.get().getWebServiceUrl(), pulsar2.getWebServiceAddress());
    }

    @Test
    public void testFilterHasException() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-filter-has-exception");
        NamespaceBundle bundle = topicAndBundle.getRight();

        doReturn(List.of(new MockBrokerFilter() {
            @Override
            public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                                ServiceUnitId serviceUnit,
                                                                                LoadManagerContext context) {
                brokers.remove(brokers.keySet().iterator().next());
                return FutureUtil.failedFuture(new BrokerFilterException("Test"));
            }
        })).when(primaryLoadManager).getBrokerFilterPipeline();

        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build()).get();
        assertTrue(brokerLookupData.isPresent());
    }

    @Test(timeOut = 30 * 1000)
    public void testUnloadUponTopicLookupFailure() throws Exception {
        TopicName topicName =
                TopicName.get("public/test/testUnloadUponTopicLookupFailure");
        NamespaceBundle bundle = pulsar1.getNamespaceService().getBundle(topicName);
        primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build()).get();

        CompletableFuture future1 = new CompletableFuture();
        CompletableFuture future2 = new CompletableFuture();
        try {
            pulsar1.getBrokerService().getTopics().put(topicName.toString(), future1);
            pulsar2.getBrokerService().getTopics().put(topicName.toString(), future2);
            CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS).execute(() -> {
                future1.completeExceptionally(new CompletionException(
                        new BrokerServiceException.ServiceUnitNotReadyException("Please redo the lookup")));
                future2.completeExceptionally(new CompletionException(
                        new BrokerServiceException.ServiceUnitNotReadyException("Please redo the lookup")));
            });
            admin.namespaces().unloadNamespaceBundle(bundle.getNamespaceObject().toString(), bundle.getBundleRange());
        } finally {
            pulsar1.getBrokerService().getTopics().remove(topicName.toString());
            pulsar2.getBrokerService().getTopics().remove(topicName.toString());
        }
    }


    @Test(timeOut = 30 * 1000)
    public void testUnloadAdminAPI() throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-unload");
        TopicName topicName = topicAndBundle.getLeft();
        NamespaceBundle bundle = topicAndBundle.getRight();

        AtomicInteger onloadCount = new AtomicInteger(0);
        AtomicInteger unloadCount = new AtomicInteger(0);

        NamespaceBundleOwnershipListener listener = new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle bundle) {
                onloadCount.incrementAndGet();
            }

            @Override
            public void unLoad(NamespaceBundle bundle) {
                unloadCount.incrementAndGet();
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.equals(bundle);
            }
        };
        pulsar1.getNamespaceService().addNamespaceBundleOwnershipListener(listener);
        pulsar2.getNamespaceService().addNamespaceBundleOwnershipListener(listener);
        String broker = admin.lookups().lookupTopic(topicName.toString());
        log.info("Assign the bundle {} to {}", bundle, broker);

        checkOwnershipState(broker, bundle);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(onloadCount.get(), 1);
            assertEquals(unloadCount.get(), 0);
        });

        admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange());
        assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(onloadCount.get(), 1);
            assertEquals(unloadCount.get(), 1);
        });

        broker = admin.lookups().lookupTopic(topicName.toString());
        log.info("Assign the bundle {} to {}", bundle, broker);

        String finalBroker = broker;
        Awaitility.await().untilAsserted(() -> {
            checkOwnershipState(finalBroker, bundle);
            assertEquals(onloadCount.get(), 2);
            assertEquals(unloadCount.get(), 1);
        });


        String dstBrokerUrl = pulsar1.getBrokerId();
        String dstBrokerServiceUrl;
        if (broker.equals(pulsar1.getBrokerServiceUrl())) {
            dstBrokerUrl = pulsar2.getBrokerId();
            dstBrokerServiceUrl = pulsar2.getBrokerServiceUrl();
        } else {
            dstBrokerServiceUrl = pulsar1.getBrokerServiceUrl();
        }
        checkOwnershipState(broker, bundle);

        admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), dstBrokerUrl);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(onloadCount.get(), 3);
            assertEquals(unloadCount.get(), 3); //one from releasing and one from owned
        });

        assertEquals(admin.lookups().lookupTopic(topicName.toString()), dstBrokerServiceUrl);

        // Test transfer to current broker.
        try {
            admin.namespaces()
                    .unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), dstBrokerUrl);
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("cannot be transfer to same broker"));
        }
    }

    @DataProvider(name = "isPersistentTopicSubscriptionTypeTest")
    public Object[][] isPersistentTopicSubscriptionTypeTest() {
        return new Object[][]{
                {TopicDomain.persistent, SubscriptionType.Exclusive},
                {TopicDomain.persistent, SubscriptionType.Shared},
                {TopicDomain.persistent, SubscriptionType.Failover},
                {TopicDomain.persistent, SubscriptionType.Key_Shared},
                {TopicDomain.non_persistent, SubscriptionType.Exclusive},
                {TopicDomain.non_persistent, SubscriptionType.Shared},
                {TopicDomain.non_persistent, SubscriptionType.Failover},
                {TopicDomain.non_persistent, SubscriptionType.Key_Shared},
        };
    }

    @Test(timeOut = 30_000, dataProvider = "isPersistentTopicSubscriptionTypeTest")
    public void testTransferClientReconnectionWithoutLookup(TopicDomain topicDomain, SubscriptionType subscriptionType)
            throws Exception {
        testTransferClientReconnectionWithoutLookup(topicDomain, subscriptionType, defaultTestNamespace, admin,
                lookupUrl.toString(), pulsar1, pulsar2, primaryLoadManager, secondaryLoadManager);
    }

    @Test(enabled = false)
    public static void testTransferClientReconnectionWithoutLookup(TopicDomain topicDomain,
                                                                   SubscriptionType subscriptionType,
                                                                   String defaultTestNamespace,
                                                                   PulsarAdmin admin, String brokerServiceUrl,
                                                                   PulsarService pulsar1, PulsarService pulsar2,
                                                                   ExtensibleLoadManager primaryLoadManager,
                                                                   ExtensibleLoadManager secondaryLoadManager)
            throws Exception {
        var id = String.format("test-tx-client-reconnect-%s-%s", subscriptionType, UUID.randomUUID());
        var topic = String.format("%s://%s/%s", topicDomain.toString(), defaultTestNamespace, id);
        var topicName = TopicName.get(topic);
        var timeoutMs = 30_000;

        var clients = new ArrayList<PulsarClient>();
        var consumers = new ArrayList<Consumer<String>>();
        try {
            var lookups = new ArrayList<LookupService>();
            var pulsarClient = pulsarClient(brokerServiceUrl, 0);
            clients.add(pulsarClient);
            @Cleanup
            var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
            lookups.add(spyLookupService(pulsarClient));

            var consumerCount = subscriptionType == SubscriptionType.Exclusive ? 1 : 3;

            for (int i = 0; i < consumerCount; i++) {
                var client = pulsarClient(brokerServiceUrl, 0);
                clients.add(client);
                var consumer = client.newConsumer(Schema.STRING).
                        subscriptionName(id).
                        subscriptionType(subscriptionType).
                        subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).
                        ackTimeout(1000, TimeUnit.MILLISECONDS).
                        topic(topic).
                        subscribe();
                consumers.add(consumer);
                lookups.add(spyLookupService(client));
            }

            Awaitility.await()
                    .until(() -> producer.isConnected() && consumers.stream().allMatch(Consumer::isConnected));

            NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get(topic)).get();
            String broker = admin.lookups().lookupTopic(topic);
            final String dstBrokerUrl;
            final String dstBrokerServiceUrl;
            if (broker.equals(pulsar1.getBrokerServiceUrl())) {
                dstBrokerUrl = pulsar2.getBrokerId();
                dstBrokerServiceUrl = pulsar2.getBrokerServiceUrl();
            } else {
                dstBrokerUrl = pulsar1.getBrokerId();
                dstBrokerServiceUrl = pulsar1.getBrokerServiceUrl();
            }
            checkOwnershipState(broker, bundle, primaryLoadManager, secondaryLoadManager, pulsar1);

            var messageCountBeforeUnloading = 100;
            var messageCountAfterUnloading = 100;
            var messageCount = messageCountBeforeUnloading + messageCountAfterUnloading;

            var semMessagesReadyToSend = new Semaphore(0);
            var cdlStart = new CountDownLatch(1);

            @Cleanup(value = "shutdown")
            var executor = Executors.newFixedThreadPool(1 /* bundle unload */ + 1 /* producer */ + consumers.size());

            var futures = new ArrayList<CompletableFuture<?>>();
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    cdlStart.await();
                    semMessagesReadyToSend.release(messageCountBeforeUnloading);
                    admin.namespaces()
                            .unloadNamespaceBundle(defaultTestNamespace, bundle.getBundleRange(), dstBrokerUrl);
                    semMessagesReadyToSend.release(messageCountAfterUnloading);
                } catch (InterruptedException | PulsarAdminException e) {
                    fail();
                }
            }, executor));

            var pendingMessages = Collections.synchronizedSet(new HashSet<>(messageCount));
            var producerFuture = CompletableFuture.runAsync(() -> {
                try {
                    cdlStart.await();
                    for (int i = 0; i < messageCount; i++) {
                        semMessagesReadyToSend.acquire();
                        String message = String.format("message-%d", i);
                        if (topicDomain == TopicDomain.persistent) {
                            // Only verify receipt of persistent topic messages.
                            pendingMessages.add(message);
                        }
                        producer.send(message);
                    }
                } catch (PulsarClientException | InterruptedException e) {
                    fail();
                }
            }, executor);
            futures.add(producerFuture);

            consumers.stream().map(consumer -> CompletableFuture.runAsync(() -> {
                try {
                    cdlStart.await();
                } catch (InterruptedException e) {
                    fail();
                }
                while (!producerFuture.isDone() || !pendingMessages.isEmpty()) {
                    try {
                        var message = consumer.receive(1500, TimeUnit.MILLISECONDS);
                        if (message != null) {
                            consumer.acknowledge(message);
                            pendingMessages.remove(message.getValue());
                        }
                    } catch (PulsarClientException e) {
                        // Retry read
                    }
                }
            }, executor)).forEach(futures::add);

            var asyncTasks = FutureUtil.waitForAllAndSupportCancel(futures).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);

            cdlStart.countDown();
            Awaitility.await().atMost(timeoutMs, TimeUnit.MILLISECONDS).ignoreExceptions().until(
                    () -> dstBrokerServiceUrl.equals(admin.lookups().lookupTopic(topic)));

            asyncTasks.get();

            assertTrue(futures.stream().allMatch(CompletableFuture::isDone));
            assertTrue(futures.stream().noneMatch(CompletableFuture::isCompletedExceptionally));
            assertTrue(pendingMessages.isEmpty());

            assertTrue(producer.isConnected());
            assertTrue(consumers.stream().allMatch(Consumer::isConnected));

            for (LookupService lookupService : lookups) {
                verify(lookupService, never()).getBroker(topicName);
            }
        } finally {
            for (var consumer: consumers) {
                consumer.close();
            }
            for (var client: clients) {
                client.close();
            }
        }
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isPersistentTopicSubscriptionTypeTest")
    public void testUnloadClientReconnectionWithLookup(TopicDomain topicDomain,
                                                       SubscriptionType subscriptionType) throws Exception {
        testUnloadClientReconnectionWithLookup(topicDomain, subscriptionType, defaultTestNamespace, admin,
                lookupUrl.toString(), pulsar1);
    }

    @Test(enabled = false)
    public static void testUnloadClientReconnectionWithLookup(TopicDomain topicDomain,
                                                              SubscriptionType subscriptionType,
                                                              String defaultTestNamespace,
                                                              PulsarAdmin admin,
                                                              String brokerServiceUrl,
                                                              PulsarService pulsar1) throws Exception {
        var id = String.format("test-unload-%s-client-reconnect-%s-%s",
                topicDomain, subscriptionType, UUID.randomUUID());
        var topic = String.format("%s://%s/%s", topicDomain, defaultTestNamespace, id);
        var topicName = TopicName.get(topic);

        var consumers = new ArrayList<Consumer<String>>();
        try {
            @Cleanup
            var pulsarClient = pulsarClient(brokerServiceUrl, 0);
            var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

            var consumerCount = subscriptionType == SubscriptionType.Exclusive ? 1 : 3;
            for (int i = 0; i < consumerCount; i++) {
                consumers.add(pulsarClient.newConsumer(Schema.STRING).
                        subscriptionName(id).subscriptionType(subscriptionType).topic(topic).subscribe());
            }
            Awaitility.await()
                    .until(() -> producer.isConnected() && consumers.stream().allMatch(Consumer::isConnected));

            var lookup = spyLookupService(pulsarClient);

            final CountDownLatch cdl = new CountDownLatch(3);

            NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get(topic)).get();
            CompletableFuture<Void> unloadNamespaceBundle = CompletableFuture.runAsync(() -> {
                try {
                    cdl.await();
                    admin.namespaces().unloadNamespaceBundle(defaultTestNamespace, bundle.getBundleRange());
                } catch (InterruptedException | PulsarAdminException e) {
                    fail();
                }
            });

            MutableInt sendCount = new MutableInt();
            Awaitility.await().atMost(20, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
                var message = String.format("message-%d", sendCount.getValue());

                boolean messageSent = false;
                while (true) {
                    var recvFutures = consumers.stream().
                            map(consumer -> consumer.receiveAsync().orTimeout(1000, TimeUnit.MILLISECONDS)).
                            collect(Collectors.toList());

                    if (!messageSent) {
                        producer.send(message);
                        messageSent = true;
                    }

                    if (topicDomain == TopicDomain.non_persistent) {
                        // No need to wait for message receipt, we're only trying to stress the consumer lookup pathway.
                        break;
                    }
                    var msg = (Message<String>) FutureUtil.waitForAny(recvFutures, __ -> true).get().get();
                    if (Objects.equals(msg.getValue(), message)) {
                        break;
                    }
                }

                cdl.countDown();
                return sendCount.incrementAndGet() == 10;
            });

            assertTrue(producer.isConnected());
            assertTrue(consumers.stream().allMatch(Consumer::isConnected));
            assertTrue(unloadNamespaceBundle.isDone());
            verify(lookup, times(1 + consumerCount)).getBroker(topicName);
        } finally {
            for (var consumer : consumers) {
                consumer.close();
            }
        }
    }

    @DataProvider(name = "isPersistentTopicTest")
    public Object[][] isPersistentTopicTest() {
        return new Object[][]{{TopicDomain.persistent}, {TopicDomain.non_persistent}};
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isPersistentTopicTest")
    public void testOptimizeUnloadDisable(TopicDomain topicDomain) throws Exception {
        testOptimizeUnloadDisable(topicDomain, defaultTestNamespace, admin, lookupUrl.toString(), pulsar1, pulsar2);
    }

    @Test(enabled = false)
    public static void testOptimizeUnloadDisable(TopicDomain topicDomain,
                                                 String defaultTestNamespace,
                                                 PulsarAdmin admin,
                                                 String brokerServiceUrl,
                                                 PulsarService pulsar1,
                                                 PulsarService pulsar2) throws Exception {
        var id = String.format("test-optimize-unload-disable-%s-%s", topicDomain, UUID.randomUUID());
        var topic = String.format("%s://%s/%s", topicDomain, defaultTestNamespace, id);
        var topicName = TopicName.get(topic);

        pulsar1.getConfig().setLoadBalancerMultiPhaseBundleUnload(false);
        pulsar2.getConfig().setLoadBalancerMultiPhaseBundleUnload(false);

        @Cleanup
        var pulsarClient = pulsarClient(brokerServiceUrl, 0);

        @Cleanup
        var producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();

        @Cleanup
        var consumer = pulsarClient.newConsumer(Schema.STRING).subscriptionName(id).topic(topic).subscribe();

        Awaitility.await().until(() -> producer.isConnected() && consumer.isConnected());

        var lookup = spyLookupService(pulsarClient);

        final CountDownLatch cdl = new CountDownLatch(3);

        NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get(topic)).get();
        var srcBrokerServiceUrl = admin.lookups().lookupTopic(topic);
        var dstBroker = srcBrokerServiceUrl.equals(pulsar1.getBrokerServiceUrl()) ? pulsar2 : pulsar1;

        CompletableFuture<Void> unloadNamespaceBundle = CompletableFuture.runAsync(() -> {
            try {
                cdl.await();
                admin.namespaces().unloadNamespaceBundle(defaultTestNamespace, bundle.getBundleRange(),
                        dstBroker.getBrokerId());
            } catch (InterruptedException | PulsarAdminException e) {
                fail();
            }
        });

        MutableInt sendCount = new MutableInt();
        Awaitility.await().atMost(20, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            var message = String.format("message-%d", sendCount.getValue());

            AtomicBoolean messageSent = new AtomicBoolean(false);
            while (true) {
                var recvFuture = consumer.receiveAsync().orTimeout(1000, TimeUnit.MILLISECONDS);
                if (!messageSent.get()) {
                    producer.sendAsync(message).thenAccept(messageId -> {
                        if (messageId != null) {
                            messageSent.set(true);
                        }
                    }).get(1000, TimeUnit.MILLISECONDS);
                }

                if (topicDomain == TopicDomain.non_persistent) {
                    // No need to wait for message receipt, we're only trying to stress the consumer lookup pathway.
                    break;
                }
                var msg = recvFuture.get();
                if (Objects.equals(msg.getValue(), message)) {
                    break;
                }
            }

            cdl.countDown();
            return sendCount.incrementAndGet() == 10;
        });

        assertTrue(producer.isConnected());
        assertTrue(consumer.isConnected());
        assertTrue(unloadNamespaceBundle.isDone());
        verify(lookup, times(2)).getBroker(topicName);
    }

    protected static LookupService spyLookupService(PulsarClient client) throws IllegalAccessException {
        LookupService svc = (LookupService) FieldUtils.readDeclaredField(client, "lookup", true);
        var lookup = spy(svc);
        FieldUtils.writeDeclaredField(client, "lookup", lookup, true);
        return lookup;
    }

    protected static void checkOwnershipState(String broker, NamespaceBundle bundle,
                                              ExtensibleLoadManager primaryLoadManager,
                                              ExtensibleLoadManager secondaryLoadManager, PulsarService pulsar1)
            throws ExecutionException, InterruptedException {
        var targetLoadManager = secondaryLoadManager;
        var otherLoadManager = primaryLoadManager;
        if (broker.equals(pulsar1.getBrokerServiceUrl())) {
            targetLoadManager = primaryLoadManager;
            otherLoadManager = secondaryLoadManager;
        }
        assertTrue(targetLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(otherLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
    }

    protected void checkOwnershipState(String broker, NamespaceBundle bundle)
            throws ExecutionException, InterruptedException {
        checkOwnershipState(broker, bundle, primaryLoadManager, secondaryLoadManager, pulsar1);
    }

    @Test(timeOut = 30 * 1000)
    public void testSplitBundleAdminAPI() throws Exception {
        final String namespace = "public/testSplitBundleAdminAPI";
        admin.namespaces().createNamespace(namespace, 1);
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("test-split");
        TopicName topicName = topicAndBundle.getLeft();
        admin.topics().createPartitionedTopic(topicName.toString(), 10);
        BundlesData bundles = admin.namespaces().getBundles(namespace);
        int numBundles = bundles.getNumBundles();
        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();

        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);

        AtomicInteger splitCount = new AtomicInteger(0);
        NamespaceBundleSplitListener namespaceBundleSplitListener = new NamespaceBundleSplitListener() {
            @Override
            public void onSplit(NamespaceBundle bundle) {
                splitCount.incrementAndGet();
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle
                        .toString()
                        .equals(String.format(namespace + "/0x%08x_0x%08x", bundleRanges.get(0), bundleRanges.get(1)));
            }
        };
        pulsar1.getNamespaceService().addNamespaceBundleSplitListener(namespaceBundleSplitListener);
        pulsar2.getNamespaceService().addNamespaceBundleSplitListener(namespaceBundleSplitListener);

        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;

        admin.namespaces().splitNamespaceBundle(namespace, firstBundle, true, null);


        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    BundlesData bundlesData = admin.namespaces().getBundles(namespace);
                    assertEquals(bundlesData.getNumBundles(), numBundles + 1);
                    String lowBundle = String.format("0x%08x", bundleRanges.get(0));
                    String midBundle = String.format("0x%08x", mid);
                    String highBundle = String.format("0x%08x", bundleRanges.get(1));
                    assertTrue(bundlesData.getBoundaries().contains(lowBundle));
                    assertTrue(bundlesData.getBoundaries().contains(midBundle));
                    assertTrue(bundlesData.getBoundaries().contains(highBundle));
                    assertEquals(splitCount.get(), 1);
                });


        // Test split bundle with invalid bundle range.
        try {
            admin.namespaces().splitNamespaceBundle(namespace, "invalid", true, null);
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("Invalid bundle range"));
        }


        // delete and retry
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    admin.namespaces().deleteNamespace(namespace);
                });
        admin.namespaces().createNamespace(namespace, 1);
        admin.namespaces().splitNamespaceBundle(namespace, firstBundle, true, null);

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    BundlesData bundlesData = admin.namespaces().getBundles(namespace);
                    assertEquals(bundlesData.getNumBundles(), numBundles + 1);
                    String lowBundle = String.format("0x%08x", bundleRanges.get(0));
                    String midBundle = String.format("0x%08x", mid);
                    String highBundle = String.format("0x%08x", bundleRanges.get(1));
                    assertTrue(bundlesData.getBoundaries().contains(lowBundle));
                    assertTrue(bundlesData.getBoundaries().contains(midBundle));
                    assertTrue(bundlesData.getBoundaries().contains(highBundle));
                    assertEquals(splitCount.get(), 2);
                });
    }

    @Test(timeOut = 30 * 1000)
    public void testSplitBundleWithSpecificPositionAdminAPI() throws Exception {
        String namespace = defaultTestNamespace;
        String topic = "persistent://" + namespace + "/test-split-with-specific-position";
        admin.topics().createPartitionedTopic(topic, 1024);
        BundlesData bundles = admin.namespaces().getBundles(namespace);
        int numBundles = bundles.getNumBundles();

        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();

        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);

        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;
        long splitPosition = mid + 100;

        admin.namespaces().splitNamespaceBundle(namespace, firstBundle, true,
                "specified_positions_divide", List.of(bundleRanges.get(0), bundleRanges.get(1), splitPosition));

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        Awaitility.waitAtMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(bundlesData.getNumBundles(), numBundles + 1));
        String lowBundle = String.format("0x%08x", bundleRanges.get(0));
        String midBundle = String.format("0x%08x", splitPosition);
        String highBundle = String.format("0x%08x", bundleRanges.get(1));
        assertTrue(bundlesData.getBoundaries().contains(lowBundle));
        assertTrue(bundlesData.getBoundaries().contains(midBundle));
        assertTrue(bundlesData.getBoundaries().contains(highBundle));
    }
    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespaceBundle() throws Exception {
        final String namespace = "public/testDeleteNamespaceBundle";
        admin.namespaces().createNamespace(namespace, 3);
        TopicName topicName = TopicName.get(namespace + "/test-delete-namespace-bundle");

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
                    String broker = admin.lookups().lookupTopic(topicName.toString());
                    log.info("Assign the bundle {} to {}", bundle, broker);
                    checkOwnershipState(broker, bundle);
                    admin.namespaces().deleteNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), true);
                    // this could fail if the system topic lookup asynchronously happens before this.
                    // we will retry if it fails.
                    assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
                });

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> admin.namespaces().deleteNamespace(namespace, true));
    }

    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespace() throws Exception {
        String namespace = "public/test-delete-namespace";
        TopicName topicName = TopicName.get(namespace + "/test-delete-namespace-topic");
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(this.conf.getClusterName()));
        assertTrue(admin.namespaces().getNamespaces("public").contains(namespace));
        admin.topics().createPartitionedTopic(topicName.toString(), 2);
        admin.lookups().lookupTopic(topicName.toString());
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
        try {
            admin.namespaces().deleteNamespaceBundle(namespace, bundle.getBundleRange());
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Cannot delete non empty bundle"));
        }
        admin.namespaces().deleteNamespaceBundle(namespace, bundle.getBundleRange(), true);
        admin.lookups().lookupTopic(topicName.toString());

        admin.namespaces().deleteNamespace(namespace, true);
        assertFalse(admin.namespaces().getNamespaces("public").contains(namespace));
    }

    @Test(timeOut = 30 * 1000)
    public void testCheckOwnershipPresentWithSystemNamespace() throws Exception {
        NamespaceBundle namespaceBundle =
                getBundleAsync(pulsar1, TopicName.get(NamespaceName.SYSTEM_NAMESPACE + "/test")).get();
        try {
            pulsar1.getNamespaceService().checkOwnershipPresent(namespaceBundle);
        } catch (Exception ex) {
            log.info("Got exception", ex);
            assertTrue(ex.getCause() instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testMoreThenOneFilter() throws Exception {
        // Use a different namespace to avoid flaky test failures
        // from unloading the default namespace and the following topic policy lookups at the init state step
        Pair<TopicName, NamespaceBundle> topicAndBundle =
                getBundleIsNotOwnByChangeEventTopic("test-filter-has-exception");
        TopicName topicName = topicAndBundle.getLeft();
        NamespaceBundle bundle = topicAndBundle.getRight();

        String brokerId1 = pulsar1.getBrokerId();
        doReturn(List.of(new MockBrokerFilter() {
            @Override
            public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                                ServiceUnitId serviceUnit,
                                                                                LoadManagerContext context) {
                brokers.remove(brokerId1);
                return CompletableFuture.completedFuture(brokers);
            }
        },new MockBrokerFilter() {
            @Override
            public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                                ServiceUnitId serviceUnit,
                                                                                LoadManagerContext context) {
                return FutureUtil.failedFuture(new BrokerFilterException("Test"));
            }
        })).when(primaryLoadManager).getBrokerFilterPipeline();
        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build()).get();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(brokerLookupData.isPresent());
            assertEquals(brokerLookupData.get().getWebServiceUrl(), pulsar2.getWebServiceAddress());
            assertEquals(brokerLookupData.get().getPulsarServiceUrl(),
                    pulsar1.getAdminClient().lookups().lookupTopic(topicName.toString()));
            assertEquals(brokerLookupData.get().getPulsarServiceUrl(),
                    pulsar2.getAdminClient().lookups().lookupTopic(topicName.toString()));
        });
    }

    @Test
    public void testDeployAndRollbackLoadManager() throws Exception {
            // Test rollback to modular load manager.
            ServiceConfiguration defaultConf = getDefaultConf();
            defaultConf.setAllowAutoTopicCreation(true);
            defaultConf.setForceDeleteNamespaceAllowed(true);
            defaultConf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
            defaultConf.setLoadBalancerSheddingEnabled(false);
            try (var additionalPulsarTestContext = createAdditionalPulsarTestContext(defaultConf)) {
                // start pulsar3 with old load manager
                var pulsar3 = additionalPulsarTestContext.getPulsarService();
                Pair<TopicName, NamespaceBundle> topicAndBundle =
                        getBundleIsNotOwnByChangeEventTopic("testDeployAndRollbackLoadManager");
                TopicName topicName = topicAndBundle.getLeft();
                NamespaceBundle bundle = topicAndBundle.getRight();
                String topic = topicName.toString();

                String lookupResult1 = pulsar3.getAdminClient().lookups().lookupTopic(topic);
                assertEquals(lookupResult1, pulsar3.getBrokerServiceUrl());

                String lookupResult2 = pulsar1.getAdminClient().lookups().lookupTopic(topic);
                String lookupResult3 = pulsar2.getAdminClient().lookups().lookupTopic(topic);
                assertEquals(lookupResult1, lookupResult2);
                assertEquals(lookupResult1, lookupResult3);

                LookupOptions options = LookupOptions.builder()
                        .authoritative(false)
                        .requestHttps(false)
                        .readOnly(false)
                        .loadTopicsInBundle(false).build();
                Optional<URL> webServiceUrl1 =
                        pulsar1.getNamespaceService().getWebServiceUrl(bundle, options);
                assertTrue(webServiceUrl1.isPresent());
                assertEquals(webServiceUrl1.get().toString(), pulsar3.getWebServiceAddress());

                Optional<URL> webServiceUrl2 =
                        pulsar2.getNamespaceService().getWebServiceUrl(bundle, options);
                assertTrue(webServiceUrl2.isPresent());
                assertEquals(webServiceUrl2.get().toString(), webServiceUrl1.get().toString());

                Optional<URL> webServiceUrl3 =
                        pulsar3.getNamespaceService().getWebServiceUrl(bundle, options);
                assertTrue(webServiceUrl3.isPresent());
                assertEquals(webServiceUrl3.get().toString(), webServiceUrl1.get().toString());

                List<PulsarService> pulsarServices = List.of(pulsar1, pulsar2, pulsar3);
                for (PulsarService pulsarService : pulsarServices) {
                    // Test lookup heartbeat namespace's topic
                    for (PulsarService pulsar : pulsarServices) {
                        assertLookupHeartbeatOwner(pulsarService,
                                pulsar.getBrokerId(), pulsar.getBrokerServiceUrl());
                    }
                    // Test lookup SLA namespace's topic
                    for (PulsarService pulsar : pulsarServices) {
                        assertLookupSLANamespaceOwner(pulsarService,
                                pulsar.getBrokerId(), pulsar.getBrokerServiceUrl());
                    }
                }

                // Test deploy new broker with new load manager
                ServiceConfiguration conf = getDefaultConf();
                conf.setAllowAutoTopicCreation(true);
                conf.setForceDeleteNamespaceAllowed(true);
                conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
                conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
                try (var additionPulsarTestContext = createAdditionalPulsarTestContext(conf)) {
                    var pulsar4 = additionPulsarTestContext.getPulsarService();

                    Set<String> availableCandidates = Sets.newHashSet(pulsar1.getBrokerServiceUrl(),
                            pulsar2.getBrokerServiceUrl(),
                            pulsar4.getBrokerServiceUrl());
                    String lookupResult4 = pulsar4.getAdminClient().lookups().lookupTopic(topic);
                    assertTrue(availableCandidates.contains(lookupResult4));

                    String lookupResult5 = pulsar1.getAdminClient().lookups().lookupTopic(topic);
                    String lookupResult6 = pulsar2.getAdminClient().lookups().lookupTopic(topic);
                    String lookupResult7 = pulsar3.getAdminClient().lookups().lookupTopic(topic);
                    assertEquals(lookupResult4, lookupResult5);
                    assertEquals(lookupResult4, lookupResult6);
                    assertEquals(lookupResult4, lookupResult7);

                    Set<String> availableWebUrlCandidates = Sets.newHashSet(pulsar1.getWebServiceAddress(),
                            pulsar2.getWebServiceAddress(),
                            pulsar4.getWebServiceAddress());

                    webServiceUrl1 =
                            pulsar1.getNamespaceService().getWebServiceUrl(bundle, options);
                    assertTrue(webServiceUrl1.isPresent());
                    assertTrue(availableWebUrlCandidates.contains(webServiceUrl1.get().toString()));

                    webServiceUrl2 =
                            pulsar2.getNamespaceService().getWebServiceUrl(bundle, options);
                    assertTrue(webServiceUrl2.isPresent());
                    assertEquals(webServiceUrl2.get().toString(), webServiceUrl1.get().toString());

                    // The pulsar3 will redirect to pulsar4
                    webServiceUrl3 =
                            pulsar3.getNamespaceService().getWebServiceUrl(bundle, options);
                    assertTrue(webServiceUrl3.isPresent());
                    // It will redirect to pulsar4
                    assertTrue(availableWebUrlCandidates.contains(webServiceUrl3.get().toString()));

                    var webServiceUrl4 =
                            pulsar4.getNamespaceService().getWebServiceUrl(bundle, options);
                    assertTrue(webServiceUrl4.isPresent());
                    assertEquals(webServiceUrl4.get().toString(), webServiceUrl1.get().toString());

                    pulsarServices = List.of(pulsar1, pulsar2, pulsar3, pulsar4);
                    for (PulsarService pulsarService : pulsarServices) {
                        // Test lookup heartbeat namespace's topic
                        for (PulsarService pulsar : pulsarServices) {
                            assertLookupHeartbeatOwner(pulsarService,
                                    pulsar.getBrokerId(), pulsar.getBrokerServiceUrl());
                        }
                        // Test lookup SLA namespace's topic
                        for (PulsarService pulsar : pulsarServices) {
                            assertLookupSLANamespaceOwner(pulsarService,
                                    pulsar.getBrokerId(), pulsar.getBrokerServiceUrl());
                        }
                    }
                    // Check if the broker is available
                    var wrapper = (ExtensibleLoadManagerWrapper) pulsar4.getLoadManager().get();
                    var loadManager4 = spy((ExtensibleLoadManagerImpl)
                            FieldUtils.readField(wrapper, "loadManager", true));
                    loadManager4.getBrokerRegistry().unregister();

                    NamespaceName slaMonitorNamespace =
                            getSLAMonitorNamespace(pulsar4.getBrokerId(), pulsar.getConfiguration());
                    String slaMonitorTopic = slaMonitorNamespace.getPersistentTopicName("test");
                    String result = pulsar.getAdminClient().lookups().lookupTopic(slaMonitorTopic);
                    assertNotNull(result);
                    log.info("{} Namespace is re-owned by {}", slaMonitorTopic, result);
                    assertNotEquals(result, pulsar4.getBrokerServiceUrl());

                    Producer<String> producer = pulsar.getClient().newProducer(Schema.STRING).topic(slaMonitorTopic).create();
                    producer.send("t1");

                    // Test re-register broker and check the lookup result
                    loadManager4.getBrokerRegistry().register();

                    result = pulsar.getAdminClient().lookups().lookupTopic(slaMonitorTopic);
                    assertNotNull(result);
                    log.info("{} Namespace is re-owned by {}", slaMonitorTopic, result);
                    assertEquals(result, pulsar4.getBrokerServiceUrl());

                    producer.send("t2");
                    Producer<String> producer1 = pulsar.getClient().newProducer(Schema.STRING).topic(slaMonitorTopic).create();
                    producer1.send("t3");

                    producer.close();
                    producer1.close();
                    @Cleanup
                    Consumer<String> consumer = pulsar.getClient().newConsumer(Schema.STRING)
                            .topic(slaMonitorTopic)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .subscriptionName("test")
                            .subscribe();
                    // receive message t1 t2 t3
                    assertEquals(consumer.receive().getValue(), "t1");
                    assertEquals(consumer.receive().getValue(), "t2");
                    assertEquals(consumer.receive().getValue(), "t3");
                }
            }
    }

    private void assertLookupHeartbeatOwner(PulsarService pulsar,
                                            String brokerId,
                                            String expectedBrokerServiceUrl) throws Exception {
        NamespaceName heartbeatNamespaceV1 =
                getHeartbeatNamespace(brokerId, pulsar.getConfiguration());

        String heartbeatV1Topic = heartbeatNamespaceV1.getPersistentTopicName("test");
        assertEquals(pulsar.getAdminClient().lookups().lookupTopic(heartbeatV1Topic), expectedBrokerServiceUrl);

        NamespaceName heartbeatNamespaceV2 =
                getHeartbeatNamespaceV2(brokerId, pulsar.getConfiguration());

        String heartbeatV2Topic = heartbeatNamespaceV2.getPersistentTopicName("test");
        assertEquals(pulsar.getAdminClient().lookups().lookupTopic(heartbeatV2Topic), expectedBrokerServiceUrl);
    }

    private void assertLookupSLANamespaceOwner(PulsarService pulsar,
                                               String brokerId,
                                               String expectedBrokerServiceUrl) throws Exception {
        NamespaceName slaMonitorNamespace = getSLAMonitorNamespace(brokerId, pulsar.getConfiguration());
        String slaMonitorTopic = slaMonitorNamespace.getPersistentTopicName("test");
        String result = pulsar.getAdminClient().lookups().lookupTopic(slaMonitorTopic);
        log.info("Topic {} Lookup result: {}", slaMonitorTopic, result);
        assertNotNull(result);
        assertEquals(result, expectedBrokerServiceUrl);
    }

    @Test(priority = 10)
    public void testTopBundlesLoadDataStoreTableViewFromChannelOwner() throws Exception {
        var topBundlesLoadDataStorePrimary =
                (LoadDataStore) FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", true);
        var serviceUnitStateChannelPrimary =
                (ServiceUnitStateChannelImpl) FieldUtils.readDeclaredField(primaryLoadManager,
                        "serviceUnitStateChannel", true);
        var tvPrimary =
                (TableViewImpl) FieldUtils.readDeclaredField(topBundlesLoadDataStorePrimary, "tableView", true);

        var topBundlesLoadDataStoreSecondary =
                (LoadDataStore) FieldUtils.readDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", true);
        var tvSecondary =
                (TableViewImpl) FieldUtils.readDeclaredField(topBundlesLoadDataStoreSecondary, "tableView", true);

        if (serviceUnitStateChannelPrimary.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
            assertNotNull(tvPrimary);
            assertNull(tvSecondary);
        } else {
            assertNull(tvPrimary);
            assertNotNull(tvSecondary);
        }

        restartBroker();
        pulsar1 = pulsar;
        setPrimaryLoadManager();
        admin.namespaces().setNamespaceReplicationClusters(defaultTestNamespace,
                Sets.newHashSet(this.conf.getClusterName()));

        var serviceUnitStateChannelPrimaryNew =
                (ServiceUnitStateChannelImpl) FieldUtils.readDeclaredField(primaryLoadManager,
                        "serviceUnitStateChannel", true);
        var topBundlesLoadDataStorePrimaryNew =
                (LoadDataStore) FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore"
                        , true);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertFalse(serviceUnitStateChannelPrimaryNew.isChannelOwnerAsync().get(5, TimeUnit.SECONDS));
                    assertNotNull(FieldUtils.readDeclaredField(topBundlesLoadDataStoreSecondary, "tableView"
                            , true));
                    assertNull(FieldUtils.readDeclaredField(topBundlesLoadDataStorePrimaryNew, "tableView"
                            , true));
                }
        );
    }

    @Test
    public void testRoleChange() throws Exception {
        var topBundlesLoadDataStorePrimary = (LoadDataStore<TopBundlesLoadData>)
                FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", true);
        var topBundlesLoadDataStorePrimarySpy = spy(topBundlesLoadDataStorePrimary);
        AtomicInteger countPri = new AtomicInteger(3);
        AtomicInteger countPri2 = new AtomicInteger(3);
        doAnswer(invocationOnMock -> {
            if (countPri.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStorePrimarySpy).startTableView();
        doAnswer(invocationOnMock -> {
            if (countPri2.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStorePrimarySpy).closeTableView();

        var topBundlesLoadDataStoreSecondary = (LoadDataStore<TopBundlesLoadData>)
                FieldUtils.readDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", true);
        var topBundlesLoadDataStoreSecondarySpy = spy(topBundlesLoadDataStoreSecondary);
        AtomicInteger countSec = new AtomicInteger(3);
        AtomicInteger countSec2 = new AtomicInteger(3);
        doAnswer(invocationOnMock -> {
            if (countSec.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStoreSecondarySpy).startTableView();
        doAnswer(invocationOnMock -> {
            if (countSec2.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStoreSecondarySpy).closeTableView();

        try {
            FieldUtils.writeDeclaredField(primaryLoadManager, "topBundlesLoadDataStore",
                    topBundlesLoadDataStorePrimarySpy, true);
            FieldUtils.writeDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore",
                    topBundlesLoadDataStoreSecondarySpy, true);


            if (channel1.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
                primaryLoadManager.playLeader();
                secondaryLoadManager.playFollower();
                verify(topBundlesLoadDataStorePrimarySpy, times(3)).startTableView();
                verify(topBundlesLoadDataStorePrimarySpy, times(5)).closeTableView();
                verify(topBundlesLoadDataStoreSecondarySpy, times(0)).startTableView();
                verify(topBundlesLoadDataStoreSecondarySpy, times(3)).closeTableView();
            } else {
                primaryLoadManager.playFollower();
                secondaryLoadManager.playLeader();
                verify(topBundlesLoadDataStoreSecondarySpy, times(3)).startTableView();
                verify(topBundlesLoadDataStoreSecondarySpy, times(5)).closeTableView();
                verify(topBundlesLoadDataStorePrimarySpy, times(0)).startTableView();
                verify(topBundlesLoadDataStorePrimarySpy, times(3)).closeTableView();
            }

            primaryLoadManager.playFollower();
            secondaryLoadManager.playFollower();

            if (channel1.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
                assertEquals(ExtensibleLoadManagerImpl.Role.Leader,
                        FieldUtils.readDeclaredField(primaryLoadManager, "role", true));
                assertEquals(ExtensibleLoadManagerImpl.Role.Follower,
                        FieldUtils.readDeclaredField(secondaryLoadManager, "role", true));
            } else {
                assertEquals(ExtensibleLoadManagerImpl.Role.Follower,
                        FieldUtils.readDeclaredField(primaryLoadManager, "role", true));
                assertEquals(ExtensibleLoadManagerImpl.Role.Leader,
                        FieldUtils.readDeclaredField(secondaryLoadManager, "role", true));
            }

            primaryLoadManager.playLeader();
            secondaryLoadManager.playLeader();

            if (channel1.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
                assertEquals(ExtensibleLoadManagerImpl.Role.Leader,
                        FieldUtils.readDeclaredField(primaryLoadManager, "role", true));
                assertEquals(ExtensibleLoadManagerImpl.Role.Follower,
                        FieldUtils.readDeclaredField(secondaryLoadManager, "role", true));
            } else {
                assertEquals(ExtensibleLoadManagerImpl.Role.Follower,
                        FieldUtils.readDeclaredField(primaryLoadManager, "role", true));
                assertEquals(ExtensibleLoadManagerImpl.Role.Leader,
                        FieldUtils.readDeclaredField(secondaryLoadManager, "role", true));
            }
        } finally {
            FieldUtils.writeDeclaredField(primaryLoadManager, "topBundlesLoadDataStore",
                    topBundlesLoadDataStorePrimary, true);
            FieldUtils.writeDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore",
                    topBundlesLoadDataStoreSecondary, true);
        }
    }

    @Test
    public void testGetMetrics() throws Exception {
        {
            ServiceConfiguration conf = getDefaultConf();
            conf.setLoadBalancerMemoryResourceWeight(1);
            var brokerLoadDataReporter = mock(BrokerLoadDataReporter.class);
            FieldUtils.writeDeclaredField(primaryLoadManager, "brokerLoadDataReporter", brokerLoadDataReporter, true);
            BrokerLoadData loadData = new BrokerLoadData();
            SystemResourceUsage usage = new SystemResourceUsage();
            var cpu = new ResourceUsage(1.0, 100.0);
            var memory = new ResourceUsage(800.0, 200.0);
            var directMemory = new ResourceUsage(2.0, 100.0);
            var bandwidthIn = new ResourceUsage(3.0, 100.0);
            var bandwidthOut = new ResourceUsage(4.0, 100.0);
            usage.setCpu(cpu);
            usage.setMemory(memory);
            usage.setDirectMemory(directMemory);
            usage.setBandwidthIn(bandwidthIn);
            usage.setBandwidthOut(bandwidthOut);
            loadData.update(usage, 1, 2, 3, 4, 5, 6, conf);
            doReturn(loadData).when(brokerLoadDataReporter).generateLoadData();
        }
        {
            var unloadMetrics = (AtomicReference<List<Metrics>>)
                    FieldUtils.readDeclaredField(primaryLoadManager, "unloadMetrics", true);
            UnloadCounter unloadCounter = new UnloadCounter();
            FieldUtils.writeDeclaredField(unloadCounter, "unloadBrokerCount", 2l, true);
            FieldUtils.writeDeclaredField(unloadCounter, "unloadBundleCount", 3l, true);
            FieldUtils.writeDeclaredField(unloadCounter, "loadAvg", 1.5, true);
            FieldUtils.writeDeclaredField(unloadCounter, "loadStd", 0.3, true);
            FieldUtils.writeDeclaredField(unloadCounter, "breakdownCounters", Map.of(
                    Success, new LinkedHashMap<>() {{
                        put(Overloaded, new AtomicLong(1));
                        put(Underloaded, new AtomicLong(2));
                    }},
                    Skip, new LinkedHashMap<>() {{
                        put(HitCount, new AtomicLong(3));
                        put(NoBundles, new AtomicLong(4));
                        put(CoolDown, new AtomicLong(5));
                        put(OutDatedData, new AtomicLong(6));
                        put(NoLoadData, new AtomicLong(7));
                        put(NoBrokers, new AtomicLong(8));
                        put(Unknown, new AtomicLong(9));
                    }},
                    Failure, Map.of(
                            Unknown, new AtomicLong(10))
            ), true);
            unloadMetrics.set(unloadCounter.toMetrics(pulsar.getAdvertisedAddress()));
        }
        {
            var splitMetrics = (AtomicReference<List<Metrics>>)
                    FieldUtils.readDeclaredField(primaryLoadManager, "splitMetrics", true);
            SplitCounter splitCounter = new SplitCounter();
            FieldUtils.writeDeclaredField(splitCounter, "splitCount", 35l, true);
            FieldUtils.writeDeclaredField(splitCounter, "breakdownCounters", Map.of(
                    SplitDecision.Label.Success, Map.of(
                            Topics, new AtomicLong(1),
                            Sessions, new AtomicLong(2),
                            MsgRate, new AtomicLong(3),
                            Bandwidth, new AtomicLong(4),
                            Admin, new AtomicLong(5)),
                    SplitDecision.Label.Failure, Map.of(
                            SplitDecision.Reason.Unknown, new AtomicLong(6))
            ), true);
            splitMetrics.set(splitCounter.toMetrics(pulsar.getAdvertisedAddress()));
        }

        {
            AssignCounter assignCounter = new AssignCounter();
            assignCounter.incrementSuccess();
            assignCounter.incrementFailure();
            assignCounter.incrementFailure();
            assignCounter.incrementSkip();
            assignCounter.incrementSkip();
            assignCounter.incrementSkip();
            FieldUtils.writeDeclaredField(primaryLoadManager, "assignCounter", assignCounter, true);
        }

        {
            FieldUtils.writeDeclaredField(channel1, "lastOwnedServiceUnitCountAt", System.currentTimeMillis(), true);
            FieldUtils.writeDeclaredField(channel1, "totalOwnedServiceUnitCnt", 10, true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupCnt", 1, true);
            FieldUtils.writeDeclaredField(channel1, "totalServiceUnitTombstoneCleanupCnt", 2, true);
            FieldUtils.writeDeclaredField(channel1, "totalOrphanServiceUnitCleanupCnt", 3, true);
            FieldUtils.writeDeclaredField(channel1, "totalCleanupErrorCnt", new AtomicLong(4), true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupScheduledCnt", 5, true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupIgnoredCnt", 6, true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupCancelledCnt", 7, true);

            Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters> ownerLookUpCounters = new LinkedHashMap<>();
            Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters> handlerCounters = new LinkedHashMap<>();
            Map<ServiceUnitStateChannelImpl.EventType, ServiceUnitStateChannelImpl.Counters> eventCounters =
                    new LinkedHashMap<>();
            int j = 0;
            for (var state : ServiceUnitState.values()) {
                ownerLookUpCounters.put(state,
                        new ServiceUnitStateChannelImpl.Counters(
                                new AtomicLong(j + 1), new AtomicLong(j + 2)));
                handlerCounters.put(state,
                        new ServiceUnitStateChannelImpl.Counters(
                                new AtomicLong(j + 1), new AtomicLong(j + 2)));
                j += 2;
            }
            int i = 0;
            for (var type : ServiceUnitStateChannelImpl.EventType.values()) {
                eventCounters.put(type,
                        new ServiceUnitStateChannelImpl.Counters(
                                new AtomicLong(i + 1), new AtomicLong(i + 2)));
                i += 2;
            }
            FieldUtils.writeDeclaredField(channel1, "ownerLookUpCounters", ownerLookUpCounters, true);
            FieldUtils.writeDeclaredField(channel1, "eventCounters", eventCounters, true);
            FieldUtils.writeDeclaredField(channel1, "handlerCounters", handlerCounters, true);
        }

        primaryLoadManager.getIgnoredSendMsgCount().incrementAndGet();
        primaryLoadManager.getIgnoredSendMsgCount().incrementAndGet();
        primaryLoadManager.getIgnoredAckCount().incrementAndGet();
        primaryLoadManager.getIgnoredAckCount().incrementAndGet();
        primaryLoadManager.getIgnoredAckCount().incrementAndGet();

        var expected = Set.of(
                """
                        dimensions=[{broker=localhost, metric=loadBalancing}], metrics=[{brk_lb_bandwidth_in_usage=3.0, brk_lb_bandwidth_out_usage=4.0, brk_lb_cpu_usage=1.0, brk_lb_directMemory_usage=2.0, brk_lb_memory_usage=400.0}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=loadBalancing}], metrics=[{brk_lb_resource_usage=4.0}]
                        dimensions=[{broker=localhost, feature=max, metric=loadBalancing}], metrics=[{brk_lb_resource_usage=0.04}]
                        dimensions=[{broker=localhost, metric=bundleUnloading}], metrics=[{brk_lb_unload_broker_total=2, brk_lb_unload_bundle_total=3}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Unknown, result=Failure}], metrics=[{brk_lb_unload_broker_breakdown_total=10}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=HitCount, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoBundles, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=4}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=CoolDown, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=5}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=OutDatedData, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=6}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoLoadData, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=7}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoBrokers, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=8}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Unknown, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=9}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Overloaded, result=Success}], metrics=[{brk_lb_unload_broker_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Underloaded, result=Success}], metrics=[{brk_lb_unload_broker_breakdown_total=2}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=bundleUnloading, stat=avg}], metrics=[{brk_lb_resource_usage_stats=1.5}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=bundleUnloading, stat=std}], metrics=[{brk_lb_resource_usage_stats=0.3}]
                        dimensions=[{broker=localhost, metric=bundlesSplit}], metrics=[{brk_lb_bundles_split_total=35}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Topics, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Sessions, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=2}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=MsgRate, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Bandwidth, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=4}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Admin, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=5}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Unknown, result=Failure}], metrics=[{brk_lb_bundles_split_breakdown_total=6}]
                        dimensions=[{broker=localhost, metric=assign, result=Failure}], metrics=[{brk_lb_assign_broker_breakdown_total=2}]
                        dimensions=[{broker=localhost, metric=assign, result=Skip}], metrics=[{brk_lb_assign_broker_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=assign, result=Success}], metrics=[{brk_lb_assign_broker_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Init}], metrics=[{brk_sunit_state_chn_owner_lookup_total=1}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Init}], metrics=[{brk_sunit_state_chn_owner_lookup_total=2}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Free}], metrics=[{brk_sunit_state_chn_owner_lookup_total=3}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Free}], metrics=[{brk_sunit_state_chn_owner_lookup_total=4}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Owned}], metrics=[{brk_sunit_state_chn_owner_lookup_total=5}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Owned}], metrics=[{brk_sunit_state_chn_owner_lookup_total=6}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Assigning}], metrics=[{brk_sunit_state_chn_owner_lookup_total=7}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Assigning}], metrics=[{brk_sunit_state_chn_owner_lookup_total=8}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Releasing}], metrics=[{brk_sunit_state_chn_owner_lookup_total=9}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Releasing}], metrics=[{brk_sunit_state_chn_owner_lookup_total=10}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Splitting}], metrics=[{brk_sunit_state_chn_owner_lookup_total=11}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Splitting}], metrics=[{brk_sunit_state_chn_owner_lookup_total=12}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Total, state=Deleted}], metrics=[{brk_sunit_state_chn_owner_lookup_total=13}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure, state=Deleted}], metrics=[{brk_sunit_state_chn_owner_lookup_total=14}]
                        dimensions=[{broker=localhost, event=Assign, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=1}]
                        dimensions=[{broker=localhost, event=Assign, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=2}]
                        dimensions=[{broker=localhost, event=Split, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=3}]
                        dimensions=[{broker=localhost, event=Split, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=4}]
                        dimensions=[{broker=localhost, event=Unload, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=5}]
                        dimensions=[{broker=localhost, event=Unload, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=6}]
                        dimensions=[{broker=localhost, event=Override, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=7}]
                        dimensions=[{broker=localhost, event=Override, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=8}]
                        dimensions=[{broker=localhost, event=Init, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=1}]
                        dimensions=[{broker=localhost, event=Init, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=2}]
                        dimensions=[{broker=localhost, event=Free, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=3}]
                        dimensions=[{broker=localhost, event=Free, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=4}]
                        dimensions=[{broker=localhost, event=Owned, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=5}]
                        dimensions=[{broker=localhost, event=Owned, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=6}]
                        dimensions=[{broker=localhost, event=Assigning, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=7}]
                        dimensions=[{broker=localhost, event=Assigning, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=8}]
                        dimensions=[{broker=localhost, event=Releasing, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=9}]
                        dimensions=[{broker=localhost, event=Releasing, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=10}]
                        dimensions=[{broker=localhost, event=Splitting, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=11}]
                        dimensions=[{broker=localhost, event=Splitting, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=12}]
                        dimensions=[{broker=localhost, event=Deleted, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=13}]
                        dimensions=[{broker=localhost, event=Deleted, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=14}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_cleanup_ops_total=4}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Skip}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=6}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Cancel}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=7}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Schedule}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=5}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Success}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=1}]
                        dimensions=[{broker=localhost, metric=sunitStateChn}], metrics=[{brk_sunit_state_chn_orphan_su_cleanup_ops_total=3, brk_sunit_state_chn_owned_su_total=10, brk_sunit_state_chn_su_tombstone_cleanup_ops_total=2}]
                        dimensions=[{broker=localhost, metric=bundleUnloading}], metrics=[{brk_lb_ignored_ack_total=3, brk_lb_ignored_send_total=2}]
                        """.split("\n"));
        var actual = primaryLoadManager.getMetrics().stream().map(Metrics::toString).collect(Collectors.toSet());
        assertEquals(actual, expected);
    }

    @Test
    public void testDisableBroker() throws Exception {
        // Test rollback to modular load manager.
        ServiceConfiguration defaultConf = getDefaultConf();
        defaultConf.setAllowAutoTopicCreation(true);
        defaultConf.setForceDeleteNamespaceAllowed(true);
        defaultConf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        defaultConf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        defaultConf.setLoadBalancerSheddingEnabled(false);
        defaultConf.setLoadBalancerDebugModeEnabled(true);
        defaultConf.setTopicLevelPoliciesEnabled(false);
        try (var additionalPulsarTestContext = createAdditionalPulsarTestContext(defaultConf)) {
            var pulsar3 = additionalPulsarTestContext.getPulsarService();
            ExtensibleLoadManagerImpl ternaryLoadManager = spy((ExtensibleLoadManagerImpl)
                    FieldUtils.readField(pulsar3.getLoadManager().get(), "loadManager", true));
            String topic = "persistent://" + defaultTestNamespace +"/test";

            String lookupResult1 = pulsar3.getAdminClient().lookups().lookupTopic(topic);
            TopicName topicName = TopicName.get(topic);
            NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
            if (!pulsar3.getBrokerServiceUrl().equals(lookupResult1)) {
                admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(),
                        pulsar3.getBrokerId());
                lookupResult1 = pulsar2.getAdminClient().lookups().lookupTopic(topic);
            }
            String lookupResult2 = pulsar1.getAdminClient().lookups().lookupTopic(topic);
            String lookupResult3 = pulsar2.getAdminClient().lookups().lookupTopic(topic);

            assertEquals(lookupResult1, pulsar3.getBrokerServiceUrl());
            assertEquals(lookupResult1, lookupResult2);
            assertEquals(lookupResult1, lookupResult3);


            assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertTrue(ternaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());

            ternaryLoadManager.disableBroker();

            assertFalse(ternaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            if (primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get()) {
                assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            } else {
                assertTrue(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            }
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testListTopic() throws Exception {
        final String namespace = "public/testListTopic";
        admin.namespaces().createNamespace(namespace, 9);

        final String persistentTopicName = TopicName.get(
                "persistent", NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID()).toString();

        final String nonPersistentTopicName = TopicName.get(
                "non-persistent", NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID()).toString();
        admin.topics().createPartitionedTopic(persistentTopicName, 9);
        admin.topics().createPartitionedTopic(nonPersistentTopicName, 9);
        pulsarClient.newProducer().topic(persistentTopicName).create().close();
        pulsarClient.newProducer().topic(nonPersistentTopicName).create().close();

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        List<String> boundaries = bundlesData.getBoundaries();
        int topicNum = 0;
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            List<String> topic = admin.topics().getListInBundle(namespace, bundle);
            if (topic == null) {
                continue;
            }
            topicNum += topic.size();
            for (String s : topic) {
                assertFalse(TopicName.get(s).isPersistent());
            }
        }
        assertEquals(topicNum, 9);

        List<String> list = admin.topics().getList(namespace);
        assertEquals(list.size(), 18);
        admin.namespaces().deleteNamespace(namespace, true);
    }

    @Test(timeOut = 30 * 1000, priority = -1)
    public void testGetOwnedServiceUnitsAndGetOwnedNamespaceStatus() throws Exception {
        NamespaceName heartbeatNamespacePulsar1V1 =
                getHeartbeatNamespace(pulsar1.getBrokerId(), pulsar1.getConfiguration());
        NamespaceName heartbeatNamespacePulsar1V2 =
                NamespaceService.getHeartbeatNamespaceV2(pulsar1.getBrokerId(), pulsar1.getConfiguration());

        NamespaceName heartbeatNamespacePulsar2V1 =
                getHeartbeatNamespace(pulsar2.getBrokerId(), pulsar2.getConfiguration());
        NamespaceName heartbeatNamespacePulsar2V2 =
                NamespaceService.getHeartbeatNamespaceV2(pulsar2.getBrokerId(), pulsar2.getConfiguration());

        NamespaceName slaMonitorNamespacePulsar1 =
                getSLAMonitorNamespace(pulsar1.getBrokerId(), pulsar1.getConfiguration());

        NamespaceName slaMonitorNamespacePulsar2 =
                getSLAMonitorNamespace(pulsar2.getBrokerId(), pulsar2.getConfiguration());

        NamespaceBundle bundle1 = pulsar1.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(heartbeatNamespacePulsar1V1);
        NamespaceBundle bundle2 = pulsar1.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(heartbeatNamespacePulsar1V2);

        NamespaceBundle bundle3 = pulsar2.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(heartbeatNamespacePulsar2V1);
        NamespaceBundle bundle4 = pulsar2.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(heartbeatNamespacePulsar2V2);

        NamespaceBundle slaBundle1 = pulsar1.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(slaMonitorNamespacePulsar1);
        NamespaceBundle slaBundle2 = pulsar2.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(slaMonitorNamespacePulsar2);


        Set<NamespaceBundle> ownedServiceUnitsByPulsar1 = primaryLoadManager.getOwnedServiceUnitsAsync()
                .get(5, TimeUnit.SECONDS);
        log.info("Owned service units: {}", ownedServiceUnitsByPulsar1);
        // heartbeat namespace bundle will own by pulsar1
        assertTrue(ownedServiceUnitsByPulsar1.contains(bundle1));
        assertTrue(ownedServiceUnitsByPulsar1.contains(bundle2));
        assertTrue(ownedServiceUnitsByPulsar1.contains(slaBundle1));
        Set<NamespaceBundle> ownedServiceUnitsByPulsar2 = secondaryLoadManager.getOwnedServiceUnitsAsync()
                .get(5, TimeUnit.SECONDS);
        log.info("Owned service units: {}", ownedServiceUnitsByPulsar2);
        assertTrue(ownedServiceUnitsByPulsar2.contains(bundle3));
        assertTrue(ownedServiceUnitsByPulsar2.contains(bundle4));
        assertTrue(ownedServiceUnitsByPulsar2.contains(slaBundle2));
        Map<String, NamespaceOwnershipStatus> ownedNamespacesByPulsar1 =
                admin.brokers().getOwnedNamespaces(conf.getClusterName(), pulsar1.getBrokerId());
        Map<String, NamespaceOwnershipStatus> ownedNamespacesByPulsar2 =
                admin.brokers().getOwnedNamespaces(conf.getClusterName(), pulsar2.getBrokerId());
        assertTrue(ownedNamespacesByPulsar1.containsKey(bundle1.toString()));
        assertTrue(ownedNamespacesByPulsar1.containsKey(bundle2.toString()));
        assertTrue(ownedNamespacesByPulsar1.containsKey(slaBundle1.toString()));

        assertTrue(ownedNamespacesByPulsar2.containsKey(bundle3.toString()));
        assertTrue(ownedNamespacesByPulsar2.containsKey(bundle4.toString()));
        assertTrue(ownedNamespacesByPulsar2.containsKey(slaBundle2.toString()));

        String topic = "persistent://" + defaultTestNamespace + "/test-get-owned-service-units";
        admin.topics().createPartitionedTopic(topic, 1);
        NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get(topic)).join();
        CompletableFuture<Optional<BrokerLookupData>> owner = primaryLoadManager.assign(Optional.empty(), bundle, LookupOptions.builder().build());
        assertFalse(owner.join().isEmpty());

        BrokerLookupData brokerLookupData = owner.join().get();
        if (brokerLookupData.getWebServiceUrl().equals(pulsar1.getWebServiceAddress())) {
            assertOwnedServiceUnits(pulsar1, primaryLoadManager, bundle);
        } else {
            assertOwnedServiceUnits(pulsar2, secondaryLoadManager, bundle);
        }
    }

    private void assertOwnedServiceUnits(
            PulsarService pulsar,
            ExtensibleLoadManagerImpl extensibleLoadManager,
            NamespaceBundle bundle) throws PulsarAdminException {
        Awaitility.await().untilAsserted(() -> {
            Set<NamespaceBundle> ownedBundles = extensibleLoadManager.getOwnedServiceUnitsAsync()
                    .get(5, TimeUnit.SECONDS);
            assertTrue(ownedBundles.contains(bundle));
        });
        Map<String, NamespaceOwnershipStatus> ownedNamespaces =
                admin.brokers().getOwnedNamespaces(conf.getClusterName(), pulsar.getBrokerId());
        assertTrue(ownedNamespaces.containsKey(bundle.toString()));
        NamespaceOwnershipStatus status = ownedNamespaces.get(bundle.toString());
        assertTrue(status.is_active);
        assertFalse(status.is_controlled);
        assertEquals(status.broker_assignment, BrokerAssignment.shared);
    }

    @Test(timeOut = 30 * 1000)
    public void testGetOwnedServiceUnitsWhenLoadManagerNotStart()
            throws Exception {
        ExtensibleLoadManagerImpl loadManager = new ExtensibleLoadManagerImpl();
        Set<NamespaceBundle> ownedServiceUnits = loadManager.getOwnedServiceUnitsAsync()
                .get(5, TimeUnit.SECONDS);
        assertNotNull(ownedServiceUnits);
        assertTrue(ownedServiceUnits.isEmpty());
    }

    @Test(timeOut = 30 * 1000)
    public void testTryAcquiringOwnership()
            throws PulsarAdminException, ExecutionException, InterruptedException {
        final String namespace = "public/testTryAcquiringOwnership";
        admin.namespaces().createNamespace(namespace, 1);
        String topic = "persistent://" + namespace + "/test";
        NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get(topic)).get();
        NamespaceEphemeralData namespaceEphemeralData = primaryLoadManager.tryAcquiringOwnership(bundle).get();
        assertTrue(Set.of(pulsar1.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl())
                .contains(namespaceEphemeralData.getNativeUrl()));
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    admin.namespaces().deleteNamespace(namespace, true);
                });
    }

    @Test(timeOut = 30 * 1000)
    public void testHealthcheck() throws PulsarAdminException {
        admin.brokers().healthcheck(TopicVersion.V2);
    }

    @Test(timeOut = 30 * 1000)
    public void compactionScheduleTest() {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> { // wait until true
                    primaryLoadManager.monitor();
                    secondaryLoadManager.monitor();
                    var threshold = admin.topicPolicies()
                            .getCompactionThreshold(ServiceUnitStateChannelImpl.TOPIC, false);
                    AssertJUnit.assertEquals(5 * 1024 * 1024, threshold == null ? 0 : threshold.longValue());
                });
    }

    @Test(timeOut = 10 * 1000)
    public void unloadTimeoutCheckTest()
            throws Exception {
        Pair<TopicName, NamespaceBundle> topicAndBundle = getBundleIsNotOwnByChangeEventTopic("unload-timeout");
        String topic = topicAndBundle.getLeft().toString();
        var bundle = topicAndBundle.getRight().toString();
        var releasing = new ServiceUnitStateData(Releasing, pulsar2.getBrokerId(), pulsar1.getBrokerId(), 1);
        overrideTableView(channel1, bundle, releasing);
        var topicFuture = pulsar1.getBrokerService().getOrCreateTopic(topic);


        try {
            topicFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.info("getOrCreateTopic failed", e);
            if (!(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException && e.getMessage()
                    .contains("Please redo the lookup"))) {
                fail();
            }
        }

        pulsar1.getBrokerService()
                .unloadServiceUnit(topicAndBundle.getRight(), true, true, 5,
                        TimeUnit.SECONDS).get(2, TimeUnit.SECONDS);
    }

    private static abstract class MockBrokerFilter implements BrokerFilter {

        @Override
        public String name() {
            return "Mock-broker-filter";
        }

    }

    protected static PulsarClient pulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return
                PulsarClient.builder()
                        .serviceUrl(url)
                        .statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

}
