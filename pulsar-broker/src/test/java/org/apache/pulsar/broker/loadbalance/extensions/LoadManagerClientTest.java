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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.service.MetadataStoreTopicPoliciesService;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testcontext.SpyConfig;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class LoadManagerClientTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] stateTableViews() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName()},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName()}
        };
    }

    @DataProvider
    public Object[][] shutdownFailureCases() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName(), false},
                {ServiceUnitStateTableViewImpl.class.getName(), true},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName(), false},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName(), true}
        };
    }

    @Test(dataProvider = "stateTableViews", timeOut = 60_000)
    public void testLoadManagerSurvivesGeneralClientClose(String stateTableViewClass) throws Exception {
        // Closing the broker's internal client requires an isolated broker, not the shared test runtime.
        try (PulsarTestContext context = newContext(stateTableViewClass)) {
            PulsarService broker = context.getPulsarService();
            ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(broker);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> loadManager.getRole() == ExtensibleLoadManagerImpl.Role.Leader);
            LoadDataStore<BrokerLoadData> store = loadManager.getBrokerLoadDataStore();
            String key = getNamespace();
            store.pushAsync(key, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(key).isPresent());

            String tenant = NamespaceName.get(getNamespace()).getTenant();
            broker.getAdminClient().clusters().createCluster(broker.getConfiguration().getClusterName(),
                    ClusterData.builder().build());
            broker.getAdminClient().tenants().createTenant(tenant,
                    new TenantInfoImpl(Set.of(), Set.of(broker.getConfiguration().getClusterName())));
            broker.getAdminClient().namespaces().createNamespace(getNamespace(), 1);

            broker.getClient().close();

            // Both the load-report producer and its table view must remain usable.
            String nextKey = key + "-after-close";
            store.pushAsync(nextKey, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(nextKey).isPresent());
            store.removeAsync(nextKey).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(nextKey).isEmpty());

            // Restarting the reporting resources must keep using the independent client.
            store.init();
            store.pushAsync(nextKey, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(nextKey).isPresent());
            store.removeAsync(nextKey).get(10, TimeUnit.SECONDS);

            // Loading a previously unowned bundle also requires a working service-unit state channel.
            String topic = newTopicName();
            try (PulsarClient client = PulsarClient.builder().serviceUrl(broker.getBrokerServiceUrl()).build();
                 Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                         .subscriptionName("client-isolation").subscribe();
                 Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create()) {
                producer.send("value");
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                assertThat(message.getValue()).isEqualTo("value");
                consumer.acknowledge(message);
            }
            store.removeAsync(key).get(10, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 60_000)
    public void testBrokerClosesLoadManagerClientAfterStoppingLoadManager() throws Exception {
        try (PulsarTestContext context = newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .configCustomizer(config -> config.setLoadManagerClassName(TrackingLoadManager.class.getName()))
                .build()) {
            PulsarService broker = context.getPulsarService();
            TrackingLoadManager loadManager = (TrackingLoadManager) ExtensibleLoadManagerImpl.get(broker);
            PulsarClientImpl generalClient = (PulsarClientImpl) broker.getClient();
            PulsarClientImpl loadManagerClient = (PulsarClientImpl) loadManager.getClient();
            assertThat(loadManagerClient).isNotSameAs(generalClient);
            assertThat(loadManagerClient.timer()).isSameAs(generalClient.timer());
            assertThat(loadManagerClient.externalExecutorProvider()).isSameAs(generalClient.externalExecutorProvider());
            assertThat(loadManagerClient.getScheduledExecutorProvider())
                    .isSameAs(generalClient.getScheduledExecutorProvider());
            assertThat(loadManagerClient.getConfiguration().getServiceUrl())
                    .isEqualTo(generalClient.getConfiguration().getServiceUrl());

            broker.closeAsync().get(15, TimeUnit.SECONDS);
            assertThat(loadManager.clientClosedAtChannelClose.get()).isFalse();
            assertThat(loadManagerClient.isClosed()).isTrue();
            assertThat(loadManager.getClient()).isNull();
        }
    }

    @Test(timeOut = 60_000)
    public void testClientClosedWhenLoadManagerFailsToStart() {
        AtomicReference<FailingLoadManager> failedLoadManager = new AtomicReference<>();
        AtomicReference<Boolean> clientClosedBeforeBrokerClose = new AtomicReference<>();
        assertThatThrownBy(() -> newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .configCustomizer(config -> config.setLoadManagerClassName(FailingLoadManager.class.getName()))
                .spyConfigCustomizer(config -> config.pulsarService(SpyConfig.SpyType.SPY))
                .pulsarServiceCustomizer(broker -> {
                    doAnswer(invocation -> {
                        FailingLoadManager loadManager = (FailingLoadManager) ExtensibleLoadManagerImpl.get(broker);
                        failedLoadManager.compareAndSet(null, loadManager);
                        clientClosedBeforeBrokerClose.compareAndSet(null, loadManager.createdClient.isClosed());
                        return invocation.callRealMethod();
                    }).when(broker).closeAsync(true);
                }).build().close()).hasRootCauseMessage("Injected load manager start failure");
        FailingLoadManager loadManager = failedLoadManager.get();
        assertThat(loadManager.createdClient).isNotNull();
        assertThat(loadManager.createdClient.isClosed()).isTrue();
        assertThat(clientClosedBeforeBrokerClose.get()).isTrue();
    }

    @Test(dataProvider = "stateTableViews", timeOut = 60_000)
    public void testClientClosedWhenLoadManagerFailsToStop(String stateTableViewClass) throws Exception {
        try (PulsarTestContext peerContext = newContext(stateTableViewClass);
             PulsarTestContext context = newContextBuilder(stateTableViewClass)
                     .reuseMockBookkeeperAndMetadataStores(peerContext).build()) {
            PulsarService broker = context.getPulsarService();
            ExtensibleLoadManagerImpl extensibleLoadManager = ExtensibleLoadManagerImpl.get(broker);
            PulsarClient client = extensibleLoadManager.getClient();
            // Keep a peer registered so unloading does not close the client before stop().
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(extensibleLoadManager.getBrokerRegistry().getAvailableBrokersAsync()
                            .get(5, TimeUnit.SECONDS)).contains(peerContext.getPulsarService().getBrokerId()));
            LoadManager original = broker.getLoadManager().get();
            LoadManager loadManager = spy(original);
            doAnswer(invocation -> {
                assertThat(client.isClosed()).as("client must remain open until stop() begins").isFalse();
                invocation.callRealMethod();
                throw new PulsarServerException("Injected load manager stop failure");
            }).when(loadManager).stop();
            broker.getLoadManager().set(loadManager);
            try {
                assertThatThrownBy(() -> broker.closeAsync().get(15, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Injected load manager stop failure");
                assertThat(client.isClosed()).isTrue();
                verify(loadManager).stop();
            } finally {
                broker.getLoadManager().set(original);
            }
        }
    }

    @Test(dataProvider = "shutdownFailureCases", timeOut = 60_000)
    public void testClientClosedWhenShutdownFailsBeforeLoadManagerStop(String stateTableViewClass,
                                                                     boolean failLoadManagerStop) throws Exception {
        try (PulsarTestContext peerContext = newContext(stateTableViewClass);
             PulsarTestContext context = newContextBuilder(stateTableViewClass)
                .reuseMockBookkeeperAndMetadataStores(peerContext)
                .spyConfigCustomizer(config -> config.namespaceService(SpyConfig.SpyType.SPY))
                .build()) {
            PulsarService broker = context.getPulsarService();
            ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(broker);
            PulsarClient client = loadManager.getClient();
            // A live peer prevents disableBroker() from closing the load manager during unloading.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(loadManager.getBrokerRegistry().getAvailableBrokersAsync().get(5, TimeUnit.SECONDS))
                            .contains(peerContext.getPulsarService().getBrokerId()));
            IllegalStateException shutdownFailure =
                    new IllegalStateException("Injected namespace service close failure");
            AtomicBoolean failOnce = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (failOnce.getAndSet(false)) {
                    throw shutdownFailure;
                }
                return invocation.callRealMethod();
            }).when(broker.getNamespaceService()).close();

            LoadManager original = broker.getLoadManager().get();
            LoadManager loadManagerWrapper = spy(original);
            PulsarServerException stopFailure = new PulsarServerException("Injected load manager stop failure");
            doAnswer(invocation -> {
                assertThat(client.isClosed()).isFalse();
                assertThat(broker.getIoEventLoopGroup().isShuttingDown()).isFalse();
                invocation.callRealMethod();
                if (failLoadManagerStop) {
                    throw stopFailure;
                }
                return null;
            }).when(loadManagerWrapper).stop();
            broker.getLoadManager().set(loadManagerWrapper);
            try {
                assertThatThrownBy(() -> broker.closeAsync().get(15, TimeUnit.SECONDS))
                        .cause().hasCause(shutdownFailure).satisfies(failure -> {
                            if (failLoadManagerStop) {
                                assertThat(failure.getSuppressed()).containsExactly(stopFailure);
                            } else {
                                assertThat(failure.getSuppressed()).isEmpty();
                            }
                        });
                verify(loadManagerWrapper).stop();
                assertThat(client.isClosed()).isTrue();
                assertThat(loadManager.getClient()).isNull();
                assertThat(peerContext.getPulsarService().isRunning()).isTrue();
            } finally {
                broker.getLoadManager().set(original);
            }
        }
    }

    @Test(dataProvider = "stateTableViews", timeOut = 60_000)
    public void testClientRemainsOpenWhenShutdownFailsBeforeUnloading(String stateTableViewClass) throws Exception {
        try (PulsarTestContext context = newContextBuilder(stateTableViewClass)
                .configCustomizer(config -> {
                    config.setTopicLevelPoliciesEnabled(true);
                    config.setTopicPoliciesServiceClassName(FailingCloseTopicPoliciesService.class.getName());
                }).build()) {
            PulsarService broker = context.getPulsarService();
            ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(broker);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> loadManager.getRole() == ExtensibleLoadManagerImpl.Role.Leader);
            PulsarClient client = ExtensibleLoadManagerImpl.get(broker).getClient();
            LoadDataStore<BrokerLoadData> store = loadManager.getBrokerLoadDataStore();
            String key = getNamespace();
            store.pushAsync(key, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(key).isPresent());

            assertThatThrownBy(() -> broker.closeAsync().get(15, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Injected topic policies service close failure");
            assertThat(broker.isRunning()).isTrue();
            assertThat(loadManager.running()).isTrue();

            // The failed close hook runs before unloading, so reporting must continue on the same client.
            String nextKey = key + "-after-close-failure";
            store.pushAsync(nextKey, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(nextKey).isPresent());
            assertThat(client.isClosed()).isFalse();
            assertThat(loadManager.getClient()).isSameAs(client);

            store.init();
            String restartedKey = key + "-after-store-restart";
            store.pushAsync(restartedKey, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(restartedKey).isPresent());

            broker.closeAsync().get(15, TimeUnit.SECONDS);
            assertThat(client.isClosed()).isTrue();
            assertThat(loadManager.running()).isFalse();
            assertThat(loadManager.getClient()).isNull();
        }
    }

    @Test(dataProvider = "stateTableViews", timeOut = 60_000)
    public void testLoadManagerOwnsClientAcrossRestart(String stateTableViewClass) throws Exception {
        try (PulsarTestContext context = newContext(stateTableViewClass)) {
            PulsarService broker = context.getPulsarService();
            ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(broker);
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> loadManager.getRole() == ExtensibleLoadManagerImpl.Role.Leader);
            PulsarClient firstClient = loadManager.getClient();
            PulsarClient generalClient = broker.getClient();

            loadManager.close();
            assertThat(firstClient.isClosed()).isTrue();
            assertThat(loadManager.getClient()).isNull();
            assertThat(generalClient.isClosed()).isFalse();
            loadManager.close();

            loadManager.start();
            assertThat(loadManager.getClient()).isNotNull().isNotSameAs(firstClient);
            assertThat(loadManager.getClient().isClosed()).isFalse();
            LoadDataStore<BrokerLoadData> store = loadManager.getBrokerLoadDataStore();
            String key = getNamespace();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    store.pushAsync(key, new BrokerLoadData()).get(5, TimeUnit.SECONDS));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(key).isPresent());
            PulsarClient secondClient = loadManager.getClient();
            broker.closeAsync().get(15, TimeUnit.SECONDS);
            assertThat(secondClient.isClosed()).isTrue();
        }
    }

    @Test(timeOut = 60_000)
    public void testClientClosedBeforeLoadManagerReachesRunning() throws Exception {
        List<Runnable> pendingStarts = new CopyOnWriteArrayList<>();
        try (PulsarTestContext context = newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .spyConfigCustomizer(config -> config.pulsarService(SpyConfig.SpyType.SPY))
                .pulsarServiceCustomizer(broker -> doAnswer(invocation -> {
                    pendingStarts.add(invocation.getArgument(0));
                    return null;
                }).when(broker).runWhenReadyForIncomingRequests(any(Runnable.class)))
                .build()) {
            ExtensibleLoadManagerImpl loadManager = ExtensibleLoadManagerImpl.get(context.getPulsarService());
            PulsarClient client = loadManager.getClient();
            assertThat(client).isNotNull();
            assertThat(client.isClosed()).isFalse();
            assertThat(loadManager.running()).isFalse();

            loadManager.close();
            assertThat(client.isClosed()).isTrue();
            assertThat(loadManager.getClient()).isNull();
            assertThat(pendingStarts).isNotEmpty();
            pendingStarts.forEach(Runnable::run);
            assertThat(loadManager.running()).isFalse();
            assertThat(loadManager.getClient()).isNull();
        }
    }

    @Test(timeOut = 60_000)
    public void testClientClosedWhenChannelCloseFails() throws Exception {
        try (PulsarTestContext context = newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .configCustomizer(config -> config.setLoadManagerClassName(TrackingLoadManager.class.getName()))
                .build()) {
            TrackingLoadManager loadManager = (TrackingLoadManager)
                    ExtensibleLoadManagerImpl.get(context.getPulsarService());
            PulsarClient client = loadManager.getClient();
            loadManager.failChannelClose = true;
            assertThatThrownBy(loadManager::close).hasMessage("Injected channel close failure");
            assertThat(loadManager.clientClosedAtChannelClose.get()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(loadManager.getClient()).isNull();
            loadManager.failChannelClose = false;
        }
    }

    private PulsarTestContext newContext(String stateTableViewClass) {
        return newContextBuilder(stateTableViewClass).build();
    }

    private PulsarTestContext.Builder newContextBuilder(String stateTableViewClass) {
        return PulsarTestContext.builder()
                .configCustomizer(config -> {
                    config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
                    config.setLoadManagerServiceUnitStateTableViewClassName(stateTableViewClass);
                    config.setTopicLevelPoliciesEnabled(false);
                    config.setBrokerShutdownTimeoutMs(10_000);
                });
    }

    public static class FailingLoadManager extends ExtensibleLoadManagerImpl {
        private PulsarClient createdClient;

        @Override
        protected ServiceUnitStateChannel createServiceUnitStateChannel(PulsarService pulsar) {
            createdClient = getClient();
            ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
            try {
                doThrow(new PulsarServerException("Injected load manager start failure")).when(channel).start();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
            return channel;
        }
    }

    public static class TrackingLoadManager extends ExtensibleLoadManagerImpl {
        private final AtomicReference<Boolean> clientClosedAtChannelClose = new AtomicReference<>();
        private boolean failChannelClose;

        @Override
        protected ServiceUnitStateChannel createServiceUnitStateChannel(PulsarService pulsar) {
            ServiceUnitStateChannel channel = spy(super.createServiceUnitStateChannel(pulsar));
            try {
                doAnswer(invocation -> {
                    clientClosedAtChannelClose.compareAndSet(null, getClient().isClosed());
                    invocation.callRealMethod();
                    if (failChannelClose) {
                        throw new PulsarServerException("Injected channel close failure");
                    }
                    return null;
                }).when(channel).close();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
            return channel;
        }
    }

    public static class FailingCloseTopicPoliciesService extends MetadataStoreTopicPoliciesService {
        private final AtomicBoolean failOnce = new AtomicBoolean(true);

        @Override
        public void close() {
            if (failOnce.getAndSet(false)) {
                throw new IllegalStateException("Injected topic policies service close failure");
            }
            super.close();
        }
    }
}
