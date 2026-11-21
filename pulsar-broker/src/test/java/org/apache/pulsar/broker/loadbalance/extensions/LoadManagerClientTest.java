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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.util.Set;
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
        try (PulsarTestContext context = newContext(ServiceUnitStateTableViewImpl.class.getName())) {
            PulsarService broker = context.getPulsarService();
            PulsarClientImpl generalClient = (PulsarClientImpl) broker.getClient();
            PulsarClientImpl loadManagerClient = (PulsarClientImpl) broker.getLoadManagerClient();
            assertThat(loadManagerClient).isNotSameAs(generalClient);
            assertThat(loadManagerClient.timer()).isSameAs(generalClient.timer());
            assertThat(loadManagerClient.externalExecutorProvider()).isSameAs(generalClient.externalExecutorProvider());
            assertThat(loadManagerClient.getScheduledExecutorProvider())
                    .isSameAs(generalClient.getScheduledExecutorProvider());
            assertThat(loadManagerClient.getConfiguration().getServiceUrl())
                    .isEqualTo(generalClient.getConfiguration().getServiceUrl());

            LoadManager original = broker.getLoadManager().get();
            LoadManager loadManager = spy(original);
            AtomicReference<Boolean> generalClientClosedAtStop = new AtomicReference<>();
            AtomicReference<Boolean> loadManagerClientClosedAtStop = new AtomicReference<>();
            doAnswer(invocation -> {
                generalClientClosedAtStop.set(generalClient.isClosed());
                loadManagerClientClosedAtStop.set(loadManagerClient.isClosed());
                return invocation.callRealMethod();
            }).when(loadManager).stop();
            broker.getLoadManager().set(loadManager);
            try {
                broker.closeAsync().get(15, TimeUnit.SECONDS);
                assertThat(generalClientClosedAtStop.get()).isTrue();
                assertThat(loadManagerClientClosedAtStop.get()).isFalse();
                assertThat(loadManagerClient.isClosed()).isTrue();
                assertThatThrownBy(broker::getLoadManagerClient).isInstanceOf(PulsarServerException.class);
            } finally {
                broker.getLoadManager().set(original);
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testClientClosedWhenLoadManagerFailsToStart() {
        AtomicReference<PulsarClient> client = new AtomicReference<>();
        assertThatThrownBy(() -> newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .configCustomizer(config -> config.setLoadManagerClassName(FailingLoadManager.class.getName()))
                .spyConfigCustomizer(config -> config.pulsarService(SpyConfig.SpyType.SPY))
                .pulsarServiceCustomizer(broker -> {
                    try {
                        doAnswer(invocation -> {
                            PulsarClient result = (PulsarClient) invocation.callRealMethod();
                            client.set(result);
                            return result;
                        }).when(broker).getLoadManagerClient();
                    } catch (PulsarServerException e) {
                        throw new RuntimeException(e);
                    }
                }).build().close()).hasRootCauseMessage("Injected load manager start failure");
        assertThat(client.get()).isNotNull();
        assertThat(client.get().isClosed()).isTrue();
    }

    @Test(timeOut = 60_000)
    public void testClientClosedWhenLoadManagerFailsToStop() throws Exception {
        try (PulsarTestContext context = newContext(ServiceUnitStateTableViewImpl.class.getName())) {
            PulsarService broker = context.getPulsarService();
            PulsarClient client = broker.getLoadManagerClient();
            LoadManager original = broker.getLoadManager().get();
            LoadManager loadManager = spy(original);
            doAnswer(invocation -> {
                invocation.callRealMethod();
                throw new PulsarServerException("Injected load manager stop failure");
            }).when(loadManager).stop();
            broker.getLoadManager().set(loadManager);
            try {
                assertThatThrownBy(() -> broker.closeAsync().get(15, TimeUnit.SECONDS))
                        .hasRootCauseMessage("Injected load manager stop failure");
                assertThat(client.isClosed()).isTrue();
            } finally {
                broker.getLoadManager().set(original);
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testClientClosedWhenShutdownFailsBeforeLoadManagerStop() throws Exception {
        try (PulsarTestContext context = newContextBuilder(ServiceUnitStateTableViewImpl.class.getName())
                .spyConfigCustomizer(config -> config.namespaceService(SpyConfig.SpyType.SPY))
                .build()) {
            PulsarService broker = context.getPulsarService();
            PulsarClient client = broker.getLoadManagerClient();
            AtomicBoolean failOnce = new AtomicBoolean(true);
            doAnswer(invocation -> {
                if (failOnce.getAndSet(false)) {
                    throw new IllegalStateException("Injected namespace service close failure");
                }
                return invocation.callRealMethod();
            }).when(broker.getNamespaceService()).close();

            assertThatThrownBy(() -> broker.closeAsync().get(15, TimeUnit.SECONDS))
                    .hasRootCauseMessage("Injected namespace service close failure");
            assertThat(client.isClosed()).isTrue();
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
            PulsarClient client = broker.getLoadManagerClient();
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
            assertThat(broker.getLoadManagerClient()).isSameAs(client);

            store.init();
            String restartedKey = key + "-after-store-restart";
            store.pushAsync(restartedKey, new BrokerLoadData()).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> store.get(restartedKey).isPresent());

            broker.closeAsync().get(15, TimeUnit.SECONDS);
            assertThat(client.isClosed()).isTrue();
            assertThat(loadManager.running()).isFalse();
            assertThatThrownBy(broker::getLoadManagerClient).isInstanceOf(PulsarServerException.class);
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
        @Override
        protected ServiceUnitStateChannel createServiceUnitStateChannel(PulsarService pulsar) {
            ServiceUnitStateChannel channel = mock(ServiceUnitStateChannel.class);
            try {
                doThrow(new PulsarServerException("Injected load manager start failure")).when(channel).start();
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
