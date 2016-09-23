/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.auth;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.MockBookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.google.common.util.concurrent.MoreExecutors;
import com.yahoo.pulsar.broker.BookKeeperClientFactory;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

/**
 * Base class for all tests that need a Pulsar instance without a ZK and BK cluster
 */
public abstract class MockedPulsarServiceBaseTest {

    protected final ServiceConfiguration conf;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;
    protected URL brokerUrl;
    protected URL brokerUrlTls;

    protected final int BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    protected final int BROKER_WEBSERVICE_PORT_TLS = PortManager.nextFreePort();
    protected final int BROKER_PORT = PortManager.nextFreePort();
    protected final int BROKER_PORT_TLS = PortManager.nextFreePort();

    protected MockZooKeeper mockZookKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;

    public MockedPulsarServiceBaseTest() {
        this.conf = new ServiceConfiguration();
        this.conf.setBrokerServicePort(BROKER_PORT);
        this.conf.setBrokerServicePortTls(BROKER_PORT_TLS);
        this.conf.setWebServicePort(BROKER_WEBSERVICE_PORT);
        this.conf.setWebServicePortTls(BROKER_WEBSERVICE_PORT_TLS);
        this.conf.setClusterName("test");
    }

    protected final void internalSetup() throws Exception {
        init();
        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        pulsarClient = PulsarClient.create(brokerUrl.toString(), clientConf);
    }

    protected final void internalSetupForStatsTest() throws Exception {
        init();
        com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
        clientConf.setStatsInterval(1, TimeUnit.SECONDS);
        pulsarClient = PulsarClient.create(brokerUrl.toString(), clientConf);
    }

    private final void init() throws Exception {
        mockZookKeeper = createMockZooKeeper();
        mockBookKeeper = new NonClosableMockBookKeeper(new ClientConfiguration(), mockZookKeeper);

        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();

        startBroker();

        brokerUrl = new URL("http://" + pulsar.getAdvertisedAddress() + ":" + BROKER_WEBSERVICE_PORT);
        brokerUrlTls = new URL("https://" + pulsar.getAdvertisedAddress() + ":" + BROKER_WEBSERVICE_PORT_TLS);

        admin = spy(new PulsarAdmin(brokerUrl, (Authentication) null));
    }

    protected final void internalCleanup() throws Exception {
        admin.close();
        pulsarClient.close();
        pulsar.close();
        mockBookKeeper.reallyShutdow();
        mockZookKeeper.shutdown();
        sameThreadOrderedSafeExecutor.shutdown();
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        pulsar.close();
        // Simulate cleanup of ephemeral nodes
        //mockZookKeeper.delete("/loadbalance/brokers/localhost:" + pulsar.getConfiguration().getWebServicePort(), -1);
    }

    protected void startBroker() throws Exception {
        this.pulsar = startBroker(conf);
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = spy(new PulsarService(conf));

        setupBrokerMocks(pulsar);
        pulsar.start();
        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(pulsar).getBookKeeperClientFactory();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    private MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.sameThreadExecutor());
        List<ACL> dummyAclList = new ArrayList<ACL>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    private static class NonClosableMockBookKeeper extends MockBookKeeper {

        public NonClosableMockBookKeeper(ClientConfiguration conf, ZooKeeper zk) throws Exception {
            super(conf, zk);
        }

        @Override
        public void close() throws InterruptedException, BKException {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdow() {
            super.shutdown();
        }
    }

    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                int zkSessionTimeoutMillis) {
            // Always return the same instance (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZookKeeper);
        }
    };

    private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient) throws IOException {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }
    };
}
