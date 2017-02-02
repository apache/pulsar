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

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.MockZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import org.apache.zookeeper.KeeperException.NoNodeException;

public class BrokerDataAndServiceZKTest {
    private static final Logger log = LoggerFactory.getLogger(BrokerDataAndServiceZKTest.class);

    /**
     * Start pulsar with same data-zk and cluster-management-zk
     * 1. Both zk instance should be the same in pulsar
     * 2. all the znodes should be craeted correctly
     * 
     * @throws Exception
     */
    @Test
    public void testPulsarWithSingleDataAndLocalZk() throws Exception {

        PulsarServiceStarterTest pulsar = new PulsarServiceStarterTest(false);
        // (1) test msg produce and consume
        final String property = "my-property";
        final String namespace = "my-ns";
        testProduceConsume(pulsar.pulsarClient, property, namespace);
        // (2) test zk-state
        Assert.assertEquals(pulsar.pulsar.getLocalZooKeeperClientFactory(), pulsar.pulsar.getZooKeeperClientFactory());
        Assert.assertEquals(pulsar.pulsar.getLocalZkCache(), pulsar.pulsar.getDataZkCache());
        List<String> brokers = pulsar.localMockZookKeeper.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                null);
        List<String> nsOwnership = pulsar.localMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT,
                null);
        List<String> localPolicies = pulsar.localMockZookKeeper
                .getChildren(LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT, null);
        List<String> managedLedger = pulsar.localMockZookKeeper
                .getChildren(LocalZooKeeperCacheService.MANAGED_LEDGER_ROOT, null);
        List<String> counters = pulsar.localMockZookKeeper.getChildren("/counters", null);
        List<String> ledgers = pulsar.localMockZookKeeper.getChildren("/ledgers", null);

        // verify znodes are created properly
        Assert.assertTrue(brokers.contains("localhost:" + pulsar.BROKER_WEBSERVICE_PORT));
        Assert.assertTrue(nsOwnership.contains(property));
        Assert.assertTrue(localPolicies.contains(property));
        Assert.assertTrue(managedLedger.contains(property));
        Assert.assertTrue(counters.contains("producer-name"));
        Assert.assertFalse(ledgers.isEmpty());
    }

    /**
     * Start pulsar with different data-zk and cluster-management-zk
     * 1. Both zk instance should be the differemt in pulsar
     * 2. all the znodes should be created correctly
     *  a. cluster-management-znode: /loadbalance, /namespace, /counter, /admin/local-policies
     *  b. data-znode: /ledgers, /managed-ledger
     * 
     * @throws Exception
     */
    @Test
    public void testPulsarWithMultipleZkForDataAndLocal() throws Exception {

        PulsarServiceStarterTest pulsar = new PulsarServiceStarterTest(true);
        // (1) test msg produce and consume
        final String property = "my-property";
        final String namespace = "my-ns";
        testProduceConsume(pulsar.pulsarClient, property, namespace);
        // (2) test zk-state
        Assert.assertNotEquals(pulsar.pulsar.getLocalZooKeeperClientFactory(),
                pulsar.pulsar.getZooKeeperClientFactory());
        Assert.assertNotEquals(pulsar.pulsar.getLocalZkCache(), pulsar.pulsar.getDataZkCache());
        List<String> brokers = pulsar.localMockZookKeeper.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT,
                null);
        List<String> nsOwnership = pulsar.localMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT,
                null);
        List<String> localPolicies = pulsar.localMockZookKeeper
                .getChildren(LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT, null);
        List<String> managedLedger = pulsar.localMockZookKeeper
                .getChildren(LocalZooKeeperCacheService.MANAGED_LEDGER_ROOT, null);
        List<String> counters = pulsar.localMockZookKeeper.getChildren("/counters", null);
        List<String> ledgers;
        try {
            ledgers = pulsar.localMockZookKeeper.getChildren("/ledgers", null);
            fail("dataZK should not have node present");
        } catch (NoNodeException ne) {
            // ok
        }

        // verify cluster-management-znodes are created properly
        Assert.assertTrue(brokers.contains("localhost:" + pulsar.BROKER_WEBSERVICE_PORT));
        Assert.assertTrue(nsOwnership.contains(property));
        Assert.assertTrue(localPolicies.contains(property));
        Assert.assertTrue(counters.contains("producer-name"));
        Assert.assertTrue(managedLedger.isEmpty());

        // LocalZooKeeperCacheService.initZK() => Creates : OWNER_INFO_ROOT,LOCAL_POLICIES_ROOT,MANAGED_LEDGER_ROOT (so,
        // znode will be created in both zk but it will be empty)
        brokers.clear();
        counters.clear();
        try {
            brokers = pulsar.dataMockZookKeeper.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT, null);
            counters = pulsar.dataMockZookKeeper.getChildren("/counters", null);
            fail("dataZK should not have node present");
        } catch (NoNodeException ne) {
            // ok
        }
        nsOwnership = pulsar.dataMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT, null);
        localPolicies = pulsar.dataMockZookKeeper.getChildren(LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT, null);
        managedLedger = pulsar.dataMockZookKeeper.getChildren(LocalZooKeeperCacheService.MANAGED_LEDGER_ROOT, null);
        ledgers = pulsar.dataMockZookKeeper.getChildren("/ledgers", null);

        // verify data-znodes are created properly
        Assert.assertTrue(brokers.isEmpty());
        Assert.assertTrue(counters.isEmpty());
        Assert.assertTrue(nsOwnership.isEmpty());
        Assert.assertTrue(localPolicies.isEmpty());
        Assert.assertTrue(managedLedger.contains(property));
        Assert.assertFalse(ledgers.isEmpty());

    }

    static class PulsarServiceStarterTest extends MockedPulsarServiceBaseTest {

        private boolean diffDataAndLocalZk = false;

        public PulsarServiceStarterTest(boolean diffDataAndLocalZk) throws Exception {
            this.diffDataAndLocalZk = diffDataAndLocalZk;
            setup();
        }

        @Override
        protected void setup() throws Exception {
            // internal-setup
            initPulsar();
            com.yahoo.pulsar.client.api.ClientConfiguration clientConf = new com.yahoo.pulsar.client.api.ClientConfiguration();
            clientConf.setStatsInterval(0, TimeUnit.SECONDS);
            String lookupUrl = brokerUrl.toString();
            if (isTcpLookup) {
                lookupUrl = new URI("pulsar://localhost:" + BROKER_PORT).toString();
            }
            pulsarClient = PulsarClient.create(lookupUrl, clientConf);

            // create test cluster, property, namespace
            admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
            admin.properties().createProperty("my-property",
                    new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
            admin.namespaces().createNamespace("my-property/use/my-ns");
        }

        @Override
        protected void cleanup() throws Exception {
            super.internalCleanup();
        }

        protected final void initPulsar() throws Exception {
            dataMockZookKeeper = createMockZooKeeper();
            if (diffDataAndLocalZk) {
                localMockZookKeeper = MockZooKeeper.newInstance(MoreExecutors.sameThreadExecutor());
            } else {
                localMockZookKeeper = dataMockZookKeeper;
            }
            mockBookKeeper = new NonClosableMockBookKeeper(new ClientConfiguration(), dataMockZookKeeper);
            sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
            startBroker();
            brokerUrl = new URL("http://" + pulsar.getAdvertisedAddress() + ":" + BROKER_WEBSERVICE_PORT);
            brokerUrlTls = new URL("https://" + pulsar.getAdvertisedAddress() + ":" + BROKER_WEBSERVICE_PORT_TLS);
            admin = spy(new PulsarAdmin(brokerUrl, (Authentication) null));
        }

        protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
            // Override default providers with mocked ones
            doReturn(!diffDataAndLocalZk).when(pulsar).equalsDataAndLocalZk();
            doReturn(localMockZooKeeperClientFactory).when(pulsar).getLocalZooKeeperClientFactory();
            doReturn(mockBookKeeperClientFactory).when(pulsar).getBookKeeperClientFactory();
            Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
            doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();
            doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
            // use diff zk instance for split-zk usecase
            if (diffDataAndLocalZk) {
                doReturn(dataMockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
            } else {
                doReturn(localMockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
            }
        }

    }

    private void testProduceConsume(PulsarClient pulsarClient, String property, String namespace) throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        final String topic = String.format("persistent://%s/use/%s/my-topic1", property, namespace);
        Consumer consumer = pulsarClient.subscribe(topic, "my-subscriber-name", conf);
        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer(topic, producerConf);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        Message msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

    }

    private void testMessageOrderAndDuplicates(Set<String> messagesReceived, String receivedMessage,
            String expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }

}
