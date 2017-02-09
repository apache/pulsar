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
package com.yahoo.pulsar.broker.namespace;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.auth.BrokerDataAndServiceZKTest;
import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import com.yahoo.pulsar.broker.cache.LocalZooKeeperCacheService;
import com.yahoo.pulsar.broker.loadbalance.LoadManagerFactory;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.namespace.OwnershipCacheFactory.OwnershipDualCacheFactoryImpl;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

public class BrokerDataAndServiceZKDualOwnerShipTest {
    private static final Logger log = LoggerFactory.getLogger(BrokerDataAndServiceZKTest.class);

    /**
     * It verifies that bundle-ownership gets acquires on both the zk
     * 
     * @throws Exception
     */
    @Test
    public void testOwnershipNodeCreatedInBothZK() throws Exception {

        PulsarServiceStarterTest pulsar = new PulsarServiceStarterTest(true);
        // (1) test msg produce and consume
        final String property = "my-property";
        final String namespace = "my-ns";
        testProduceConsume(pulsar.pulsarClient, property, namespace);
        // verify both zk are different
        assertNotEquals(pulsar.pulsar.getLocalZooKeeperClientFactory(),
                pulsar.pulsar.getZooKeeperClientFactory());
        assertNotEquals(pulsar.pulsar.getLocalZkCache(), pulsar.pulsar.getDataZkCache());
        
        // verify ownershipCache should be instance of newly injected ownershipCacheType
        NamespaceOwnershipCache ownershipCache = pulsar.pulsar.getNamespaceService().getOwnershipCache();
        assertTrue(ownershipCache instanceof DualOwnershipCache);
        
        // verify: ownership node is created in both the cluster
        List<String> nsOwnership = null;
        nsOwnership = pulsar.localMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT, null);
        assertTrue(nsOwnership.contains(property));
        nsOwnership = pulsar.dataMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT, null);
        assertTrue(nsOwnership.contains(property));
    }
    
    /**
     * It makes sure that bundle-ownership should be atomic:
     * 
     * A. ownership-znode present into secondary
     * 1. Ownership node is present into secondary-zk
     * 2. tryToAcquireOwnership: 
     *    a. creates ownership-node on primary-zk
     *    b. Failed to create on secondary as node is present
     *    c. delete ownership-node on primary-zk
     * 3. Verify primary-zk should not have ownership-znode
     * 
     * B. ownership-znode present into primary
     * 1. It fails to acquire ownership on secondary-zk 
     * @throws Exception
     */
    @Test
    public void testAtomicOwnershipFailOnAcquiringOnAnyZk() throws Exception {

        PulsarServiceStarterTest pulsar = new PulsarServiceStarterTest(true);
        // (1) test msg produce and consume
        final String property = "my-property";
        final String namespace1 = "my-ns1";
        final String namespace2 = "my-ns2";
        final String cluster = "use";
        final String topicName1 = String.format("persistent://%s/%s/%s/my-topic1", property, cluster, namespace1);
        // verify both zk are different
        assertNotEquals(pulsar.pulsar.getLocalZooKeeperClientFactory(),
                pulsar.pulsar.getZooKeeperClientFactory());
        assertNotEquals(pulsar.pulsar.getLocalZkCache(), pulsar.pulsar.getDataZkCache());

        /***** A. ownership-znode present into secondary ***/
        String ownershipNodePath = String.format("/namespace/%s/%s/%s/0x00000000_0xffffffff", property, cluster,
                namespace1);
        // create ownership on data-zk(secondary-zk)
        ZkUtils.createFullPathOptimistic(pulsar.dataMockZookKeeper, ownershipNodePath,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), new ArrayList<ACL>(0), CreateMode.PERSISTENT);
        assertNotNull(pulsar.dataMockZookKeeper.getData(ownershipNodePath, null, null));
        // try to acquire ownership : it should fail as it should be atomic in sense of acquiring on both the zk
        NamespaceOwnershipCache ownershipCache = pulsar.pulsar.getNamespaceService().getOwnershipCache();
        NamespaceBundle bundle = pulsar.pulsar.getNamespaceService().getBundle(DestinationName.get(topicName1));
        CompletableFuture<NamespaceEphemeralData> future = ownershipCache.tryAcquiringOwnership(bundle);
        // (1) Acquiring-Ownership should fail
        try {
            future.get();
            fail("acquire-ownership should have failed");
        } catch (Exception ne) {
            if (ne.getCause() instanceof org.apache.zookeeper.KeeperException.NodeExistsException) {
                // ok : node already exist on zk2
            } else {
                fail("It should not fail with " + ne.getCause().getMessage());
            }
        }
        // (2) first zk should have deleted the ownership
        List<String> nsOwnership = null;
        nsOwnership = pulsar.localMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT, null);
        try {
            pulsar.localMockZookKeeper.getData(ownershipNodePath, null, null);
            fail("it should have failed as ownership node should be deleted");
        } catch (org.apache.zookeeper.KeeperException.NoNodeException nn) {
            // ok
        }

        /****** B. ownership-znode present into primary ******/
        ownershipNodePath = String.format("/namespace/%s/%s/%s/0x00000000_0xffffffff", property, cluster, namespace2);
        // create ownership on data-zk(secondary-zk)
        ZkUtils.createFullPathOptimistic(pulsar.localMockZookKeeper, ownershipNodePath,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), new ArrayList<ACL>(0), CreateMode.PERSISTENT);
        assertNotNull(pulsar.localMockZookKeeper.getData(ownershipNodePath, null, null));
        // try to acquire ownership : it should fail as it should be atomic in sense of acquiring on both the zk
        final String topicName2 = String.format("persistent://%s/%s/%s/my-topic1", property, cluster, namespace2);
        bundle = pulsar.pulsar.getNamespaceService().getBundle(DestinationName.get(topicName2));
        future = ownershipCache.tryAcquiringOwnership(bundle);
        // (1) Acquiring-Ownership should fail
        try {
            future.get();
            fail("acquire-ownership should have failed");
        } catch (Exception ne) {
            if (ne.getCause() instanceof org.apache.zookeeper.KeeperException.NodeExistsException) {
                // ok : node already exist on zk2
            } else {
                fail("It should not fail with " + ne.getCause().getMessage());
            }
        }
        // (2) first zk should have deleted the ownership
        nsOwnership = null;
        nsOwnership = pulsar.localMockZookKeeper.getChildren(LocalZooKeeperCacheService.OWNER_INFO_ROOT, null);
        // ownership should fail on secondary-zk
        try {
            pulsar.dataMockZookKeeper.getData(ownershipNodePath, null, null);
            fail("it should have failed as ownership node should be deleted");
        } catch (org.apache.zookeeper.KeeperException.NoNodeException nn) {
            // ok
        }

    }

    /**
     * 
     * It verifies lookup aggregates result from local_zk and data_zk and return response.
     * 
     * @throws Exception
     */
    @Test
    public void testLookupOwnership() throws Exception {

        PulsarServiceStarterTest pulsar = new PulsarServiceStarterTest(true);
        // (1) test msg produce and consume
        final String property = "my-property";
        final String namespace1 = "1ns";
        final String namespace2 = "2ns";
        final String cluster = "use";
        final String topicName1 = String.format("persistent://%s/%s/%s/my-topic1", property, cluster, namespace1);

        // removeOwnership on primary first then secondary : Lookup should succeed
        String ownershipNodePath = String.format("/namespace/%s/%s/%s/0x00000000_0xffffffff", property, cluster, namespace1);
        DualOwnershipCache ownershipCache = (DualOwnershipCache) pulsar.pulsar.getNamespaceService().getOwnershipCache();
        NamespaceBundle bundle = pulsar.pulsar.getNamespaceService().getBundle(DestinationName.get(topicName1));
        CompletableFuture<NamespaceEphemeralData> future = ownershipCache.tryAcquiringOwnership(bundle);
        NamespaceEphemeralData owner = future.get();
        assertNotNull(owner);
        assertEquals(owner.getNativeUrl(), pulsar.pulsar.getBrokerServiceUrl());
        
        // check before removing from any zk
        assertNotNull(ownershipCache.getOwnedBundle(bundle));
        assertNotNull(ownershipCache.getOwnedBundles().get(ownershipNodePath));
        assertNotNull(ownershipCache.getOwnerAsync(bundle).get());
        // remove from primary zk
        ownershipCache.localZkOwnershipCache.removeOwnership(bundle).get();
        assertNotNull(ownershipCache.getOwnedBundle(bundle));
        assertNotNull(ownershipCache.getOwnedBundles().get(ownershipNodePath));
        assertTrue(ownershipCache.getOwnerAsync(bundle).get().isPresent());
        // remove secondary zk
        ownershipCache.dataZkOwnershipCache.removeOwnership(bundle).get();
        // it should be remove from all
        assertNull(ownershipCache.getOwnedBundle(bundle));
        assertNull(ownershipCache.getOwnedBundles().get(ownershipNodePath));
        assertFalse(ownershipCache.getOwnerAsync(bundle).get().isPresent());
        
        // removeOwnership on secondary first then primary: Lookup should succeed
        final String topicName2 = String.format("persistent://%s/%s/%s/my-topic1", property, cluster, namespace2);
        bundle = pulsar.pulsar.getNamespaceService().getBundle(DestinationName.get(topicName2));
        future = ownershipCache.tryAcquiringOwnership(bundle);
        owner = future.get();
        assertNotNull(owner);
        assertEquals(owner.getNativeUrl(), pulsar.pulsar.getBrokerServiceUrl());
        
        ownershipNodePath = String.format("/namespace/%s/%s/%s/0x00000000_0xffffffff", property, cluster, namespace2);
        assertNotNull(ownershipCache.getOwnedBundle(bundle));
        assertNotNull(ownershipCache.getOwnedBundles().get(ownershipNodePath));
        assertTrue(ownershipCache.getOwnerAsync(bundle).get().isPresent());
        ownershipCache.removeOwnership(bundle);
        assertNull(ownershipCache.getOwnedBundles().get(ownershipNodePath));
        assertNull(ownershipCache.getOwnedBundle(bundle));
        assertFalse(ownershipCache.getOwnerAsync(bundle).get().isPresent());

    }
    
    /**
     * It verifies that LoadManager writes loadReport and register broker in both the zookeeper
     * 
     * verify:
     * <p>
     * 1. pulsar1-broker registered itself to only data_zk, but pulsar2-broker registered itself to both local_zk and
     * data_zk
     * <p>
     * 2. data_zk-load-balancer should return : active-brokers and load-report with both brokers, BUT
     * local_zk-load-balancer should return : active-brokers and load-report with only broker1 (pulsar1)
     * <p>
     * 3. pulsar1-broker: due to dual-write: it knows load-report and namespace-bundle of both brokers, pulsar2-broker:
     * due to dual-read: it knows load-report and namespace-bundle of both brokers
     * 
     * @throws Exception
     */
    @Test
    public void testLoadBalancerWriteReadOnMultipleZk() throws Exception {
       
        // this pulsar instance will use one ZK for cluster-management and ledger-data
        PulsarServiceStarterTest pulsar1 = new PulsarServiceStarterTest(false);
        // this pulsar instance will use two different zk for cluster-mngmt and ledger-data, however it will use the same data_zk as pulsar1 instance
        PulsarServiceStarterTest pulsar2 = new PulsarServiceStarterTest(true, true, pulsar1.localMockZookKeeper);
        // verify the data_zk instance is same for both
        assertEquals(pulsar1.pulsar.getLocalZkClient(), pulsar2.pulsar.getDataZkClient());
        
        Set<String> activeBrokers1 = pulsar1.pulsar.getLoadManager().getActiveBrokers();
        // broker1-loadBalancer should give both broker in activeList as broker2 has done dual-write
        assertEquals(activeBrokers1.size(), 2);
        // broker2-loadBalancer should give both broker in activeList as broker2 has done dual-read
        Set<String> activeBrokers2 = pulsar2.pulsar.getLoadManager().getActiveBrokers();
        assertEquals(activeBrokers2.size(), 2);
        /**
         *  verify: 
         *  1. pulsar2 has registered brokers in data_zk and local_zk + pulsar1 has just registered in local_zk
         *  2. so data_zk has both brokers registered but local_zk will just have broker2
         */
        List<String> dataZkActiveBrokers = pulsar1.localMockZookKeeper.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT, null);
        List<String> localZkActiveBrokers = pulsar2.localMockZookKeeper.getChildren(SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT, null);
        assertEquals(dataZkActiveBrokers.size(), 2);
        assertEquals(localZkActiveBrokers.size(), 1);
        String pulsar1Znode = pulsar1.pulsar.getAdvertisedAddress()+":"+pulsar1.pulsar.getConfiguration().getWebServicePort();
        String pulsar2Znode = pulsar1.pulsar.getAdvertisedAddress()+":"+pulsar2.pulsar.getConfiguration().getWebServicePort();
        assertTrue(dataZkActiveBrokers.contains(pulsar1Znode));
        assertTrue(dataZkActiveBrokers.contains(pulsar2Znode));
        assertTrue(localZkActiveBrokers.contains(pulsar2Znode));
        assertFalse(localZkActiveBrokers.contains(pulsar1Znode));
    }
    
    
    static class PulsarServiceStarterTest extends MockedPulsarServiceBaseTest {

        private boolean diffDataAndLocalZk = false;
        private boolean dualLoadBalancer = false;

        public PulsarServiceStarterTest(boolean diffDataAndLocalZk) throws Exception {
            this.diffDataAndLocalZk = diffDataAndLocalZk;
            // initialize data-zk first
            dataMockZookKeeper = createMockZooKeeper();
            setup();
        }
        
        public PulsarServiceStarterTest(boolean diffDataAndLocalZk, boolean dualLoadBalancer, MockZooKeeper dataMockZk) throws Exception {
            this.diffDataAndLocalZk = diffDataAndLocalZk;
            this.dualLoadBalancer = dualLoadBalancer;
            dataMockZookKeeper = dataMockZk;
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
            try {
                admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
                admin.properties().createProperty("my-property",
                        new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
                admin.namespaces().createNamespace("my-property/use/my-ns");                
            } catch(Exception e){
                //ignore if cluster is already exist
            }
        }

        @Override
        protected void cleanup() throws Exception {
            super.internalCleanup();
        }

        protected final void initPulsar() throws Exception {
            if (diffDataAndLocalZk) {
                localMockZookKeeper = MockZooKeeper.newInstance(MoreExecutors.sameThreadExecutor());
            } else {
                localMockZookKeeper = dataMockZookKeeper;
            }
            mockBookKeeper = new NonClosableMockBookKeeper(new ClientConfiguration(), dataMockZookKeeper);
            sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
            if(diffDataAndLocalZk) {
                conf.setNamespaceOwnershipCacheFactoryProvider(OwnershipDualCacheFactoryImpl.class.getName());
            }
            if(dualLoadBalancer) {
                conf.setLoadBalancerFactoryProvider(LoadManagerFactory.SimpleLoadManagerDualZkImplFactory.class.getName());
            }
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

    private String testProduceConsume(PulsarClient pulsarClient, String property, String namespace) throws Exception {
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
        return topic;
    }

    private void testMessageOrderAndDuplicates(Set<String> messagesReceived, String receivedMessage,
            String expectedMessage) {
        // Make sure that messages are received in order
        assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }

}
