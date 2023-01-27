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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterData.ClusterUrl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

import lombok.Cleanup;

@Test(groups = "broker")
public class ClusterMigrationTest {

    private static final Logger log = LoggerFactory.getLogger(ClusterMigrationTest.class);
    protected String methodName;

    String namespace = "pulsar/migrationNs";
    TestBroker broker1, broker2;
    URL url1;
    URL urlTls1;
    PulsarService pulsar1;

    PulsarAdmin admin1;

    URL url2;
    URL urlTls2;
    PulsarService pulsar2;
    PulsarAdmin admin2;

    @DataProvider(name = "TopicsubscriptionTypes")
    public Object[][] subscriptionTypes() {
        return new Object[][] {
                {true, SubscriptionType.Shared},
                {true, SubscriptionType.Key_Shared},
                {true, SubscriptionType.Shared},
                {true, SubscriptionType.Key_Shared},

                {false, SubscriptionType.Shared},
                {false, SubscriptionType.Key_Shared},
                {false, SubscriptionType.Shared},
                {false, SubscriptionType.Key_Shared},
        };
    }

    @BeforeMethod(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {

        log.info("--- Starting ReplicatorTestBase::setup ---");

        broker1 = new TestBroker();
        broker2 = new TestBroker();
        String clusterName = broker1.getClusterName();

        pulsar1 = broker1.getPulsarService();
        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        pulsar2 = broker2.getPulsarService();
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        // Start region 3

        // Provision the global namespace
        admin1.clusters().createCluster(clusterName,
                ClusterData.builder().serviceUrl(url1.toString()).serviceUrlTls(urlTls1.toString())
                        .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls()).build());
        admin2.clusters().createCluster(clusterName,
                ClusterData.builder().serviceUrl(url2.toString()).serviceUrlTls(urlTls2.toString())
                        .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls()).build());

        admin1.tenants().createTenant("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet(clusterName)));
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet(clusterName));

        admin2.tenants().createTenant("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet(clusterName)));
        admin2.namespaces().createNamespace(namespace, Sets.newHashSet(clusterName));

        assertEquals(admin1.clusters().getCluster(clusterName).getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster(clusterName).getServiceUrl(), url2.toString());
        assertEquals(admin1.clusters().getCluster(clusterName).getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster(clusterName).getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());

        Thread.sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    @AfterMethod(alwaysRun = true, timeOut = 300000)
    protected void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        broker1.cleanup();
        broker2.cleanup();
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    /**
     * Test producer/consumer migration: using persistent/non-persistent topic and all types of subscriptions
     * (1) Producer1 and consumer1 connect to cluster-1
     * (2) Close consumer1 to build backlog and publish messages using producer1
     * (3) Migrate topic to cluster-2
     * (4) Validate producer-1 is connected to cluster-2
     * (5) create consumer1, drain backlog and migrate and reconnect to cluster-2
     * (6) Create new consumer2 with different subscription on cluster-1, 
     *     which immediately migrate and reconnect to cluster-2
     * (7) Create producer-2 directly to cluster-2
     * (8) Create producer-3 on cluster-1 which should be redirected to cluster-2 
     * (8) Publish messages using producer1, producer2, and producer3
     * (9) Consume all messages by both consumer1 and consumer2
     * (10) Create Producer/consumer on non-migrated cluster and verify their connection with cluster-1 
     * (11) Restart Broker-1 and connect producer/consumer on cluster-1 
     * @throws Exception
     */
    @Test(dataProvider = "TopicsubscriptionTypes")
    public void testClusterMigration(boolean persistent, SubscriptionType subType) throws Exception {
        log.info("--- Starting ReplicatorTest::testClusterMigration ---");
        persistent = false;
        final String topicName = BrokerTestUtil
                .newUniqueName((persistent ? "persistent" : "non-persistent") + "://" + namespace + "/migrationTopic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-1 producer/consumer
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s1").subscribe();
        AbstractTopic topic1 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName, false).getNow(null).get();
        retryStrategically((test) -> !topic1.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !topic1.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(topic1.getProducers().isEmpty());
        assertFalse(topic1.getSubscriptions().isEmpty());

        // build backlog
        consumer1.close();
        int n = 5;
        for (int i = 0; i < n; i++) {
            producer1.send("test1".getBytes());
        }

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-2 producer/consumer
        Producer<byte[]> producer2 = client2.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster2-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic topic2 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName, false).getNow(null).get();
        assertFalse(topic2.getProducers().isEmpty());

        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls());
        admin1.clusters().updateClusterMigration(broker2.getClusterName(), true, migratedUrl);

        retryStrategically((test) -> {
            try {
                topic1.checkClusterMigration().get();
                return true;
            } catch (Exception e) {
                // ok
            }
            return false;
        }, 10, 500);

        topic1.checkClusterMigration().get();

        producer1.sendAsync("test1".getBytes());

        // producer is disconnected from cluster-1
        retryStrategically((test) -> topic1.getProducers().isEmpty(), 10, 500);
        assertTrue(topic1.getProducers().isEmpty());

        // create 3rd producer on cluster-1 which should be redirected to cluster-2
        Producer<byte[]> producer3 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-2").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        // producer is connected with cluster-2
        retryStrategically((test) -> topic2.getProducers().size() == 3, 10, 500);
        assertTrue(topic2.getProducers().size() == 3);

        // try to consume backlog messages from cluster-1
        consumer1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        if (persistent) {
            for (int i = 0; i < n; i++) {
                Message<byte[]> msg = consumer1.receive();
                assertEquals(msg.getData(), "test1".getBytes());
                consumer1.acknowledge(msg);
            }
        }
        // after consuming all messages, consumer should have disconnected
        // from cluster-1 and reconnect with cluster-2
        retryStrategically((test) -> !topic2.getSubscriptions().isEmpty(), 10, 500);
        assertFalse(topic2.getSubscriptions().isEmpty());

        // not also create a new consumer which should also reconnect to cluster-2
        Consumer<byte[]> consumer2 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s2").subscribe();
        retryStrategically((test) -> topic2.getSubscription("s2") != null, 10, 500);
        assertFalse(topic2.getSubscription("s2").getConsumers().isEmpty());

        // publish messages to cluster-2 and consume them
        for (int i = 0; i < n; i++) {
            producer1.send("test2".getBytes());
            producer2.send("test2".getBytes());
            producer3.send("test2".getBytes());
        }
        log.info("Successfully published messages by migrated producers");
        for (int i = 0; i < n * 3; i++) {
            assertEquals(consumer1.receive(2, TimeUnit.SECONDS).getData(), "test2".getBytes());
            assertEquals(consumer2.receive(2, TimeUnit.SECONDS).getData(), "test2".getBytes());

        }

        // create non-migrated topic which should connect to cluster-1
        String diffTopic = BrokerTestUtil
                .newUniqueName((persistent ? "persistent" : "non-persistent") + "://" + namespace + "/migrationTopic");
        Consumer<byte[]> consumerDiff = client1.newConsumer().topic(diffTopic).subscriptionType(subType)
                .subscriptionName("s1-d").subscribe();
        Producer<byte[]> producerDiff = client1.newProducer().topic(diffTopic).enableBatching(false)
                .producerName("cluster1-d").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic topicDiff = (AbstractTopic) pulsar1.getBrokerService().getTopic(diffTopic, false).getNow(null).get();
        assertNotNull(topicDiff);
        for (int i = 0; i < n; i++) {
            producerDiff.send("diff".getBytes());
            assertEquals(consumerDiff.receive(2, TimeUnit.SECONDS).getData(), "diff".getBytes());
        }

        // restart broker-1
        broker1.restart();
        Producer<byte[]> producer4 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-4").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> consumer3 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s3").subscribe();
        retryStrategically((test) -> topic2.getProducers().size() == 4, 10, 500);
        assertTrue(topic2.getProducers().size() == 4);
        retryStrategically((test) -> topic2.getSubscription("s3") != null, 10, 500);
        assertFalse(topic2.getSubscription("s3").getConsumers().isEmpty());
        for (int i = 0; i < n; i++) {
            producer4.send("test3".getBytes());
            assertEquals(consumer1.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
            assertEquals(consumer2.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
            assertEquals(consumer3.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
        }

        log.info("Successfully consumed messages by migrated consumers");
    }

    static class TestBroker extends MockedPulsarServiceBaseTest {

        public TestBroker() throws Exception {
            setup();
        }

        @Override
        protected void setup() throws Exception {
            super.internalSetup();
        }

        public PulsarService getPulsarService() {
            return pulsar;
        }

        public String getClusterName() {
            return configClusterName;
        }

        @Override
        protected void cleanup() throws Exception {
            internalCleanup();
        }
        
        public void restart() throws Exception {
            restartBroker();
        }

    }
}
