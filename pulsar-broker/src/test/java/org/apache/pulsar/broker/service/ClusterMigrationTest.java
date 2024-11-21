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

import static java.lang.Thread.sleep;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "cluster-migration")
public class ClusterMigrationTest {

    private static final Logger log = LoggerFactory.getLogger(ClusterMigrationTest.class);
    protected String methodName;

    String namespace = "pulsar/migrationNs";
    String namespaceNotToMigrate = "pulsar/notToMigrateNs";
    TestBroker broker1, broker2, broker3, broker4;

    URL url1;
    URL urlTls1;
    PulsarService pulsar1;

    PulsarAdmin admin1;

    URL url2;
    URL urlTls2;
    PulsarService pulsar2;
    PulsarAdmin admin2;

    URL url3;
    URL urlTls3;
    PulsarService pulsar3;
    PulsarAdmin admin3;

    URL url4;
    URL urlTls4;
    PulsarService pulsar4;
    PulsarAdmin admin4;

    String loadManagerClassName;

    @DataProvider(name="NamespaceMigrationTopicSubscriptionTypes")
    public Object[][] namespaceMigrationSubscriptionTypes() {
        return new Object[][] {
            {SubscriptionType.Shared, true, false},
            {SubscriptionType.Shared, false, true},
            {SubscriptionType.Shared, true, true},
        };
    }

    @DataProvider(name = "loadManagerClassName")
    public static Object[][] loadManagerClassName() {
        return new Object[][]{
                {ModularLoadManagerImpl.class.getName()},
                {ExtensibleLoadManagerImpl.class.getName()}
        };
    }

    @Factory(dataProvider = "loadManagerClassName")
    public ClusterMigrationTest(String loadManagerClassName) {
        this.loadManagerClassName = loadManagerClassName;
    }

    @BeforeMethod(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {

        log.info("--- Starting ReplicatorTestBase::setup ---");

        broker1 = new TestBroker("r1", loadManagerClassName);
        broker2 = new TestBroker("r2", loadManagerClassName);
        broker3 = new TestBroker("r3", loadManagerClassName);
        broker4 = new TestBroker("r4", loadManagerClassName);

        pulsar1 = broker1.getPulsarService();
        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        pulsar2 = broker2.getPulsarService();
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        pulsar3 = broker3.getPulsarService();
        url3 = new URL(pulsar3.getWebServiceAddress());
        urlTls3 = new URL(pulsar3.getWebServiceAddressTls());
        admin3 = PulsarAdmin.builder().serviceHttpUrl(url3.toString()).build();

        pulsar4 = broker4.getPulsarService();
        url4 = new URL(pulsar4.getWebServiceAddress());
        urlTls4 = new URL(pulsar4.getWebServiceAddressTls());
        admin4 = PulsarAdmin.builder().serviceHttpUrl(url4.toString()).build();


        admin1.clusters().createCluster("r1",
                ClusterData.builder().serviceUrl(url1.toString()).serviceUrlTls(urlTls1.toString())
                        .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls()).build());
        admin3.clusters().createCluster("r1",
                ClusterData.builder().serviceUrl(url1.toString()).serviceUrlTls(urlTls1.toString())
                        .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls()).build());
        admin2.clusters().createCluster("r2",
                ClusterData.builder().serviceUrl(url2.toString()).serviceUrlTls(urlTls2.toString())
                        .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls()).build());
        admin4.clusters().createCluster("r2",
                ClusterData.builder().serviceUrl(url2.toString()).serviceUrlTls(urlTls2.toString())
                        .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls()).build());

        admin1.clusters().createCluster("r3",
                ClusterData.builder().serviceUrl(url3.toString()).serviceUrlTls(urlTls3.toString())
                        .brokerServiceUrl(pulsar3.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar3.getBrokerServiceUrlTls()).build());

        admin3.clusters().createCluster("r3",
                ClusterData.builder().serviceUrl(url3.toString()).serviceUrlTls(urlTls3.toString())
                        .brokerServiceUrl(pulsar3.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar3.getBrokerServiceUrlTls()).build());

        admin2.clusters().createCluster("r4",
                ClusterData.builder().serviceUrl(url4.toString()).serviceUrlTls(urlTls4.toString())
                        .brokerServiceUrl(pulsar4.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar4.getBrokerServiceUrlTls()).build());
        admin4.clusters().createCluster("r4",
                ClusterData.builder().serviceUrl(url4.toString()).serviceUrlTls(urlTls4.toString())
                        .brokerServiceUrl(pulsar4.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar4.getBrokerServiceUrlTls()).build());

        // Setting r3 as replication cluster for r1
        updateTenantInfo(admin1, "pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r3")));
        updateTenantInfo(admin3, "pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r3")));
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet("r1", "r3"));
        admin3.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r3"));
        admin1.namespaces().createNamespace(namespaceNotToMigrate, Sets.newHashSet("r1", "r3"));
        admin3.namespaces().createNamespace(namespaceNotToMigrate);
        admin1.namespaces().setNamespaceReplicationClusters(namespaceNotToMigrate, Sets.newHashSet("r1", "r3"));

        // Setting r4 as replication cluster for r2
        updateTenantInfo(admin2, "pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r2", "r4")));
        updateTenantInfo(admin4,"pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r2", "r4")));
        admin2.namespaces().createNamespace(namespace, Sets.newHashSet("r2", "r4"));
        admin4.namespaces().createNamespace(namespace);
        admin2.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r2", "r4"));
        admin2.namespaces().createNamespace(namespaceNotToMigrate, Sets.newHashSet("r2", "r4"));
        admin4.namespaces().createNamespace(namespaceNotToMigrate);
        admin2.namespaces().setNamespaceReplicationClusters(namespaceNotToMigrate, Sets.newHashSet("r2", "r4"));

        assertEquals(admin1.clusters().getCluster("r1").getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster("r2").getServiceUrl(), url2.toString());
        assertEquals(admin3.clusters().getCluster("r3").getServiceUrl(), url3.toString());
        assertEquals(admin4.clusters().getCluster("r4").getServiceUrl(), url4.toString());
        assertEquals(admin1.clusters().getCluster("r1").getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r2").getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());
        assertEquals(admin3.clusters().getCluster("r3").getBrokerServiceUrl(), pulsar3.getBrokerServiceUrl());
        assertEquals(admin4.clusters().getCluster("r4").getBrokerServiceUrl(), pulsar4.getBrokerServiceUrl());

        sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    protected void updateTenantInfo(PulsarAdmin admin, String tenant, TenantInfoImpl tenantInfo) throws Exception {
        if (!admin.tenants().getTenants().contains(tenant)) {
            admin.tenants().createTenant(tenant, tenantInfo);
        } else {
            admin.tenants().updateTenant(tenant, tenantInfo);
        }
    }

    @AfterMethod(alwaysRun = true, timeOut = 300000)
    protected void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        admin1.close();
        admin2.close();
        admin3.close();
        admin4.close();
        broker1.cleanup();
        broker2.cleanup();
        broker3.cleanup();
        broker4.cleanup();
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
    @Test
    public void testClusterMigration() throws Exception {
        log.info("--- Starting ReplicatorTest::testClusterMigration ---");
        final String topicName = BrokerTestUtil
                .newUniqueName("persistent://" + namespace + "/migrationTopic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-1 producer/consumer
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
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

        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getWebServiceAddress(), pulsar2.getWebServiceAddressTls(),
                pulsar2.getBrokerServiceUrl(), null);
        admin1.clusters().updateClusterMigration("r1", true, migratedUrl);
        assertEquals(admin1.clusters().getClusterMigration("r1").getMigratedClusterUrl(), migratedUrl);

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

        log.info("before sending message");
        sleep(1000);
        producer1.sendAsync("test1".getBytes());

        // producer is disconnected from cluster-1
        retryStrategically((test) -> topic1.getProducers().isEmpty(), 10, 500);
        log.info("before asserting");
        assertTrue(topic1.getProducers().isEmpty());

        // create 3rd producer on cluster-1 which should be redirected to cluster-2
        Producer<byte[]> producer3 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-2").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        // producer is connected with cluster-2
        retryStrategically((test) -> topic2.getProducers().size() == 3, 10, 500);
        assertTrue(topic2.getProducers().size() == 3);

        // try to consume backlog messages from cluster-1
        consumer1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        for (int i = 0; i < n; i++) {
            Message<byte[]> msg = consumer1.receive();
            assertEquals(msg.getData(), "test1".getBytes());
            consumer1.acknowledge(msg);
        }
        // after consuming all messages, consumer should have disconnected
        // from cluster-1 and reconnect with cluster-2
        retryStrategically((test) -> !topic2.getSubscriptions().isEmpty(), 10, 500);
        assertFalse(topic2.getSubscriptions().isEmpty());

        topic1.checkClusterMigration().get();
        final var replicators = topic1.getReplicators();
        replicators.forEach((r, replicator) -> {
            assertFalse(replicator.isConnected());
        });

        assertTrue(topic1.getSubscriptions().isEmpty());

        // not also create a new consumer which should also reconnect to cluster-2
        Consumer<byte[]> consumer2 = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("s2").subscribe();
        retryStrategically((test) -> topic2.getSubscription("s2") != null, 10, 500);
        assertFalse(topic2.getSubscription("s2").getConsumers().isEmpty());

        // new sub on migration topic must be redirected immediately
        Consumer<byte[]> consumerM = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sM").subscribe();
        assertFalse(pulsar2.getBrokerService().getTopicReference(topicName).get().getSubscription("sM").getConsumers()
                .isEmpty());
        consumerM.close();

        // migrate topic after creating subscription
        String newTopicName = topicName + "-new";
        consumerM = client1.newConsumer().topic(newTopicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sM").subscribe();
        retryStrategically((t) -> pulsar2.getBrokerService().getTopicReference(newTopicName).isPresent(), 5, 100);
        pulsar2.getBrokerService().getTopicReference(newTopicName).get().checkClusterMigration().get();
        retryStrategically((t) ->
        pulsar2.getBrokerService().getTopicReference(newTopicName).isPresent() &&
        pulsar2.getBrokerService().getTopicReference(newTopicName).get().getSubscription("sM")
                .getConsumers().isEmpty(), 5, 100);
        assertFalse(pulsar2.getBrokerService().getTopicReference(newTopicName).get().getSubscription("sM").getConsumers()
                .isEmpty());
        consumerM.close();

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
                .newUniqueName("persistent://" + namespace + "/migrationTopic");
        Consumer<byte[]> consumerDiff = client1.newConsumer().topic(diffTopic).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("s1-d").subscribe();
        Producer<byte[]> producerDiff = client1.newProducer().topic(diffTopic).enableBatching(false)
                .producerName("cluster1-d").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic topicDiff = (AbstractTopic) pulsar2.getBrokerService().getTopic(diffTopic, false).getNow(null).get();
        assertNotNull(topicDiff);
        for (int i = 0; i < n; i++) {
            producerDiff.send("diff".getBytes());
            assertEquals(consumerDiff.receive(2, TimeUnit.SECONDS).getData(), "diff".getBytes());
        }

        // restart broker-1
        broker1.restart();
        Producer<byte[]> producer4 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-4").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> consumer3 = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
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

        client1.close();
        client2.close();
        log.info("Successfully consumed messages by migrated consumers");
    }

    @Test
    public void testClusterMigrationWithReplicationBacklog() throws Exception {
        log.info("--- Starting ReplicatorTest::testClusterMigrationWithReplicationBacklog ---");
        final String topicName = BrokerTestUtil
                .newUniqueName("persistent://" + namespace + "/migrationTopic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client3 = PulsarClient.builder().serviceUrl(url3.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-1 producer/consumer
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("s1").subscribe();

        // cluster-3 consumer
        Consumer<byte[]> consumer3 = client3.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("s1").subscribe();
        AbstractTopic topic1 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName, false).getNow(null).get();
        retryStrategically((test) -> !topic1.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !topic1.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(topic1.getProducers().isEmpty());
        assertFalse(topic1.getSubscriptions().isEmpty());

        // build backlog
        consumer1.close();
        retryStrategically((test) -> topic1.getReplicators().size() == 1, 10, 3000);
        assertEquals(topic1.getReplicators().size(), 1);

       // stop service in the replication cluster to build replication backlog
        broker3.stop();
        retryStrategically((test) -> broker3.getPulsarService() == null, 10, 1000);
        assertNull(pulsar3.getBrokerService());

        //publish messages into topic in "r1" cluster
        int n = 5;
        for (int i = 0; i < n; i++) {
            producer1.send("test1".getBytes());
        }
        retryStrategically((test) -> topic1.isReplicationBacklogExist(), 10, 1000);
        assertTrue(topic1.isReplicationBacklogExist());

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-2 producer/consumer
        Producer<byte[]> producer2 = client2.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster2-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic topic2 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName, false).getNow(null).get();
        log.info("name of topic 2 - {}", topic2.getName());
        assertFalse(topic2.getProducers().isEmpty());

        retryStrategically((test) -> topic2.getReplicators().size() == 1, 10, 2000);
        log.info("replicators should be ready");

        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getWebServiceAddress(), pulsar2.getWebServiceAddressTls(),
                pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls());
        admin1.clusters().updateClusterMigration("r1", true, migratedUrl);
        log.info("update cluster migration called");

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

        // verify that the disconnected producer is not redirected
        // to replication cluster since there is replication backlog.
        assertEquals(topic2.getProducers().size(), 1);

        // Restart the service in cluster "r3".
        broker3.restart();
        retryStrategically((test) -> broker3.getPulsarService() != null, 10, 1000);
        assertNotNull(broker3.getPulsarService());
        pulsar3 = broker3.getPulsarService();

        // verify that the replication backlog drains once service in cluster "r3" is restarted.
        retryStrategically((test) -> !topic1.isReplicationBacklogExist(), 10, 1000);
        assertFalse(topic1.isReplicationBacklogExist());

        // verify that the producer1 is now connected to migrated cluster "r2" since backlog is cleared.
        topic1.checkClusterMigration().get();

        // verify that the producer1 is now is now connected to migrated cluster "r2" since backlog is cleared.
        retryStrategically((test) -> topic2.getProducers().size()==2, 10, 500);
        assertEquals(topic2.getProducers().size(), 2);

        client1.close();
        client2.close();
        client3.close();
    }

    /**
     * This test validates that blue cluster first creates list of subscriptions into green cluster so, green cluster
     * will not lose the data if producer migrates.
     *
     * @throws Exception
     */
    @Test
    public void testClusterMigrationWithResourceCreated() throws Exception {
        log.info("--- Starting testClusterMigrationWithResourceCreated ---");

        String tenant = "pulsar2";
        String namespace = tenant + "/migration";
        String greenClusterName = pulsar2.getConfig().getClusterName();
        String blueClusterName = pulsar1.getConfig().getClusterName();
        admin1.clusters().createCluster(greenClusterName,
                ClusterData.builder().serviceUrl(url2.toString()).serviceUrlTls(urlTls2.toString())
                        .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls()).build());
        admin2.clusters().createCluster(blueClusterName,
                ClusterData.builder().serviceUrl(url1.toString()).serviceUrlTls(urlTls1.toString())
                        .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls()).build());

        admin1.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"),
                Sets.newHashSet("r1", greenClusterName)));
        // broker should handle already tenant creation
        admin2.tenants().createTenant(tenant, new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"),
                Sets.newHashSet("r1", greenClusterName)));
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet("r1", greenClusterName));

        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/migrationTopic");

        broker1.getPulsarService().getConfig().setClusterMigrationAutoResourceCreation(true);
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // cluster-1 producer/consumer
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("cluster1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        // create subscriptions
        admin1.topics().createSubscription(topicName, "s1", MessageId.earliest);
        admin1.topics().createSubscription(topicName, "s2", MessageId.earliest);

        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getWebServiceAddress(), pulsar2.getWebServiceAddressTls(),
                pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls());
        admin1.clusters().updateClusterMigration("r1", true, migratedUrl);

        PersistentTopic topic1 = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).getNow(null)
                .get();
        retryStrategically((test) -> {
            try {
                topic1.checkClusterMigration().get();
                return true;
            } catch (Exception e) {
                // ok
            }
            return false;
        }, 10, 500);

        assertNotNull(admin2.tenants().getTenantInfo(tenant));
        assertNotNull(admin2.namespaces().getPolicies(namespace));
        List<String> subLists = admin2.topics().getSubscriptions(topicName);
        assertTrue(subLists.contains("s1"));
        assertTrue(subLists.contains("s2"));

        int n = 5;
        for (int i = 0; i < n; i++) {
            producer1.send("test1".getBytes());
        }

        Consumer<byte[]> consumer1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        for (int i = 0; i < n; i++) {
            assertNotNull(consumer1.receive());
        }

        consumer1.close();
        producer1.close();

        // publish to new topic which should be redirected immediately
        String newTopic = topicName+"-new";
        producer1 = client1.newProducer().topic(newTopic).enableBatching(false)
                .producerName("cluster1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        retryStrategically((test) -> {
            try {
                pulsar2.getBrokerService().getTopic(newTopic, false).getNow(null).get();
                return true;
            } catch (Exception e) {
                // ok
            }
            return false;
        }, 10, 500);
        PersistentTopic pulsar2Topic = (PersistentTopic) pulsar2.getBrokerService().getTopic(newTopic, false).getNow(null)
                .get();
        retryStrategically((test) -> {
            try {
                return !pulsar2Topic.getProducers().isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, 10, 500);
        assertFalse(pulsar2Topic.getProducers().isEmpty());
        consumer1 = client1.newConsumer().topic(newTopic).subscriptionName("s1").subscribe();
        retryStrategically((test) -> {
            try {
                return !pulsar2Topic.getSubscription("s1").getConsumers().isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, 10, 500);
        assertFalse(pulsar2Topic.getSubscription("s1").getConsumers().isEmpty());

        client1.close();
    }

    @Test(dataProvider = "NamespaceMigrationTopicSubscriptionTypes")
    public void testNamespaceMigration(SubscriptionType subType, boolean isClusterMigrate, boolean isNamespaceMigrate) throws Exception {
        log.info("--- Starting Test::testNamespaceMigration ---");
        // topic for the namespace1 (to be migrated)
        final String topicName = BrokerTestUtil
                .newUniqueName("persistent://" + namespace + "/migrationTopic");
        // topic for namespace2 (not to be migrated)
        final String topicName2 = BrokerTestUtil
                .newUniqueName("persistent://" + namespaceNotToMigrate + "/migrationTopic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // blue cluster - namespace1 - producer/consumer
        Producer<byte[]> blueProducerNs1_1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("blue-producer-ns1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> blueConsumerNs1_1 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s1").subscribe();
        AbstractTopic blueTopicNs1_1 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName, false).getNow(null).get();
        retryStrategically((test) -> !blueTopicNs1_1.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !blueTopicNs1_1.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(blueTopicNs1_1.getProducers().isEmpty());
        assertFalse(blueTopicNs1_1.getSubscriptions().isEmpty());

        // blue cluster - namespace2 - producer/consumer
        Producer<byte[]> blueProducerNs2_1 = client1.newProducer().topic(topicName2).enableBatching(false)
                .producerName("blue-producer-ns2-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> blueConsumerNs2_1 = client1.newConsumer().topic(topicName2).subscriptionType(subType)
                .subscriptionName("s1").subscribe();
        AbstractTopic blueTopicNs2_1 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName2, false).getNow(null).get();
        retryStrategically((test) -> !blueTopicNs2_1.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !blueTopicNs2_1.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(blueTopicNs2_1.getProducers().isEmpty());
        assertFalse(blueTopicNs2_1.getSubscriptions().isEmpty());

        // build backlog on the blue cluster
        blueConsumerNs1_1.close();
        blueConsumerNs2_1.close();
        int n = 5;
        for (int i = 0; i < n; i++) {
            blueProducerNs1_1.send("test1".getBytes());
            blueProducerNs2_1.send("test1".getBytes());
        }

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // green cluster - namespace1 - producer/consumer
        Producer<byte[]> greenProducerNs1_1 = client2.newProducer().topic(topicName).enableBatching(false)
                .producerName("green-producer-ns1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic greenTopicNs1_1 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName, false).getNow(null).get();
        assertFalse(greenTopicNs1_1.getProducers().isEmpty());

        // green cluster - namespace2 - producer/consumer
        Producer<byte[]> greenProducerNs2_1 = client2.newProducer().topic(topicName2).enableBatching(false)
                .producerName("cluster2-nm1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic greenTopicNs2_1 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName2, false).getNow(null).get();
        assertFalse(greenTopicNs2_1.getProducers().isEmpty());

        // blue - green cluster migration
        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getWebServiceAddress(), pulsar2.getWebServiceAddressTls(),
                pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls());
        admin1.clusters().updateClusterMigration("r1", isClusterMigrate, migratedUrl);
        admin1.namespaces().updateMigrationState(namespace, isNamespaceMigrate);

        retryStrategically((test) -> {
            try {
                blueTopicNs1_1.checkClusterMigration().get();
                if (isClusterMigrate) {
                    blueTopicNs2_1.checkClusterMigration().get();
                }
                return true;
            } catch (Exception e) {
                // ok
            }
            return false;
        }, 10, 500);


        blueTopicNs1_1.checkClusterMigration().get();
        if (isClusterMigrate) {
            blueTopicNs2_1.checkClusterMigration().get();
        }

        log.info("before sending message");
        sleep(1000);
        blueProducerNs1_1.sendAsync("test1".getBytes());
        blueProducerNs2_1.sendAsync("test1".getBytes());

        // producer is disconnected from blue for namespace1 as cluster or namespace migration is enabled
        retryStrategically((test) -> blueTopicNs1_1.getProducers().isEmpty(), 10, 500);
        assertTrue(blueTopicNs1_1.getProducers().isEmpty());

        if(isClusterMigrate){
            // producer is disconnected from blue for namespace2 if cluster migration is enabled
            retryStrategically((test) -> blueTopicNs2_1.getProducers().isEmpty(), 10, 500);
            assertTrue(blueTopicNs2_1.getProducers().isEmpty());
        } else {
            // producer is not disconnected from blue for namespace2 if namespace migration is disabled
            retryStrategically((test) -> !blueTopicNs2_1.getProducers().isEmpty(), 10, 500);
            assertTrue(!blueTopicNs2_1.getProducers().isEmpty());
        }

        // create producer on blue which should be redirected to green
        Producer<byte[]> blueProducerNs1_2 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("blue-producer-ns1-2").messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        // producer is connected with green
        retryStrategically((test) -> greenTopicNs1_1.getProducers().size() == 3, 10, 500);
        assertTrue(greenTopicNs1_1.getProducers().size() == 3);

        // blueProducerNs2_1 should be migrated to green if the cluster migration is enabled
        // should not be migrated if the namespace migration is disabled for namespace2
        if (isClusterMigrate) {
            retryStrategically((test) -> greenTopicNs2_1.getProducers().size() == 2, 10, 500);
            assertTrue(greenTopicNs2_1.getProducers().size() == 2);
        } else{
            retryStrategically((test) -> greenTopicNs2_1.getProducers().size() == 1, 10, 500);
            assertTrue(greenTopicNs2_1.getProducers().size() == 1);
        }

        // try to consume backlog messages from cluster-1
        blueConsumerNs1_1 = client1.newConsumer().topic(topicName).subscriptionName("s1").subscribe();
        blueConsumerNs2_1 = client1.newConsumer().topic(topicName2).subscriptionName("s1").subscribe();
        for (int i = 0; i < n; i++) {
            Message<byte[]> msg = blueConsumerNs1_1.receive();
            assertEquals(msg.getData(), "test1".getBytes());
            blueConsumerNs1_1.acknowledge(msg);

            Message<byte[]> msg2 = blueConsumerNs2_1.receive();
            assertEquals(msg2.getData(), "test1".getBytes());
            blueConsumerNs2_1.acknowledge(msg2);
        }
        // after consuming all messages, consumer should have disconnected
        // from blue and reconnect with green
        retryStrategically((test) -> !greenTopicNs1_1.getSubscriptions().isEmpty(), 10, 500);
        assertFalse(greenTopicNs1_1.getSubscriptions().isEmpty());
        if (isClusterMigrate) {
            retryStrategically((test) -> !greenTopicNs2_1.getSubscriptions().isEmpty(), 10, 500);
            assertFalse(greenTopicNs2_1.getSubscriptions().isEmpty());
        } else {
            retryStrategically((test) -> greenTopicNs2_1.getSubscriptions().isEmpty(), 10, 500);
            assertTrue(greenTopicNs2_1.getSubscriptions().isEmpty());
        }

        blueTopicNs1_1.checkClusterMigration().get();
        if (isClusterMigrate) {
            blueTopicNs2_1.checkClusterMigration().get();
        }

        final var replicators = blueTopicNs1_1.getReplicators();
        replicators.forEach((r, replicator) -> {
            assertFalse(replicator.isConnected());
        });
        assertTrue(blueTopicNs1_1.getSubscriptions().isEmpty());

        if (isClusterMigrate) {
            final var replicatorsNm = blueTopicNs2_1.getReplicators();
            replicatorsNm.forEach((r, replicator) -> {
                assertFalse(replicator.isConnected());
            });
            assertTrue(blueTopicNs2_1.getSubscriptions().isEmpty());
        } else {
            final var replicatorsNm = blueTopicNs2_1.getReplicators();
            replicatorsNm.forEach((r, replicator) -> {
                assertTrue(replicator.isConnected());
            });
            assertFalse(blueTopicNs2_1.getSubscriptions().isEmpty());
        }

        // create a new consumer on blue which should also reconnect to green
        Consumer<byte[]> blueConsumerNs1_2 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s2").subscribe();
        Consumer<byte[]> blueConsumerNs2_2 = client1.newConsumer().topic(topicName2).subscriptionType(subType)
                .subscriptionName("s2").subscribe();
        retryStrategically((test) -> greenTopicNs1_1.getSubscription("s2") != null, 10, 500);
        assertFalse(greenTopicNs1_1.getSubscription("s2").getConsumers().isEmpty());
        if (isClusterMigrate) {
            retryStrategically((test) -> greenTopicNs2_1.getSubscription("s2") != null, 10, 500);
            assertFalse(greenTopicNs2_1.getSubscription("s2").getConsumers().isEmpty());
        } else {
            retryStrategically((test) -> greenTopicNs2_1.getSubscription("s2") == null, 10, 500);
        }

        // new sub on migration topic must be redirected immediately
        Consumer<byte[]> consumerM = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("sM").subscribe();
        assertFalse(pulsar2.getBrokerService().getTopicReference(topicName).get().getSubscription("sM").getConsumers()
                .isEmpty());
        consumerM.close();

        // migrate topic after creating subscription
        String newTopicName = topicName + "-new";
        consumerM = client1.newConsumer().topic(newTopicName).subscriptionType(subType)
                .subscriptionName("sM").subscribe();
        retryStrategically((t) -> pulsar1.getBrokerService().getTopicReference(newTopicName).isPresent(), 5, 100);
        pulsar2.getBrokerService().getTopicReference(newTopicName).get().checkClusterMigration().get();
        retryStrategically((t) ->
                pulsar2.getBrokerService().getTopicReference(newTopicName).isPresent() &&
                        pulsar2.getBrokerService().getTopicReference(newTopicName).get().getSubscription("sM")
                                .getConsumers().isEmpty(), 5, 100);
        assertFalse(pulsar2.getBrokerService().getTopicReference(newTopicName).get().getSubscription("sM").getConsumers()
                .isEmpty());
        consumerM.close();

        // publish messages to cluster-2 and consume them
        for (int i = 0; i < n; i++) {
            blueProducerNs1_1.send("test2".getBytes());
            blueProducerNs1_2.send("test2".getBytes());
            greenProducerNs1_1.send("test2".getBytes());
        }
        log.info("Successfully published messages by migrated producers");
        for (int i = 0; i < n * 3; i++) {
            assertEquals(blueConsumerNs1_1.receive(2, TimeUnit.SECONDS).getData(), "test2".getBytes());
            assertEquals(blueConsumerNs1_2.receive(2, TimeUnit.SECONDS).getData(), "test2".getBytes());

        }

        // create non-migrated topic which should connect to blue
        String diffTopic = BrokerTestUtil
                .newUniqueName("persistent://" + namespace + "/migrationTopic");
        Consumer<byte[]> consumerDiff = client1.newConsumer().topic(diffTopic).subscriptionType(subType)
                .subscriptionName("s1-d").subscribe();
        Producer<byte[]> producerDiff = client1.newProducer().topic(diffTopic).enableBatching(false)
                .producerName("cluster1-d").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic topicDiff = (AbstractTopic) pulsar2.getBrokerService().getTopic(diffTopic, false).getNow(null).get();
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
        retryStrategically((test) -> greenTopicNs1_1.getProducers().size() == 4, 10, 500);
        assertTrue(greenTopicNs1_1.getProducers().size() == 4);
        retryStrategically((test) -> greenTopicNs1_1.getSubscription("s3") != null, 10, 500);
        assertFalse(greenTopicNs1_1.getSubscription("s3").getConsumers().isEmpty());
        for (int i = 0; i < n; i++) {
            producer4.send("test3".getBytes());
            assertEquals(blueConsumerNs1_1.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
            assertEquals(blueConsumerNs1_2.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
            assertEquals(consumer3.receive(2, TimeUnit.SECONDS).getData(), "test3".getBytes());
        }

        log.info("Successfully consumed messages by migrated consumers");

        // clean up
        blueConsumerNs1_1.close();
        blueConsumerNs1_2.close();
        blueConsumerNs2_1.close();
        blueProducerNs1_1.close();
        blueProducerNs1_2.close();
        blueProducerNs2_1.close();
        greenProducerNs1_1.close();
        greenProducerNs2_1.close();
        client1.close();
        client2.close();
    }

    @Test(dataProvider = "NamespaceMigrationTopicSubscriptionTypes")
    public void testNamespaceMigrationWithReplicationBacklog(SubscriptionType subType, boolean isClusterMigrate, boolean isNamespaceMigrate) throws Exception {
        log.info("--- Starting ReplicatorTest::testNamespaceMigrationWithReplicationBacklog ---");
        // topic for namespace1 (to be migrated)
        final String topicName = BrokerTestUtil
                .newUniqueName("persistent://" + namespace + "/migrationTopic");
        // topic for namespace2 (not to be migrated)
        final String topicName2 = BrokerTestUtil
                .newUniqueName("persistent://" + namespaceNotToMigrate + "/migrationTopic");

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client3 = PulsarClient.builder().serviceUrl(url3.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // blue cluster - namespace1 - producer/consumer
        Producer<byte[]> blueProducerNs1_1 = client1.newProducer().topic(topicName).enableBatching(false)
                .producerName("blue-producer-ns1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> blueConsumerNs1_1 = client1.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s1").subscribe();

        // blue cluster - namespace2 - producer/consumer
        Producer<byte[]> blueProducerNs2_1 = client1.newProducer().topic(topicName2).enableBatching(false)
                .producerName("blue-producer-ns1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        Consumer<byte[]> blueConsumerNs2_1 = client1.newConsumer().topic(topicName2).subscriptionType(subType)
                .subscriptionName("s1").subscribe();

        // blue cluster replication consumer namespace1
        Consumer<byte[]> blueConsumerReplicationNs1 = client3.newConsumer().topic(topicName).subscriptionType(subType)
                .subscriptionName("s1").subscribe();

        // blue cluster replication consumer namespace2
        Consumer<byte[]> blueConsumerReplicationNs2 = client3.newConsumer().topic(topicName2).subscriptionType(subType)
                .subscriptionName("s1").subscribe();


        AbstractTopic blueTopicNs1 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName, false).getNow(null).get();
        retryStrategically((test) -> !blueTopicNs1.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !blueTopicNs1.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(blueTopicNs1.getProducers().isEmpty());
        assertFalse(blueTopicNs1.getSubscriptions().isEmpty());

        AbstractTopic blueTopicNs2 = (AbstractTopic) pulsar1.getBrokerService().getTopic(topicName2, false).getNow(null).get();
        retryStrategically((test) -> !blueTopicNs2.getProducers().isEmpty(), 5, 500);
        retryStrategically((test) -> !blueTopicNs2.getSubscriptions().isEmpty(), 5, 500);
        assertFalse(blueTopicNs2.getProducers().isEmpty());
        assertFalse(blueTopicNs2.getSubscriptions().isEmpty());

        // build backlog
        blueConsumerNs1_1.close();
        blueConsumerNs2_1.close();
        retryStrategically((test) -> blueTopicNs1.getReplicators().size() == 1, 10, 3000);
        assertEquals(blueTopicNs1.getReplicators().size(), 1);
        retryStrategically((test) -> blueTopicNs2.getReplicators().size() == 1, 10, 3000);
        assertEquals(blueTopicNs2.getReplicators().size(), 1);

        // stop service in the replication cluster to build replication backlog
        broker3.stop();
        retryStrategically((test) -> broker3.getPulsarService() == null, 10, 1000);
        assertNull(pulsar3.getBrokerService());

        //publish messages into topic in blue cluster
        int n = 5;
        for (int i = 0; i < n; i++) {
            blueProducerNs1_1.send("test1".getBytes());
            blueProducerNs2_1.send("test1".getBytes());
        }
        retryStrategically((test) -> blueTopicNs1.isReplicationBacklogExist(), 10, 1000);
        assertTrue(blueTopicNs1.isReplicationBacklogExist());
        retryStrategically((test) -> blueTopicNs2.isReplicationBacklogExist(), 10, 1000);
        assertTrue(blueTopicNs2.isReplicationBacklogExist());

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        // green cluster - namespace1 -  producer/consumer
        Producer<byte[]> greenProducerNs1_1 = client2.newProducer().topic(topicName).enableBatching(false)
                .producerName("green-producer-ns1-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic greenTopicNs1 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName, false).getNow(null).get();
        Producer<byte[]> greenProducerNs2_1 = client2.newProducer().topic(topicName2).enableBatching(false)
                .producerName("green-producer-ns2-1").messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        AbstractTopic greenTopicNs2 = (AbstractTopic) pulsar2.getBrokerService().getTopic(topicName2, false).getNow(null).get();
        log.info("name of topic 2 - {}", greenTopicNs1.getName());
        assertFalse(greenTopicNs1.getProducers().isEmpty());

        retryStrategically((test) -> greenTopicNs1.getReplicators().size() == 1, 10, 2000);
        log.info("replicators should be ready");

        ClusterUrl migratedUrl = new ClusterUrl(pulsar2.getWebServiceAddress(), pulsar2.getWebServiceAddressTls(),
                pulsar2.getBrokerServiceUrl(), pulsar2.getBrokerServiceUrlTls());
        admin1.clusters().updateClusterMigration("r1", isClusterMigrate, migratedUrl);
        admin1.namespaces().updateMigrationState(namespace, isNamespaceMigrate);
        assertEquals(admin1.namespaces().getPolicies(namespace).migrated, isNamespaceMigrate);
        log.info("update cluster migration called");

        retryStrategically((test) -> {
            try {
                blueTopicNs1.checkClusterMigration().get();
                if (isClusterMigrate) {
                    blueTopicNs2.checkClusterMigration().get();
                }
                return true;
            } catch (Exception e) {
                // ok
            }
            return false;
        }, 10, 500);

        blueTopicNs1.checkClusterMigration().get();
        if (isClusterMigrate) {
            blueTopicNs2.checkClusterMigration().get();
        }

        blueProducerNs1_1.sendAsync("test1".getBytes());
        blueProducerNs2_1.sendAsync("test1".getBytes());

        // producer is disconnected from blue
        retryStrategically((test) -> blueTopicNs1.getProducers().isEmpty(), 10, 500);
        assertTrue(blueTopicNs1.getProducers().isEmpty());
        if (isClusterMigrate) {
            retryStrategically((test) -> blueTopicNs2.getProducers().isEmpty(), 10, 500);
            assertTrue(blueTopicNs2.getProducers().isEmpty());
        } else {
            retryStrategically((test) -> !blueTopicNs2.getProducers().isEmpty(), 10, 500);
            assertFalse(blueTopicNs2.getProducers().isEmpty());
        }

        // verify that the disconnected producer is not redirected
        // to replication cluster since there is replication backlog.
        assertEquals(greenTopicNs1.getProducers().size(), 1);

        // Restart the service in cluster "r3".
        broker3.restart();
        retryStrategically((test) -> broker3.getPulsarService() != null, 10, 1000);
        assertNotNull(broker3.getPulsarService());
        pulsar3 = broker3.getPulsarService();

        // verify that the replication backlog drains once service in cluster "r3" is restarted.
        retryStrategically((test) -> !blueTopicNs1.isReplicationBacklogExist(), 10, 1000);
        assertFalse(blueTopicNs1.isReplicationBacklogExist());
        retryStrategically((test) -> !blueTopicNs2.isReplicationBacklogExist(), 10, 1000);
        assertFalse(blueTopicNs2.isReplicationBacklogExist());

        blueTopicNs1.checkClusterMigration().get();
        blueTopicNs2.checkClusterMigration().get();

        // verify that the producer1 is now is now connected to migrated cluster green since backlog is cleared.
        retryStrategically((test) -> greenTopicNs1.getProducers().size()==2, 10, 500);
        assertEquals(greenTopicNs1.getProducers().size(), 2);
        if (isClusterMigrate) {
            retryStrategically((test) -> greenTopicNs2.getProducers().size()==2, 10, 500);
            assertEquals(greenTopicNs2.getProducers().size(), 2);
        } else {
            retryStrategically((test) -> greenTopicNs2.getProducers().size()==1, 10, 500);
            assertEquals(greenTopicNs2.getProducers().size(), 1);
        }

        // clean up
        blueProducerNs1_1.close();
        blueProducerNs2_1.close();
        blueConsumerNs1_1.close();
        blueConsumerNs2_1.close();
        greenProducerNs1_1.close();
        greenProducerNs2_1.close();
        client1.close();
        client2.close();
        client3.close();
    }

    static class TestBroker extends MockedPulsarServiceBaseTest {

        private String clusterName;
        private String loadManagerClassName;

        public TestBroker(String clusterName, String loadManagerClassName) throws Exception {
            this.clusterName = clusterName;
            this.loadManagerClassName = loadManagerClassName;
            setup();
        }

        @Override
        protected void setup() throws Exception {
            super.setupWithClusterName(clusterName);
        }

        @Override
        protected void doInitConf() throws Exception {
            super.doInitConf();
            this.conf.setLoadManagerClassName(loadManagerClassName);
            this.conf.setWebServicePortTls(Optional.of(0));
            this.conf.setBrokerServicePortTls(Optional.of(0));
            this.conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
            this.conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
            this.conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        }


        public PulsarService getPulsarService() {
            return pulsar;
        }

        public String getClusterName() {
            return configClusterName;
        }

        public void stop() throws Exception {
            stopBroker();
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
