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

import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * The tests in this class should be denied in a production pulsar cluster. they are very dangerous, which leads to
 * a lot of topic deletion and makes namespace policies being incorrect.
 */
@Slf4j
@Test(groups = "broker-impl")
public class ReplicatorGlobalNSTest extends ReplicatorTestBase {

    protected String methodName;
    @DataProvider(name = "loadManagerClassName")
    public static Object[][] loadManagerClassName() {
        return new Object[][]{
                {ModularLoadManagerImpl.class.getName()},
                {ExtensibleLoadManagerImpl.class.getName()}
        };
    }

    @Factory(dataProvider = "loadManagerClassName")
    public ReplicatorGlobalNSTest(String loadManagerClassName) {
        this.loadManagerClassName = loadManagerClassName;
    }

    @BeforeMethod
    public void beforeMethod(Method m) {
        methodName = m.getName();
    }

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * If local cluster is removed from the global namespace then all topics under that namespace should be deleted from
     * the cluster.
     *
     * @throws Exception
     */
    @Test(priority = Integer.MAX_VALUE)
    public void testRemoveLocalClusterOnGlobalNamespace() throws Exception {
        log.info("--- Starting ReplicatorTest::testRemoveLocalClusterOnGlobalNamespace ---");

        final String namespace = "pulsar/global/removeClusterTest";
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));

        final String topicName = "persistent://" + namespace + "/topic";

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) client2.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();

        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r2", "r3"));

        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            Assert.assertFalse(pulsar1.getBrokerService().getTopics().containsKey(topicName));
            Assert.assertFalse(producer1.isConnected());
            Assert.assertFalse(consumer1.isConnected());
            Assert.assertTrue(consumer2.isConnected());
        });
    }

    /**
     * This is not a formal operation and can cause serious problems if call it in a production environment.
     */
    @Test(priority = Integer.MAX_VALUE - 1)
    public void testConfigChange() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChange ---");
        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        List<Future<Void>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName.get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/topic-" + i));

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    @Cleanup
                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    @Cleanup
                    MessageConsumer consumer = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    producer.produce(2);
                    consumer.receive(2);
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }

        Thread.sleep(1000L);
        // Make sure that the internal replicators map contains remote cluster info
        final var replicationClients1 = ns1.getReplicationClients();
        final var replicationClients2 = ns2.getReplicationClients();
        final var replicationClients3 = ns3.getReplicationClients();

        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 1: Update the global namespace replication configuration to only contains the local cluster itself
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 2: Update the configuration back
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 3: TODO: Once automatic cleanup is implemented, add tests case to verify auto removal of clusters
    }
}
