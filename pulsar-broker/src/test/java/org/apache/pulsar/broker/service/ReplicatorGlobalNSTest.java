/**
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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class ReplicatorGlobalNSTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) {
        methodName = m.getName();
    }

    @Override
    @BeforeClass(timeOut = 300000)
    void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(timeOut = 300000)
    void shutdown() throws Exception {
        super.shutdown();
    }

    /**
     * If local cluster is removed from the global namespace then all topics under that namespace should be deleted from
     * the cluster.
     *
     * @throws Exception
     */
    @Test
    public void testRemoveLocalClusterOnGlobalNamespace() throws Exception {
        log.info("--- Starting ReplicatorTest::testRemoveLocalClusterOnGlobalNamespace ---");

        final String namespace = "pulsar/global/removeClusterTest";
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));

        final String topicName = "persistent://" + namespace + "/topic";

        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) client2.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();

        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r2", "r3"));

        MockedPulsarServiceBaseTest
                .retryStrategically((test) -> !pulsar1.getBrokerService().getTopics().containsKey(topicName), 50, 150);

        Assert.assertFalse(pulsar1.getBrokerService().getTopics().containsKey(topicName));
        Assert.assertFalse(producer1.isConnected());
        Assert.assertFalse(consumer1.isConnected());
        Assert.assertTrue(consumer2.isConnected());

        client1.close();
        client2.close();
    }

    @Test
    public void testForcefullyTopicDeletion() throws Exception {
        log.info("--- Starting ReplicatorTest::testForcefullyTopicDeletion ---");

        final String namespace = "pulsar/removeClusterTest";
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1"));

        final String topicName = "persistent://" + namespace + "/topic";

        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        producer1.close();

        admin1.topics().delete(topicName, true);

        MockedPulsarServiceBaseTest
                .retryStrategically((test) -> !pulsar1.getBrokerService().getTopics().containsKey(topicName), 50, 150);

        Assert.assertFalse(pulsar1.getBrokerService().getTopics().containsKey(topicName));

        client1.close();
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorGlobalNSTest.class);

}
