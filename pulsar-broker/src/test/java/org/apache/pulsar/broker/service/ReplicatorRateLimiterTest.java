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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "quarantine")
public class ReplicatorRateLimiterTest extends ReplicatorTestBase {

    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
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

    enum DispatchRateType {
        messageRate, byteRate
    }

    @DataProvider(name = "dispatchRateType")
    public Object[][] dispatchRateProvider() {
        return new Object[][] { { DispatchRateType.messageRate }, { DispatchRateType.byteRate } };
    }

    @Test
    public void testReplicatorRatePriority() throws Exception {
        cleanup();
        config1.setSystemTopicEnabled(true);
        config1.setTopicLevelPoliciesEnabled(true);
        config1.setDispatchThrottlingRatePerReplicatorInMsg(100);
        config1.setDispatchThrottlingRatePerReplicatorInByte(200L);
        setup();

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/ratechange";

        admin1.namespaces().createNamespace(namespace);
        // set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        client1.newProducer().topic(topicName).create().close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        //use broker-level by default
        assertTrue(topic.getReplicators().values().get(0).getRateLimiter().isPresent());
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 100);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 200L);

        //set namespace-level policy, which should take effect
        DispatchRate nsDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(50)
                .dispatchThrottlingRateInByte(60)
                .ratePeriodInSecond(60)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, nsDispatchRate);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), nsDispatchRate));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 50);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 60L);

        //set topic-level policy, which should take effect
        DispatchRate topicRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(20)
                .ratePeriodInSecond(30)
                .build();
        admin1.topics().setReplicatorDispatchRate(topicName, topicRate);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin1.topics().getReplicatorDispatchRate(topicName), topicRate));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 10);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //Set the namespace-level policy, which should not take effect
        DispatchRate nsDispatchRate2 = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(500)
                .dispatchThrottlingRateInByte(600)
                .ratePeriodInSecond(700)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, nsDispatchRate2);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), nsDispatchRate2));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), 20L);

        //remove topic-level policy, namespace-level should take effect
        admin1.topics().removeReplicatorDispatchRate(topicName);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin1.topics().getReplicatorDispatchRate(topicName)));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 500);
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
                600L);

        //remove namespace-level policy, broker-level should take effect
        admin1.namespaces().setReplicatorDispatchRate(namespace, null);
        Awaitility.await().untilAsserted(() ->
                assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), 100));
        assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(),
                200L);
    }

    /**
     * verifies dispatch rate for replicators get changed once namespace policies changed.
     *
     * 1. verify default replicator not configured.
     * 2. change namespace setting of replicator dispatchRateMsg, verify topic changed.
     * 3. change namespace setting of replicator dispatchRateByte, verify topic changed.
     *
     * @throws Exception
     */
    @Test
    public void testReplicatorRateLimiterDynamicallyChange() throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatorchange-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/ratechange";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        // 1. default replicator throttling not configured
        Assert.assertFalse(topic.getReplicators().values().get(0).getRateLimiter().isPresent());

        // 2. change namespace setting of replicator dispatchRateMsg, verify topic changed.
        int messageRate = 100;
        DispatchRate dispatchRateMsg = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRateMsg);

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);

        // 3. change namespace setting of replicator dispatchRateByte, verify topic changed.
        messageRate = 500;
        DispatchRate dispatchRateByte = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(messageRate)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRateByte);
        replicatorUpdated = false;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte() == messageRate) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(admin1.namespaces().getReplicatorDispatchRate(namespace), dispatchRateByte);
    }

    /**
     * verifies dispatch rate for replicators works well for both Message limit and Byte limit .
     *
     * 1. verify topic replicator get configured.
     * 2. namespace setting of replicator dispatchRate, verify consumer in other cluster could not receive all messages.
     *
     * @throws Exception
     */
    @Test(dataProvider =  "dispatchRateType")
    public void testReplicatorRateLimiterMessageNotReceivedAllMessages(DispatchRateType dispatchRateType) throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatorbyteandmsg-" + dispatchRateType.toString() + "-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/notReceivedAll";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int messageRate = 100;
        DispatchRate dispatchRate;
        if (DispatchRateType.messageRate.equals(dispatchRateType)) {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(messageRate)
                    .dispatchThrottlingRateInByte(-1)
                    .ratePeriodInSecond(360)
                    .build();
        } else {
            dispatchRate = DispatchRate.builder()
                    .dispatchThrottlingRateInMsg(-1)
                    .dispatchThrottlingRateInByte(messageRate)
                    .ratePeriodInSecond(360)
                    .build();
        }
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        if (DispatchRateType.messageRate.equals(dispatchRateType)) {
            Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);
        } else {
            Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnByte(), messageRate);
        }

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2").messageListener((c1, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            totalReceived.incrementAndGet();
        }).subscribe();

        int numMessages = 500;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertTrue(totalReceived.get() < messageRate * 2);

        consumer.close();
        producer.close();
    }

    /**
     * verifies dispatch rate for replicators works well for both Message limit.
     *
     * 1. verify topic replicator get configured.
     * 2. namespace setting of replicator dispatchRate,
     *      verify consumer in other cluster could receive all messages < message limit.
     * 3. verify consumer in other cluster could not receive all messages > message limit.
     *
     * @throws Exception
     */
    @Test
    public void testReplicatorRateLimiterMessageReceivedAllMessages() throws Exception {
        log.info("--- Starting ReplicatorTest::{} --- ", methodName);

        final String namespace = "pulsar/replicatormsg-" + System.currentTimeMillis();
        final String topicName = "persistent://" + namespace + "/notReceivedAll";

        admin1.namespaces().createNamespace(namespace);
        // 0. set 2 clusters, there will be 1 replicator in each topic
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));

        final int messageRate = 100;
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(messageRate)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(360)
                .build();
        admin1.namespaces().setReplicatorDispatchRate(namespace, dispatchRate);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();

        Producer<byte[]> producer = client1.newProducer().topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getOrCreateTopic(topicName).get();

        boolean replicatorUpdated = false;
        int retry = 5;
        for (int i = 0; i < retry; i++) {
            if (topic.getReplicators().values().get(0).getRateLimiter().isPresent()) {
                replicatorUpdated = true;
                break;
            } else {
                if (i != retry - 1) {
                    Thread.sleep(100);
                }
            }
        }
        Assert.assertTrue(replicatorUpdated);
        Assert.assertEquals(topic.getReplicators().values().get(0).getRateLimiter().get().getDispatchRateOnMsg(), messageRate);

        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
            .build();
        final AtomicInteger totalReceived = new AtomicInteger(0);

        Consumer<byte[]> consumer = client2.newConsumer().topic(topicName).subscriptionName("sub2-in-cluster2").messageListener((c1, msg) -> {
            Assert.assertNotNull(msg, "Message cannot be null");
            String receivedMessage = new String(msg.getData());
            log.debug("Received message [{}] in the listener", receivedMessage);
            totalReceived.incrementAndGet();
        }).subscribe();

        int numMessages = 50;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }

        Thread.sleep(1000);
        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertEquals(totalReceived.get(), numMessages);


        numMessages = 200;
        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            producer.send(new byte[80]);
        }
        Thread.sleep(1000);
        log.info("Received message number: [{}]", totalReceived.get());

        Assert.assertEquals(totalReceived.get(), messageRate);

        consumer.close();
        producer.close();
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorRateLimiterTest.class);
}
