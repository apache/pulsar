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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.PrometheusMetricsTest;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Test validates that broker cleans up topic which failed to unload while bundle unloading.
     *
     * @throws Exception
     */
    @Test
    public void testCleanFailedUnloadTopic() throws Exception {
        final String topicName = "persistent://prop/ns-abc/failedUnload";

        // 1. producer connect
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topicRef);

        ManagedLedger ml = topicRef.ledger;
        LedgerHandle ledger = mock(LedgerHandle.class);
        Field handleField = ml.getClass().getDeclaredField("currentLedger");
        handleField.setAccessible(true);
        handleField.set(ml, ledger);
        doNothing().when(ledger).asyncClose(any(), any());

        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
        pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 5, TimeUnit.SECONDS).get();

        retryStrategically((test) -> !pulsar.getBrokerService().getTopicReference(topicName).isPresent(), 5, 500);
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        producer.close();
    }

    /**
     * Test validates if topic's dispatcher is stuck then broker can doscover and unblock it.
     *
     * @throws Exception
     */
    @Test
    public void testUnblockStuckSubscription() throws Exception {
        final String topicName = "persistent://prop/ns-abc/stuckSubscriptionTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription sharedSub = topic.getSubscription(sharedSubName);
        PersistentSubscription failOverSub = topic.getSubscription(failoverSubName);

        PersistentDispatcherMultipleConsumers sharedDispatcher = (PersistentDispatcherMultipleConsumers) sharedSub
                .getDispatcher();
        PersistentDispatcherSingleActiveConsumer failOverDispatcher = (PersistentDispatcherSingleActiveConsumer) failOverSub
                .getDispatcher();

        // build backlog
        consumer1.close();
        consumer2.close();

        // block sub to read messages
        sharedDispatcher.havePendingRead = true;
        failOverDispatcher.havePendingRead = true;

        producer.newMessage().value("test").eventTime(5).send();
        producer.newMessage().value("test").eventTime(5).send();

        consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionType(SubscriptionType.Shared)
                .subscriptionName(sharedSubName).subscribe();
        consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionType(SubscriptionType.Failover)
                .subscriptionName(failoverSubName).subscribe();
        Message<String> msg = consumer1.receive(2, TimeUnit.SECONDS);
        assertNull(msg);
        msg = consumer2.receive(2, TimeUnit.SECONDS);
        assertNull(msg);

        // allow reads but dispatchers are still blocked
        sharedDispatcher.havePendingRead = false;
        failOverDispatcher.havePendingRead = false;

        // run task to unblock stuck dispatcher: first iteration sets the lastReadPosition and next iteration will
        // unblock the dispatcher read because read-position has not been moved since last iteration.
        sharedSub.checkAndUnblockIfStuck();
        failOverDispatcher.checkAndUnblockIfStuck();
        assertTrue(sharedSub.checkAndUnblockIfStuck());
        assertTrue(failOverDispatcher.checkAndUnblockIfStuck());

        msg = consumer1.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        msg = consumer2.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
    }

    @Test
    public void testDeleteNamespaceInfiniteRetry() throws Exception {
        //init namespace
        final String myNamespace = "prop/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testDeleteNamespaceInfiniteRetry";
        conf.setForceDeleteNamespaceAllowed(true);
        //init topic and policies
        pulsarClient.newProducer().topic(topic).create().close();
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 0);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 0);

        PersistentTopic persistentTopic =
                spy((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get());

        Policies policies = new Policies();
        policies.deleted = true;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(0)).checkReplicationAndRetryOnFailure();

        policies.deleted = false;
        persistentTopic.onPoliciesUpdate(policies);
        verify(persistentTopic, times(1)).checkReplicationAndRetryOnFailure();
    }

    @Test
    public void testAccumulativeStats() throws Exception {
        final String topicName = "persistent://prop/ns-abc/aTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        // stats are at zero before any activity
        TopicStats stats = topic.getStats(false, false, false);
        assertEquals(stats.getBytesInCounter(), 0);
        assertEquals(stats.getMsgInCounter(), 0);
        assertEquals(stats.getBytesOutCounter(), 0);
        assertEquals(stats.getMsgOutCounter(), 0);

        producer.newMessage().value("test").eventTime(5).send();

        Message<String> msg = consumer1.receive();
        assertNotNull(msg);
        msg = consumer2.receive();
        assertNotNull(msg);

        // send/receive result in non-zero stats
        TopicStats statsBeforeUnsubscribe = topic.getStats(false, false, false);
        assertTrue(statsBeforeUnsubscribe.getBytesInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgInCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getBytesOutCounter() > 0);
        assertTrue(statsBeforeUnsubscribe.getMsgOutCounter() > 0);

        consumer1.unsubscribe();
        consumer2.unsubscribe();
        producer.close();
        topic.getProducers().values().forEach(topic::removeProducer);
        assertEquals(topic.getProducers().size(), 0);

        // consumer unsubscribe/producer removal does not result in stats loss
        TopicStats statsAfterUnsubscribe = topic.getStats(false, false, false);
        assertEquals(statsAfterUnsubscribe.getBytesInCounter(), statsBeforeUnsubscribe.getBytesInCounter());
        assertEquals(statsAfterUnsubscribe.getMsgInCounter(), statsBeforeUnsubscribe.getMsgInCounter());
        assertEquals(statsAfterUnsubscribe.getBytesOutCounter(), statsBeforeUnsubscribe.getBytesOutCounter());
        assertEquals(statsAfterUnsubscribe.getMsgOutCounter(), statsBeforeUnsubscribe.getMsgOutCounter());
    }

    @Test
    public void testPersistentPartitionedTopicUnload() throws Exception {
        final String topicName = "persistent://prop/ns/failedUnload";
        final String ns = "prop/ns";
        final int partitions = 5;
        final int producers = 1;
        // ensure that the number of bundle is greater than 1
        final int bundles = 2;

        admin.namespaces().createNamespace(ns, bundles);
        admin.topics().createPartitionedTopic(topicName, partitions);

        List<Producer> producerSet = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            producerSet.add(pulsarClient.newProducer(Schema.STRING).topic(topicName).create());
        }

        assertFalse(pulsar.getBrokerService().getTopics().containsKey(topicName));
        pulsar.getBrokerService().getTopicIfExists(topicName).get();
        assertTrue(pulsar.getBrokerService().getTopics().containsKey(topicName));

        // ref of partitioned-topic name should be empty
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
        pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 5, TimeUnit.SECONDS).get();

        for (Producer producer : producerSet) {
            producer.close();
        }
    }


    @DataProvider(name = "topicAndMetricsLevel")
    public Object[][] indexPatternTestData() {
        return new Object[][]{
                new Object[] {"persistent://prop/autoNs/test_delayed_message_metric", true},
                new Object[] {"persistent://prop/autoNs/test_delayed_message_metric", false},
        };
    }


    @Test(dataProvider = "topicAndMetricsLevel")
    public void testDelayedDeliveryTrackerMemoryUsageMetric(String topic, boolean exposeTopicLevelMetrics) throws Exception {
        PulsarClient client = pulsar.getClient();
        String namespace = TopicName.get(topic).getNamespace();
        admin.namespaces().createNamespace(namespace);

        final int messages = 100;
        CountDownLatch latch = new CountDownLatch(messages);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).enableBatching(false).create();
        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test_sub")
                .subscriptionType(SubscriptionType.Shared)
                .messageListener((MessageListener<String>) (consumer1, msg) -> {
                    try {
                        latch.countDown();
                        consumer1.acknowledge(msg);
                    } catch (PulsarClientException e) {
                        e.printStackTrace();
                    }
                })
                .subscribe();
        for (int a = 0; a < messages; a++) {
            producer.newMessage()
                    .value(UUID.randomUUID().toString())
                    .deliverAfter(30, TimeUnit.SECONDS)
                    .sendAsync();
        }
        producer.flush();

        latch.await(10, TimeUnit.SECONDS);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, exposeTopicLevelMetrics, true, true, output);
        String metricsStr = output.toString(StandardCharsets.UTF_8);

        Multimap<String, PrometheusMetricsTest.Metric> metricsMap = PrometheusMetricsTest.parseMetrics(metricsStr);
        Collection<PrometheusMetricsTest.Metric> metrics = metricsMap.get("pulsar_delayed_message_index_size_bytes");
        Assert.assertTrue(metrics.size() > 0);

        int topicLevelNum = 0;
        int namespaceLevelNum = 0;
        for (PrometheusMetricsTest.Metric metric : metrics) {
            if (exposeTopicLevelMetrics && metric.tags.get("topic").equals(topic)) {
                Assert.assertTrue(metric.value > 0);
                topicLevelNum++;
            } else if (!exposeTopicLevelMetrics && metric.tags.get("namespace").equals(namespace)) {
                Assert.assertTrue(metric.value > 0);
                namespaceLevelNum++;
            }
        }

        if (exposeTopicLevelMetrics) {
            Assert.assertTrue(topicLevelNum > 0);
            Assert.assertEquals(0, namespaceLevelNum);
        } else {
            Assert.assertTrue(namespaceLevelNum > 0);
            Assert.assertEquals(topicLevelNum, 0);
        }
    }

    @Test
    public void testUpdateCursorLastActive() throws Exception {
        final String topicName = "persistent://prop/ns-abc/aTopic";
        final String sharedSubName = "shared";
        final String failoverSubName = "failOver";

        long beforeAddConsumerTimestamp = System.currentTimeMillis();
        Thread.sleep(1);
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Shared).subscriptionName(sharedSubName)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionType(SubscriptionType.Failover).subscriptionName(failoverSubName)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        PersistentSubscription persistentSubscription =  topic.getSubscription(sharedSubName);
        PersistentSubscription persistentSubscription2 =  topic.getSubscription(failoverSubName);

        // `addConsumer` should update last active
        assertTrue(persistentSubscription.getCursor().getLastActive() > beforeAddConsumerTimestamp);
        assertTrue(persistentSubscription2.getCursor().getLastActive() > beforeAddConsumerTimestamp);

        long beforeAckTimestamp = System.currentTimeMillis();
        Thread.sleep(1);
        producer.newMessage().value("test").send();
        Message<String> msg = consumer.receive();
        assertNotNull(msg);
        consumer.acknowledge(msg);
        msg = consumer2.receive();
        assertNotNull(msg);
        consumer2.acknowledge(msg);

        // Make sure ack commands have been sent to broker
        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
                .until(() -> persistentSubscription.getCursor().getLastActive() > beforeAckTimestamp);
        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
                .until(() -> persistentSubscription2.getCursor().getLastActive() > beforeAckTimestamp);

        // `acknowledgeMessage` should update last active
        assertTrue(persistentSubscription.getCursor().getLastActive() > beforeAckTimestamp);
        assertTrue(persistentSubscription2.getCursor().getLastActive() > beforeAckTimestamp);

        long beforeRemoveConsumerTimestamp = System.currentTimeMillis();
        Thread.sleep(1);
        consumer.unsubscribe();
        consumer2.unsubscribe();
        producer.close();

        // `removeConsumer` should update last active
        assertTrue(persistentSubscription.getCursor().getLastActive() > beforeRemoveConsumerTimestamp);
        assertTrue(persistentSubscription2.getCursor().getLastActive() > beforeRemoveConsumerTimestamp);
    }


    @Test
    public void testCreateNonExistentPartitions() throws PulsarAdminException, PulsarClientException {
        final String topicName = "persistent://prop/ns-abc/testCreateNonExistentPartitions";
        admin.topics().createPartitionedTopic(topicName, 4);
        TopicName partition = TopicName.get(topicName).getPartition(4);
        try {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(partition.toString())
                    .create();
            fail("unexpected behaviour");
        } catch (PulsarClientException.TopicDoesNotExistException ignored) {

        }
        Assert.assertEquals(admin.topics().getPartitionedTopicMetadata(topicName).partitions, 4);
    }

    @Test
    public void testCompatibilityWithPartitionKeyword() throws PulsarAdminException, PulsarClientException {
        final String topicName = "persistent://prop/ns-abc/testCompatibilityWithPartitionKeyword";
        TopicName topicNameEntity = TopicName.get(topicName);
        String partition2 = topicNameEntity.getPartition(2).toString();
        // Create a non-partitioned topic with -partition- keyword
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(partition2)
                .create();
        List<String> topics = admin.topics().getList("prop/ns-abc");
        // Close previous producer to simulate reconnect
        producer.close();
        // Disable auto topic creation
        conf.setAllowAutoTopicCreation(false);
        // Check the topic exist in the list.
        Assert.assertTrue(topics.contains(partition2));
        // Check this topic has no partition metadata.
        Assert.assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.topics().getPartitionedTopicMetadata(topicName));
        // Reconnect to the broker and expect successful because the topic has existed in the broker.
        producer = pulsarClient.newProducer()
                .topic(partition2)
                .create();
        producer.close();
        // Check the topic exist in the list again.
        Assert.assertTrue(topics.contains(partition2));
        // Check this topic has no partition metadata again.
        Assert.assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.topics().getPartitionedTopicMetadata(topicName));
    }
}
