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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
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

    @Override protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerCursorBackloggedThreshold(10);
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
        // The map topics should only contain partitions, does not contain partitioned topic.
        assertFalse(pulsar.getBrokerService().getTopics().containsKey(topicName));

        // ref of partitioned-topic name should be empty
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
        pulsar.getNamespaceService().unloadNamespaceBundle(bundle, 5, TimeUnit.SECONDS).get();

        for (Producer producer : producerSet) {
            producer.close();
        }
    }

    @DataProvider(name = "closeWithoutWaitingClientDisconnectInFirstBatch")
    public Object[][] closeWithoutWaitingClientDisconnectInFirstBatch() {
        return new Object[][]{
                new Object[] {true},
                new Object[] {false},
        };
    }

    @Test(dataProvider = "closeWithoutWaitingClientDisconnectInFirstBatch")
    public void testConcurrentClose(boolean closeWithoutWaitingClientDisconnectInFirstBatch) throws Exception {
        final String topicName = "persistent://prop/ns/concurrentClose";
        final String ns = "prop/ns";
        admin.namespaces().createNamespace(ns, 1);
        admin.topics().createNonPartitionedTopic(topicName);
        final Topic topic = pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        List<CompletableFuture<Void>> futureList =
                make2ConcurrentBatchesOfClose(topic, 10, closeWithoutWaitingClientDisconnectInFirstBatch);
        Map<Integer, List<CompletableFuture<Void>>> futureMap =
                futureList.stream().collect(Collectors.groupingBy(Objects::hashCode));
        /**
         * The first call: get the return value of "topic.close".
         * The other 19 calls: get the cached value which related {@link PersistentTopic#closeFutures}.
         */
        assertTrue(futureMap.size() <= 3);
        for (List list : futureMap.values()){
            if (list.size() == 1){
                // This is the first call, the future is the return value of `topic.close`.
            } else {
                // Two types future list: wait client close or not.
                assertTrue(list.size() >= 9 && list.size() <= 10);
            }
        }
    }

    private List<CompletableFuture<Void>> make2ConcurrentBatchesOfClose(Topic topic, int tryTimes,
                                                              boolean closeWithoutWaitingClientDisconnectInFirstBatch){
        final List<CompletableFuture<Void>> futureList = Collections.synchronizedList(new ArrayList<>());
        final List<Thread> taskList = new ArrayList<>();
        CountDownLatch allTaskBeginLatch = new CountDownLatch(1);
        // Call a batch of close.
        for (int i = 0; i < tryTimes; i++) {
            Thread thread = new Thread(() -> {
                try {
                    allTaskBeginLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                futureList.add(topic.close(closeWithoutWaitingClientDisconnectInFirstBatch));
            });
            thread.start();
            taskList.add(thread);
        }
        // Call another batch of close.
        for (int i = 0; i < tryTimes; i++) {
            Thread thread = new Thread(() -> {
                try {
                    allTaskBeginLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                futureList.add(topic.close(!closeWithoutWaitingClientDisconnectInFirstBatch));
            });
            thread.start();
            taskList.add(thread);
        }
        // Wait close task executed.
        allTaskBeginLatch.countDown();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->{
            for (Thread thread : taskList){
                if (thread.isAlive()){
                    return false;
                }
            }
            return true;
        });
        return futureList;
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
        String subName = "test_sub";
        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
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
        PrometheusMetricsTestUtil.generate(pulsar, exposeTopicLevelMetrics, true, true, output);
        String metricsStr = output.toString(StandardCharsets.UTF_8);

        Multimap<String, Metric> metricsMap = parseMetrics(metricsStr);
        Collection<Metric> metrics = metricsMap.get("pulsar_delayed_message_index_size_bytes");
        Collection<Metric> subMetrics = metricsMap.get("pulsar_subscription_delayed_message_index_size_bytes");
        assertFalse(metrics.isEmpty());
        if (exposeTopicLevelMetrics) {
            assertFalse(subMetrics.isEmpty());
        } else {
            assertTrue(subMetrics.isEmpty());
        }

        int topicLevelNum = 0;
        int namespaceLevelNum = 0;
        int subscriptionLevelNum = 0;
        for (Metric metric : metrics) {
            if (exposeTopicLevelMetrics && metric.tags.get("topic").equals(topic)) {
                Assert.assertTrue(metric.value > 0);
                topicLevelNum++;
            } else if (!exposeTopicLevelMetrics && metric.tags.get("namespace").equals(namespace)) {
                Assert.assertTrue(metric.value > 0);
                namespaceLevelNum++;
            }
        }
        if (exposeTopicLevelMetrics) {
            for (Metric metric : subMetrics) {
                if (metric.tags.get("topic").equals(topic) &&
                        subName.equals(metric.tags.get("subscription"))) {
                    Assert.assertTrue(metric.value > 0);
                    subscriptionLevelNum++;
                }
            }
        }

        if (exposeTopicLevelMetrics) {
            Assert.assertTrue(topicLevelNum > 0);
            Assert.assertTrue(subscriptionLevelNum > 0);
            Assert.assertEquals(0, namespaceLevelNum);
        } else {
            Assert.assertTrue(namespaceLevelNum > 0);
            Assert.assertEquals(topicLevelNum, 0);
        }

        TopicStats stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("test_sub").getDelayedMessageIndexSizeInBytes() > 0);
        assertTrue(stats.getDelayedMessageIndexSizeInBytes() > 0);
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
        } catch (PulsarClientException.NotAllowedException ex) {
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

    @Test
    public void testDeleteTopicFail() throws Exception {
        final String fullyTopicName = "persistent://prop/ns-abc/" + "tp_"
                + UUID.randomUUID().toString().replaceAll("-", "");
        // Mock topic.
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();

        // Create a sub, and send one message.
        Consumer consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(fullyTopicName).subscriptionName("sub1")
                .subscribe();
        consumer1.close();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(fullyTopicName).create();
        producer.send("1");
        producer.close();

        // Make a failed delete operation.
        AtomicBoolean makeDeletedFailed = new AtomicBoolean(true);
        PersistentTopic persistentTopic = (PersistentTopic) brokerService.getTopic(fullyTopicName, false).get().get();
        doAnswer(invocation -> {
            CompletableFuture future = (CompletableFuture) invocation.getArguments()[1];
            if (makeDeletedFailed.get()) {
                future.completeExceptionally(new RuntimeException("mock ex for test"));
            } else {
                future.complete(null);
            }
            return null;
        }).when(brokerService)
                .deleteTopicAuthenticationWithRetry(any(String.class), any(CompletableFuture.class), anyInt());
        try {
            persistentTopic.delete().get();
        } catch (Exception e) {
            org.testng.Assert.assertTrue(e instanceof ExecutionException);
            org.testng.Assert.assertTrue(e.getCause() instanceof java.lang.RuntimeException);
            org.testng.Assert.assertEquals(e.getCause().getMessage(), "mock ex for test");
        }

        // Assert topic works after deleting failure.
        Consumer consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(fullyTopicName).subscriptionName("sub1")
                .subscribe();
        org.testng.Assert.assertEquals("1", consumer2.receive(2, TimeUnit.SECONDS).getValue());
        consumer2.close();

        // Make delete success.
        makeDeletedFailed.set(false);
        persistentTopic.delete().get();
    }

    @DataProvider(name = "topicLevelPolicy")
    public static Object[][] topicLevelPolicy() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "topicLevelPolicy")
    public void testCreateTopicWithZombieReplicatorCursor(boolean topicLevelPolicy) throws Exception {
        final String namespace = "prop/ns-abc";
        final String topicName = "persistent://" + namespace
                + "/testCreateTopicWithZombieReplicatorCursor" + topicLevelPolicy;
        final String remoteCluster = "remote";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, conf.getReplicatorPrefix() + "." + remoteCluster,
                MessageId.earliest, true);

        admin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl("http://localhost:11112")
                .brokerServiceUrl("pulsar://localhost:11111")
                .build());
        TenantInfo tenantInfo = admin.tenants().getTenantInfo("prop");
        tenantInfo.getAllowedClusters().add(remoteCluster);
        admin.tenants().updateTenant("prop", tenantInfo);

        if (topicLevelPolicy) {
            admin.topics().setReplicationClusters(topicName, Arrays.asList("test", remoteCluster));
        } else {
            admin.namespaces().setNamespaceReplicationClustersAsync(
                    namespace, Sets.newHashSet("test", remoteCluster)).get();
        }

        final PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false)
                .get(3, TimeUnit.SECONDS).orElse(null);
        assertNotNull(topic);

        final Supplier<Set<String>> getCursors = () -> {
            final Set<String> cursors = new HashSet<>();
            final Iterable<ManagedCursor> iterable = topic.getManagedLedger().getCursors();
            iterable.forEach(c -> cursors.add(c.getName()));
            return cursors;
        };
        assertEquals(getCursors.get(), Collections.singleton(conf.getReplicatorPrefix() + "." + remoteCluster));

        // PersistentTopics#onPoliciesUpdate might happen in different threads, so there might be a race between two
        // updates of the replication clusters. So here we sleep for a while to reduce the flakiness.
        Thread.sleep(100);

        // Configure the local cluster to avoid the topic being deleted in PersistentTopics#checkReplication
        if (topicLevelPolicy) {
            admin.topics().setReplicationClusters(topicName, Collections.singletonList("test"));
        } else {
            admin.namespaces().setNamespaceReplicationClustersAsync(namespace, Collections.singleton("test")).get();
        }
        admin.clusters().deleteCluster(remoteCluster);
        // Now the cluster and its related policy has been removed but the replicator cursor still exists

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            log.info("Before initialize...");
            try {
                topic.initialize().get(3, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                log.warn("Failed to initialize: {}", e.getCause().getMessage());
            }
            return !topic.getManagedLedger().getCursors().iterator().hasNext();
        });
    }

    @Test
    public void testCheckPersistencePolicies() throws Exception {
        final String myNamespace = "prop/ns";
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testConfig" + UUID.randomUUID();
        conf.setForceDeleteNamespaceAllowed(true);
        pulsarClient.newProducer().topic(topic).create().close();
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        PersistentTopic persistentTopic = spy((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get());
        TopicPoliciesService policiesService = spy(pulsar.getTopicPoliciesService());
        doReturn(policiesService).when(pulsar).getTopicPoliciesService();
        TopicPolicies policies = new TopicPolicies();
        policies.setRetentionPolicies(retentionPolicies);
        doReturn(CompletableFuture.completedFuture(Optional.of(policies))).when(policiesService).getTopicPoliciesAsync(TopicName.get(topic));
        persistentTopic.onUpdate(policies);
        verify(persistentTopic, times(1)).checkPersistencePolicies();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionSizeInMB(), 1L);
            assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(1));
        });
        // throw exception
        doReturn(CompletableFuture.failedFuture(new RuntimeException())).when(persistentTopic).checkPersistencePolicies();
        policies.setRetentionPolicies(new RetentionPolicies(2, 2));
        persistentTopic.onUpdate(policies);
        assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionSizeInMB(), 1L);
        assertEquals(persistentTopic.getManagedLedger().getConfig().getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(1));
    }

    @Test
    public void testDynamicConfigurationAutoSkipNonRecoverableData() throws Exception {
        pulsar.getConfiguration().setAutoSkipNonRecoverableData(false);
        final String topicName = "persistent://prop/ns-abc/testAutoSkipNonRecoverableData";
        final String subName = "test_sub";

        Consumer<byte[]> subscribe = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subName);

        assertFalse(persistentTopic.ledger.getConfig().isAutoSkipNonRecoverableData());
        assertFalse(subscription.getExpiryMonitor().isAutoSkipNonRecoverableData());

        String key = "autoSkipNonRecoverableData";
        admin.brokers().updateDynamicConfiguration(key, "true");
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin.brokers().getAllDynamicConfigurations().get(key), "true"));

        assertTrue(persistentTopic.ledger.getConfig().isAutoSkipNonRecoverableData());
        assertTrue(subscription.getExpiryMonitor().isAutoSkipNonRecoverableData());

        subscribe.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testCursorGetConfigAfterTopicPoliciesChanged() throws Exception {
        final String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        final String subName = "test_sub";

        @Cleanup
        Consumer<byte[]> subscribe = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subName);

        int maxConsumers = 100;
        admin.topicPolicies().setMaxConsumers(topicName, 100);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getMaxConsumers(topicName, false), maxConsumers);
        });

        ManagedCursorImpl cursor = (ManagedCursorImpl) subscription.getCursor();
        assertEquals(cursor.getConfig(), persistentTopic.getManagedLedger().getConfig());

        subscribe.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testAddWaitingCursorsForNonDurable() throws Exception {
        final String ns = "prop/ns-test";
        admin.namespaces().createNamespace(ns, 2);
        final String topicName = "persistent://prop/ns-test/testAddWaitingCursors";
        admin.topics().createNonPartitionedTopic(topicName);
        final Optional<Topic> topic = pulsar.getBrokerService().getTopic(topicName, false).join();
        assertNotNull(topic.get());
        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl)persistentTopic.getManagedLedger();
        final ManagedCursor spyCursor= spy(ledger.newNonDurableCursor(PositionImpl.LATEST, "sub-2"));
        doAnswer((invocation) -> {
            Thread.sleep(5_000);
            invocation.callRealMethod();
            return null;
        }).when(spyCursor).asyncReadEntriesOrWait(any(int.class), any(long.class),
                any(AsyncCallbacks.ReadEntriesCallback.class), any(Object.class), any(PositionImpl.class));
        Field cursorField = ManagedLedgerImpl.class.getDeclaredField("cursors");
        cursorField.setAccessible(true);
        ManagedCursorContainer container = (ManagedCursorContainer) cursorField.get(ledger);
        container.removeCursor("sub-2");
        container.add(spyCursor, null);
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub-2").subscribe();
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        producer.send("test");
        producer.close();
        final Message<String> receive = consumer.receive();
        assertEquals("test", receive.getValue());
        consumer.close();
        Awaitility.await()
                .pollDelay(5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertEquals(ledger.getWaitingCursorsCount(), 0);
        });
    }

    @Test
    public void testAddWaitingCursorsForNonDurable2() throws Exception {
        final String ns = "prop/ns-test";
        admin.namespaces().createNamespace(ns, 2);
        final String topicName = "persistent://prop/ns-test/testAddWaitingCursors2";
        admin.topics().createNonPartitionedTopic(topicName);
        pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub-1").subscribe().close();
        @Cleanup
        final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(topicName).create();
        for (int i = 0; i < 100; i ++) {
            producer.sendAsync("test-" + i);
        }
        @Cleanup
        final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub-2").subscribe();
        int count = 0;
        while(true) {
            final Message<String> msg = consumer.receive(3, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                count++;
            } else {
                break;
            }
        }
        Assert.assertEquals(count, 100);
        Thread.sleep(3_000);
        for (int i = 0; i < 100; i ++) {
            producer.sendAsync("test-" + i);
        }
        while(true) {
            final Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            if (msg != null) {
                consumer.acknowledge(msg);
                count++;
            } else {
                break;
            }
        }
        Assert.assertEquals(count, 200);
    }
}
