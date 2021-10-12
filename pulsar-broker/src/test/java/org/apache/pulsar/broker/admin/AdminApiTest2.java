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
package org.apache.pulsar.broker.admin;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response.Status;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminApiTest.MockedPulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class AdminApiTest2 extends MockedPulsarServiceBaseTest {

    private MockedPulsarService mockPulsarSetup;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setEnableNamespaceIsolationUpdateOnTime(true);
        super.internalSetup();

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();

        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        if (mockPulsarSetup != null) {
            mockPulsarSetup.cleanup();
        }
        resetConfig();
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { TopicDomain.persistent.value() }, { TopicDomain.non_persistent.value() } };
    }

    @DataProvider(name = "namespaceNames")
    public Object[][] namespaceNameProvider() {
        return new Object[][] { { "ns1" }, { "global" } };
    }

    @DataProvider(name = "isV1")
    public Object[][] isV1() {
        return new Object[][] { { true }, { false } };
    }

    /**
     * <pre>
     * It verifies increasing partitions for partitioned-topic.
     * 1. create a partitioned-topic
     * 2. update partitions with larger number of partitions
     * 3. verify: getPartitionedMetadata and check number of partitions
     * 4. verify: this api creates existing subscription to new partitioned-topics
     *            so, message will not be lost in new partitions
     *  a. start producer and produce messages
     *  b. check existing subscription for new topics and it should have backlog msgs
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testIncrementPartitionsOfTopic() throws Exception {
        final String topicName = "increment-partitionedTopic";
        final String subName1 = topicName + "-my-sub-1/encode";
        final String subName2 = topicName + "-my-sub-2/encode";
        final int startPartitions = 4;
        final int newPartitions = 8;
        final String partitionedTopicName = "persistent://prop-xyz/ns1/" + topicName;

        URL pulsarUrl = new URL(pulsar.getWebServiceAddress());

        admin.topics().createPartitionedTopic(partitionedTopicName, startPartitions);
        // validate partition topic is created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                startPartitions);

        // create consumer and subscriptions : check subscriptions
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl.toString()).build();
        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), Lists.newArrayList(subName1));
        Consumer<byte[]> consumer2 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        assertEquals(Sets.newHashSet(admin.topics().getSubscriptions(partitionedTopicName)),
                Sets.newHashSet(subName1, subName2));

        // (1) update partitions
        admin.topics().updatePartitionedTopic(partitionedTopicName, newPartitions);
        // verify new partitions have been created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                newPartitions);
        // (2) No Msg loss: verify new partitions have the same existing subscription names
        final String newPartitionTopicName = TopicName.get(partitionedTopicName).getPartition(startPartitions + 1)
                .toString();

        // (3) produce messages to all partitions including newly created partitions (RoundRobin)
        Producer<byte[]> producer = client.newProducer()
            .topic(partitionedTopicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        final int totalMessages = newPartitions * 2;
        for (int i = 0; i < totalMessages; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        // (4) verify existing subscription has not lost any message: create new consumer with sub-2: it will load all
        // newly created partition topics
        consumer2.close();
        consumer2 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName2)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        assertEquals(Sets.newHashSet(admin.topics().getSubscriptions(newPartitionTopicName)),
                Sets.newHashSet(subName1, subName2));

        assertEquals(Sets.newHashSet(admin.topics().getList("prop-xyz/ns1")).size(), newPartitions);

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet(Lists.newArrayList(subName1, subName2)));
        assertEquals(topicStats.getSubscriptions().get(subName2).getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get(subName2).getMsgBacklog(), totalMessages);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.getPartitions(), Maps.newHashMap());

        // (5) verify: each partition should have backlog
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.getMetadata().partitions, newPartitions);
        Set<String> partitionSet = Sets.newHashSet();
        for (int i = 0; i < newPartitions; i++) {
            partitionSet.add(partitionedTopicName + "-partition-" + i);
        }
        assertEquals(topicStats.getPartitions().keySet(), partitionSet);
        for (int i = 0; i < newPartitions; i++) {
            TopicStats partitionStats = topicStats.getPartitions()
                    .get(TopicName.get(partitionedTopicName).getPartition(i).toString());
            assertEquals(partitionStats.getPublishers().size(), 1);
            assertEquals(partitionStats.getSubscriptions().get(subName2).getConsumers().size(), 1);
            assertEquals(partitionStats.getSubscriptions().get(subName2).getMsgBacklog(), 2, 1);
        }

        producer.close();
        consumer1.close();
        consumer2.close();
        consumer2.close();
    }

    @Test
    public void testTopicPoliciesWithMultiBroker() throws Exception {
        //setup cluster with 3 broker
        cleanup();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl((pulsar.getWebServiceAddress() + ",localhost:1026," + "localhost:2050")).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
        conf.setBrokerServicePort(Optional.of(1024));
        conf.setBrokerServicePortTls(Optional.of(1025));
        conf.setWebServicePort(Optional.of(1026));
        conf.setWebServicePortTls(Optional.of(1027));
        @Cleanup
        PulsarService pulsar2 = startBrokerWithoutAuthorization(conf);
        conf.setBrokerServicePort(Optional.of(2048));
        conf.setBrokerServicePortTls(Optional.of(2049));
        conf.setWebServicePort(Optional.of(2050));
        conf.setWebServicePortTls(Optional.of(2051));
        @Cleanup
        PulsarService pulsar3 = startBrokerWithoutAuthorization(conf);
        @Cleanup
        PulsarAdmin admin2 = PulsarAdmin.builder().serviceHttpUrl(pulsar2.getWebServiceAddress()).build();
        @Cleanup
        PulsarAdmin admin3 = PulsarAdmin.builder().serviceHttpUrl(pulsar3.getWebServiceAddress()).build();

        //for partitioned topic, we can get topic policies from every broker
        final String topic = "persistent://prop-xyz/ns1/" + BrokerTestUtil.newUniqueName("test");
        int partitionNum = 3;
        admin.topics().createPartitionedTopic(topic, partitionNum);
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();

        setTopicPoliciesAndValidate(admin2, admin3, topic);
        //for non-partitioned topic, we can get topic policies from every broker
        final String topic2 = "persistent://prop-xyz/ns1/" + BrokerTestUtil.newUniqueName("test");
        pulsarClient.newConsumer().topic(topic2).subscriptionName("sub").subscribe().close();
        setTopicPoliciesAndValidate(admin2, admin3, topic2);
    }

    private void setTopicPoliciesAndValidate(PulsarAdmin admin2
            , PulsarAdmin admin3, String topic) throws Exception {
        admin.topics().setMaxUnackedMessagesOnConsumer(topic, 100);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getMaxUnackedMessagesOnConsumer(topic)));
        admin.topics().setMaxConsumers(topic, 101);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getMaxConsumers(topic)));
        admin.topics().setMaxProducers(topic, 102);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getMaxProducers(topic)));

        assertEquals(admin.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 100);
        assertEquals(admin2.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 100);
        assertEquals(admin3.topics().getMaxUnackedMessagesOnConsumer(topic).intValue(), 100);
        assertEquals(admin.topics().getMaxConsumers(topic).intValue(), 101);
        assertEquals(admin2.topics().getMaxConsumers(topic).intValue(), 101);
        assertEquals(admin3.topics().getMaxConsumers(topic).intValue(), 101);
        assertEquals(admin.topics().getMaxProducers(topic).intValue(), 102);
        assertEquals(admin2.topics().getMaxProducers(topic).intValue(), 102);
        assertEquals(admin3.topics().getMaxProducers(topic).intValue(), 102);
    }

    /**
     * verifies admin api command for non-persistent topic. It verifies: partitioned-topic, stats
     *
     * @throws Exception
     */
    @Test
    public void nonPersistentTopics() throws Exception {
        final String topicName = "nonPersistentTopic";

        final String nonPersistentTopicName = "non-persistent://prop-xyz/ns1/" + topicName;
        // Force to create a topic
        publishMessagesOnTopic(nonPersistentTopicName, 0, 0);

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(nonPersistentTopicName).subscriptionName("my-sub")
                .subscribe();

        publishMessagesOnTopic(nonPersistentTopicName, 10, 0);

        NonPersistentTopicStats topicStats = (NonPersistentTopicStats) admin.topics().getStats(nonPersistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
        assertEquals(topicStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgDropRate(), 0);
        assertEquals(topicStats.getPublishers().size(), 0);
        assertEquals(topicStats.getMsgDropRate(), 0);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(nonPersistentTopicName, false);
        assertEquals(internalStats.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));

        consumer.close();
        topicStats = (NonPersistentTopicStats) admin.topics().getStats(nonPersistentTopicName);
        assertTrue(topicStats.getSubscriptions().containsKey("my-sub"));
        assertEquals(topicStats.getPublishers().size(), 0);
        // test partitioned-topic
        final String partitionedTopicName = "non-persistent://prop-xyz/ns1/paritioned";
        admin.topics().createPartitionedTopic(partitionedTopicName, 5);
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 5);
    }

    private void publishMessagesOnTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }

    /**
     * verifies validation on persistent-policies
     *
     * @throws Exception
     */
    @Test
    public void testSetPersistencePolicies() throws Exception {

        final String namespace = "prop-xyz/ns2";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        assertNull(admin.namespaces().getPersistence(namespace));
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 10.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 10.0));

        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 4, 3, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 4, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(6, 3, 1, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 400);
        }

        // make sure policies has not been changed
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 10.0));
    }

    /**
     * validates update of persistent-policies reflects on managed-ledger and managed-cursor
     *
     * @throws Exception
     */
    @Test
    public void testUpdatePersistencePolicyUpdateManagedCursor() throws Exception {

        final String namespace = "prop-xyz/ns2";
        final String topicName = "persistent://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 50.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 50.0));

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.getCursors().iterator().next();

        final double newThrottleRate = 100;
        final int newEnsembleSize = 5;
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(newEnsembleSize, 3, 3, newThrottleRate));

        retryStrategically((test) -> managedLedger.getConfig().getEnsembleSize() == newEnsembleSize
                && cursor.getThrottleMarkDelete() != newThrottleRate, 5, 200);

        // (1) verify cursor.markDelete has been updated
        assertEquals(cursor.getThrottleMarkDelete(), newThrottleRate);

        // (2) verify new ledger creation takes new config
        producer.close();
        consumer.close();
    }

    /**
     * Verify unloading topic
     *
     * @throws Exception
     */
    @Test(dataProvider = "topicType")
    public void testUnloadTopic(final String topicType) throws Exception {

        final String namespace = "prop-xyz/ns2";
        final String topicName = topicType + "://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        // create a topic by creating a producer
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        producer.close();

        Topic topic = pulsar.getBrokerService().getTopicIfExists(topicName).join().get();
        final boolean isPersistentTopic = topic instanceof PersistentTopic;

        // (1) unload the topic
        unloadTopic(topicName);

        // topic must be removed from map
        assertFalse(pulsar.getBrokerService().getTopicReference(topicName).isPresent());

        // recreation of producer will load the topic again
        pulsarClient.newProducer().topic(topicName).create();
        topic = pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topic);
        // unload the topic
        unloadTopic(topicName);
        // producer will retry and recreate the topic
        Awaitility.await().until(() -> pulsar.getBrokerService().getTopicReference(topicName).isPresent());
        // topic should be loaded by this time
        topic = pulsar.getBrokerService().getTopicReference(topicName).get();
        assertNotNull(topic);
    }

    private void unloadTopic(String topicName) throws Exception {
        admin.topics().unload(topicName);
    }

    /**
     * Verifies reset-cursor at specific position using admin-api.
     *
     * <pre>
     * 1. Publish 50 messages
     * 2. Consume 20 messages
     * 3. reset cursor position on 10th message
     * 4. consume 40 messages from reset position
     * </pre>
     *
     * @param namespaceName
     * @throws Exception
     */
    @Test(dataProvider = "namespaceNames", timeOut = 10000)
    public void testResetCursorOnPosition(String namespaceName) throws Exception {
        final String topicName = "persistent://prop-xyz/use/" + namespaceName + "/resetPosition";
        final int totalProducedMessages = 50;

        // set retention
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, totalProducedMessages, 0);

        List<Message<byte[]>> messages = admin.topics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        Message<byte[]> message = null;
        MessageIdImpl resetMessageId = null;
        int resetPositionId = 10;
        for (int i = 0; i < 20; i++) {
            message = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            if (i == resetPositionId) {
                resetMessageId = (MessageIdImpl) message.getMessageId();
            }
        }

        // close consumer which will clean up internal-receive-queue
        consumer.close();

        // messages should still be available due to retention
        MessageIdImpl messageId = new MessageIdImpl(
                resetMessageId.getLedgerId(),
                resetMessageId.getEntryId(),
                -1);
        // reset position at resetMessageId
        admin.topics().resetCursor(topicName, "my-sub", messageId);

        consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();
        MessageIdImpl msgId2 = (MessageIdImpl) consumer.receive(1, TimeUnit.SECONDS).getMessageId();
        assertEquals(resetMessageId, msgId2);

        int receivedAfterReset = 1; // start with 1 because we have already received 1 msg

        for (int i = 0; i < totalProducedMessages; i++) {
            message = consumer.receive(500, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
            ++receivedAfterReset;
        }
        assertEquals(receivedAfterReset, totalProducedMessages - resetPositionId);

        // invalid topic name
        try {
            admin.topics().resetCursor(topicName + "invalid", "my-sub", messageId);
            fail("It should have failed due to invalid topic name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid cursor name
        try {
            admin.topics().resetCursor(topicName, "invalid-sub", messageId);
            fail("It should have failed due to invalid subscription name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid position
        try {
            messageId = new MessageIdImpl(0, 0, -1);
            admin.topics().resetCursor(topicName, "my-sub", messageId);
        } catch (PulsarAdminException.PreconditionFailedException e) {
            fail("It shouldn't fail for a invalid position");
        }

        consumer.close();
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }


    @Test(timeOut = 20000)
    public void testMaxConsumersOnSubApi() throws Exception {
        final String namespace = "prop-xyz/ns1";
        assertNull(admin.namespaces().getMaxConsumersPerSubscription(namespace));
        admin.namespaces().setMaxConsumersPerSubscription(namespace, 10);
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.namespaces().getMaxConsumersPerSubscription(namespace));
            assertEquals(admin.namespaces().getMaxConsumersPerSubscription(namespace).intValue(), 10);
        });
        admin.namespaces().removeMaxConsumersPerSubscription(namespace);
        Awaitility.await().untilAsserted(() ->
                admin.namespaces().getMaxConsumersPerSubscription(namespace));
    }

    /**
     * It verifies that pulsar with different load-manager generates different load-report and returned by admin-api
     *
     * @throws Exception
     */
    @Test
    public void testLoadReportApi() throws Exception {

        this.conf.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        MockedPulsarService mockPulsarSetup1 = new MockedPulsarService(this.conf);
        mockPulsarSetup1.setup();
        PulsarAdmin simpleLoadManagerAdmin = mockPulsarSetup1.getAdmin();
        assertNotNull(simpleLoadManagerAdmin.brokerStats().getLoadReport());

        this.conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        MockedPulsarService mockPulsarSetup2 = new MockedPulsarService(this.conf);
        mockPulsarSetup2.setup();
        PulsarAdmin modularLoadManagerAdmin = mockPulsarSetup2.getAdmin();
        assertNotNull(modularLoadManagerAdmin.brokerStats().getLoadReport());

        mockPulsarSetup1.cleanup();
        mockPulsarSetup2.cleanup();
    }

    @Test
    public void testPeerCluster() throws Exception {
        admin.clusters().createCluster("us-west1",
                ClusterData.builder().serviceUrl("http://broker.messaging.west1.example.com:8080").build());
        admin.clusters().createCluster("us-west2",
                ClusterData.builder().serviceUrl("http://broker.messaging.west2.example.com:8080").build());
        admin.clusters().createCluster("us-east1",
                ClusterData.builder().serviceUrl("http://broker.messaging.east1.example.com:8080").build());
        admin.clusters().createCluster("us-east2",
                ClusterData.builder().serviceUrl("http://broker.messaging.east2.example.com:8080").build());

        admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(Lists.newArrayList("us-west2")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(), Lists.newArrayList("us-west2"));
        assertNull(admin.clusters().getCluster("us-west2").getPeerClusterNames());
        // update cluster with duplicate peer-clusters in the list
        admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(
                Lists.newArrayList("us-west2", "us-east1", "us-west2", "us-east1", "us-west2", "us-east1")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                Lists.newArrayList("us-west2", "us-east1"));
        admin.clusters().updatePeerClusterNames("us-west1", null);
        assertNull(admin.clusters().getCluster("us-west1").getPeerClusterNames());

        // Check name validation
        try {
            admin.clusters().updatePeerClusterNames("us-west1",
                    Sets.newLinkedHashSet(Lists.newArrayList("invalid-cluster")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // Cluster itself can't be part of peer-list
        try {
            admin.clusters().updatePeerClusterNames("us-west1", Sets.newLinkedHashSet(Lists.newArrayList("us-west1")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    /**
     * It validates that peer-cluster can't coexist in replication-cluster list
     *
     * @throws Exception
     */
    @Test
    public void testReplicationPeerCluster() throws Exception {
        admin.clusters().createCluster("us-west1",
                ClusterData.builder().serviceUrl("http://broker.messaging.west1.example.com:8080").build());
        admin.clusters().createCluster("us-west2",
                ClusterData.builder().serviceUrl("http://broker.messaging.west2.example.com:8080").build());
        admin.clusters().createCluster("us-west3",
                ClusterData.builder().serviceUrl("http://broker.messaging.west2.example.com:8080").build());
        admin.clusters().createCluster("us-west4",
                ClusterData.builder().serviceUrl("http://broker.messaging.west2.example.com:8080").build());
        admin.clusters().createCluster("us-east1",
                ClusterData.builder().serviceUrl("http://broker.messaging.east1.example.com:8080").build());
        admin.clusters().createCluster("us-east2",
                ClusterData.builder().serviceUrl("http://broker.messaging.east2.example.com:8080").build());
        admin.clusters().createCluster("global", ClusterData.builder().build());

        List<String> allClusters = admin.clusters().getClusters();
        Collections.sort(allClusters);
        assertEquals(allClusters,
                Lists.newArrayList("test", "us-east1", "us-east2", "us-west1", "us-west2", "us-west3", "us-west4"));

        final String property = "peer-prop";
        Set<String> allowedClusters = Sets.newHashSet("us-west1", "us-west2", "us-west3", "us-west4", "us-east1",
                "us-east2", "global");
        TenantInfoImpl propConfig = new TenantInfoImpl(Sets.newHashSet("test"), allowedClusters);
        admin.tenants().createTenant(property, propConfig);

        final String namespace = property + "/global/conflictPeer";
        admin.namespaces().createNamespace(namespace);

        admin.clusters().updatePeerClusterNames("us-west1",
                Sets.newLinkedHashSet(Lists.newArrayList("us-west2", "us-west3")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                Lists.newArrayList("us-west2", "us-west3"));

        // (1) no conflicting peer
        Set<String> clusterIds = Sets.newHashSet("us-east1", "us-east2");
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        // (2) conflicting peer
        clusterIds = Sets.newHashSet("us-west2", "us-west3", "us-west1");
        try {
            admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
            fail("Peer-cluster can't coexist in replication cluster list");
        } catch (PulsarAdminException.ConflictException e) {
            // Ok
        }

        clusterIds = Sets.newHashSet("us-west2", "us-west3");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        clusterIds = Sets.newHashSet("us-west1", "us-west4");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
    }

    @Test
    public void clusterFailureDomain() throws PulsarAdminException {

        final String cluster = pulsar.getConfiguration().getClusterName();
        // create
        FailureDomain domain = FailureDomain.builder()
                .brokers(Sets.newHashSet("b1", "b2", "b3"))
                .build();
        admin.clusters().createFailureDomain(cluster, "domain-1", domain);
        admin.clusters().updateFailureDomain(cluster, "domain-1", domain);

        assertEquals(admin.clusters().getFailureDomain(cluster, "domain-1"), domain);

        Map<String, FailureDomain> domains = admin.clusters().getFailureDomains(cluster);
        assertEquals(domains.size(), 1);
        assertTrue(domains.containsKey("domain-1"));

        try {
            // try to create domain with already registered brokers
            admin.clusters().createFailureDomain(cluster, "domain-2", domain);
            fail("should have failed because of brokers are already registered");
        } catch (PulsarAdminException.ConflictException e) {
            // Ok
        }

        admin.clusters().deleteFailureDomain(cluster, "domain-1");
        assertTrue(admin.clusters().getFailureDomains(cluster).isEmpty());

        admin.clusters().createFailureDomain(cluster, "domain-2", domain);
        domains = admin.clusters().getFailureDomains(cluster);
        assertEquals(domains.size(), 1);
        assertTrue(domains.containsKey("domain-2"));
    }

    @Test
    public void namespaceAntiAffinity() throws PulsarAdminException {
        final String namespace = "prop-xyz/ns1";
        final String antiAffinityGroup = "group";
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));
        admin.namespaces().setNamespaceAntiAffinityGroup(namespace, antiAffinityGroup);
        assertEquals(admin.namespaces().getNamespaceAntiAffinityGroup(namespace), antiAffinityGroup);
        admin.namespaces().deleteNamespaceAntiAffinityGroup(namespace);
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));

        final String ns1 = "prop-xyz/antiAG1";
        final String ns2 = "prop-xyz/antiAG2";
        final String ns3 = "prop-xyz/antiAG3";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns2, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns3, Sets.newHashSet("test"));
        admin.namespaces().setNamespaceAntiAffinityGroup(ns1, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns2, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns3, antiAffinityGroup);

        Set<String> namespaces = new HashSet<>(
                admin.namespaces().getAntiAffinityNamespaces("prop-xyz", "test", antiAffinityGroup));
        assertEquals(namespaces.size(), 3);
        assertTrue(namespaces.contains(ns1));
        assertTrue(namespaces.contains(ns2));
        assertTrue(namespaces.contains(ns3));

        List<String> namespaces2 = admin.namespaces().getAntiAffinityNamespaces("prop-xyz", "test", "invalid-group");
        assertEquals(namespaces2.size(), 0);
    }

    @Test
    public void testNonPersistentTopics() throws Exception {
        final String namespace = "prop-xyz/ns2";
        final String topicName = "non-persistent://" + namespace + "/topic";
        admin.namespaces().createNamespace(namespace, 20);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("test"));
        int totalTopics = 100;

        Set<String> topicNames = Sets.newHashSet();
        for (int i = 0; i < totalTopics; i++) {
            topicNames.add(topicName + i);
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName + i).create();
            producer.close();
        }

        for (int i = 0; i < totalTopics; i++) {
            Topic topic = pulsar.getBrokerService().getTopicReference(topicName + i).get();
            assertNotNull(topic);
        }

        Set<String> topicsInNs = Sets.newHashSet(admin.topics().getList(namespace));
        assertEquals(topicsInNs.size(), totalTopics);
        topicsInNs.removeAll(topicNames);
        assertEquals(topicsInNs.size(), 0);
    }

    @Test
    public void testPublishConsumerStats() throws Exception {
        final String topicName = "statTopic";
        final String subscriberName = topicName + "-my-sub-1";
        final String topic = "persistent://prop-xyz/ns1/" + topicName;
        final String producerName = "myProducer";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(subscriberName)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .producerName(producerName)
            .create();

        retryStrategically((test) -> {
            TopicStats stats;
            try {
                stats = admin.topics().getStats(topic);
                return stats.getPublishers().size() > 0 && stats.getSubscriptions().get(subscriberName) != null
                        && stats.getSubscriptions().get(subscriberName).getConsumers().size() > 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertNotNull(topicStats.getPublishers().get(0).getAddress());
        assertNotNull(topicStats.getPublishers().get(0).getClientVersion());
        assertNotNull(topicStats.getPublishers().get(0).getConnectedSince());
        assertNotNull(topicStats.getPublishers().get(0).getProducerName());
        assertEquals(topicStats.getPublishers().get(0).getProducerName(), producerName);

        SubscriptionStats subscriber = topicStats.getSubscriptions().get(subscriberName);
        assertNotNull(subscriber);
        assertEquals(subscriber.getConsumers().size(), 1);
        ConsumerStats consumerStats = subscriber.getConsumers().get(0);
        assertNotNull(consumerStats.getAddress());
        assertNotNull(consumerStats.getClientVersion());
        assertNotNull(consumerStats.getConnectedSince());

        producer.close();
        consumer.close();
    }

    @Test
    public void testTenantNameWithUnderscore() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop_xyz", tenantInfo);

        admin.namespaces().createNamespace("prop_xyz/my-namespace", Sets.newHashSet("test"));

        String topic = "persistent://prop_xyz/use/my-namespace/my-topic";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();

        TopicStats stats = admin.topics().getStats(topic);
        assertEquals(stats.getPublishers().size(), 1);
        producer.close();
    }

    @Test
    public void testTenantNameWithInvalidCharacters() {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));

        // If we try to create property with invalid characters, it should fail immediately
        try {
            admin.tenants().createTenant("prop xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        try {
            admin.tenants().createTenant("prop&xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void testTenantWithNonexistentClusters() throws Exception {
        // Check non-existing cluster
        assertFalse(admin.clusters().getClusters().contains("cluster-non-existing"));

        Set<String> allowedClusters = Sets.newHashSet("cluster-non-existing");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), allowedClusters);

        // If we try to create tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().createTenant("test-tenant", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        assertFalse(admin.tenants().getTenants().contains("test-tenant"));

        // Check existing tenant
        assertTrue(admin.tenants().getTenants().contains("prop-xyz"));

        // If we try to update existing tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().updateTenant("prop-xyz", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void brokerNamespaceIsolationPolicies() throws Exception {

        // create
        String policyName1 = "policy-1";
        String cluster = pulsar.getConfiguration().getClusterName();
        String namespaceRegex = "other/" + cluster + "/other.*";
        String brokerName = pulsar.getAdvertisedAddress();
        String brokerAddress = brokerName + ":" + pulsar.getConfiguration().getWebServicePort().get();

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");

        NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList(namespaceRegex))
                .primary(Collections.singletonList(brokerName + ":[0-9]*"))
                .secondary(Collections.singletonList(brokerName + ".*"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters1)
                        .build())
                .build();
        admin.clusters().createNamespaceIsolationPolicy(cluster, policyName1, nsPolicyData1);

        List<BrokerNamespaceIsolationData> brokerIsolationDataList = admin.clusters()
                .getBrokersWithNamespaceIsolationPolicy(cluster);
        assertEquals(brokerIsolationDataList.size(), 1);
        assertEquals(brokerIsolationDataList.get(0).getBrokerName(), brokerAddress);
        assertEquals(brokerIsolationDataList.get(0).getNamespaceRegex().size(), 1);
        assertEquals(brokerIsolationDataList.get(0).getNamespaceRegex().get(0), namespaceRegex);

        BrokerNamespaceIsolationDataImpl brokerIsolationData = (BrokerNamespaceIsolationDataImpl) admin.clusters()
                .getBrokerWithNamespaceIsolationPolicy(cluster, brokerAddress);
        assertEquals(brokerIsolationData.getBrokerName(), brokerAddress);
        assertEquals(brokerIsolationData.getNamespaceRegex().size(), 1);
        assertEquals(brokerIsolationData.getNamespaceRegex().get(0), namespaceRegex);

        BrokerNamespaceIsolationDataImpl isolationData = (BrokerNamespaceIsolationDataImpl) admin.clusters()
                .getBrokerWithNamespaceIsolationPolicy(cluster, "invalid-broker");
        assertFalse(isolationData.isPrimary());
    }

    // create 1 namespace:
    //  0. without isolation policy configured, lookup will success.
    //  1. with matched isolation broker configured and matched, lookup will success.
    //  2. update isolation policy, without broker matched, lookup will fail.
    @Test
    public void brokerNamespaceIsolationPoliciesUpdateOnTime() throws Exception {
        String brokerName = pulsar.getAdvertisedAddress();
        String ns1Name = "prop-xyz/test_ns1_iso_" + System.currentTimeMillis();
        admin.namespaces().createNamespace(ns1Name, Sets.newHashSet("test"));

        //  0. without isolation policy configured, lookup will success.
        String brokerUrl = admin.lookups().lookupTopic(ns1Name + "/topic1");
        assertTrue(brokerUrl.contains(brokerName));
        log.info("0 get lookup url {}", brokerUrl);

        // create
        String policyName1 = "policy-1";
        String cluster = pulsar.getConfiguration().getClusterName();

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");

        NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList(ns1Name))
                .primary(Collections.singletonList(brokerName + ".*"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters1)
                        .build())
                .build();
        admin.clusters().createNamespaceIsolationPolicyAsync(cluster, policyName1, nsPolicyData1).get();

        //  1. with matched isolation broker configured and matched, lookup will success.
        brokerUrl = admin.lookups().lookupTopic(ns1Name + "/topic2");
        assertTrue(brokerUrl.contains(brokerName));
        log.info(" 1 get lookup url {}", brokerUrl);

        //  2. update isolation policy, without broker matched, lookup will fail.
        nsPolicyData1.getPrimary().clear();;
        nsPolicyData1.getPrimary().add(brokerName + "not_match");
        admin.clusters().updateNamespaceIsolationPolicyAsync(cluster, policyName1, nsPolicyData1).get();

        try {
            admin.lookups().lookupTopic(ns1Name + "/topic3");
        } catch (Exception e) {
            // expected lookup fail, because no brokers matched the policy.
            log.info(" 2 expected fail lookup");
        }

        try {
            admin.lookups().lookupTopic(ns1Name + "/topic1");
        } catch (Exception e) {
            // expected lookup fail, because no brokers matched the policy.
            log.info(" 22 expected fail lookup");
        }
    }

    @Test
    public void clustersList() throws PulsarAdminException {
        final String cluster = pulsar.getConfiguration().getClusterName();
        admin.clusters().createCluster("global", ClusterData.builder()
                .serviceUrl("http://localhost:6650").build());

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList(cluster));
    }
    /**
     * verifies cluster has been set before create topic
     *
     * @throws PulsarAdminException
     */
    @Test
    public void testClusterIsReadyBeforeCreateTopic() throws PulsarAdminException {
        final String topicName = "partitionedTopic";
        final int partitions = 4;
        final String persistentPartitionedTopicName = "persistent://prop-xyz/ns2/" + topicName;
        final String NonPersistentPartitionedTopicName = "non-persistent://prop-xyz/ns2/" + topicName;

        admin.namespaces().createNamespace("prop-xyz/ns2");
        // By default the cluster will configure as configuration file. So the create topic operation
        // will never throw exception except there is no cluster.
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns2", new HashSet<String>());

        try {
            admin.topics().createPartitionedTopic(persistentPartitionedTopicName, partitions);
            Assert.fail("should have failed due to Namespace does not have any clusters configured");
        } catch (PulsarAdminException.PreconditionFailedException ignored) {
        }

        try {
            admin.topics().createPartitionedTopic(NonPersistentPartitionedTopicName, partitions);
            Assert.fail("should have failed due to Namespace does not have any clusters configured");
        } catch (PulsarAdminException.PreconditionFailedException ignored) {
        }
    }

    @Test
    public void testCreateNamespaceWithNoClusters() throws PulsarAdminException {
        String localCluster = pulsar.getConfiguration().getClusterName();
        String namespace = "prop-xyz/test-ns-with-no-clusters";
        admin.namespaces().createNamespace(namespace);

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.namespaces().getNamespaceReplicationClusters(namespace),
                Collections.singletonList(localCluster));
    }

    @Test(timeOut = 30000)
    public void testConsumerStatsLastTimestamp() throws PulsarClientException, PulsarAdminException, InterruptedException {
        long timestamp = System.currentTimeMillis();
        final String topicName = "consumer-stats-" + timestamp;
        final String subscribeName = topicName + "-test-stats-sub";
        final String topic = "persistent://prop-xyz/ns1/" + topicName;
        final String producerName = "producer-" + topicName;

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        Producer<byte[]> producer = client.newProducer().topic(topic)
            .enableBatching(false)
            .producerName(producerName)
            .create();

        // a. Send a message to the topic.
        producer.send("message-1".getBytes(StandardCharsets.UTF_8));

        // b. Create a consumer, because there was a message in the topic, the consumer will receive the message pushed
        // by the broker, the lastConsumedTimestamp will as the consume subscribe time.
        Consumer<byte[]> consumer = client.newConsumer().topic(topic)
            .subscriptionName(subscribeName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();
        Message<byte[]> message = consumer.receive();

        // Get the consumer stats.
        TopicStats topicStats = admin.topics().getStats(topic);
        SubscriptionStats subscriptionStats = topicStats.getSubscriptions().get(subscribeName);
        long startConsumedFlowTimestamp = subscriptionStats.getLastConsumedFlowTimestamp();
        long startAckedTimestampInSubStats = subscriptionStats.getLastAckedTimestamp();
        ConsumerStats consumerStats = subscriptionStats.getConsumers().get(0);
        long startConsumedTimestampInConsumerStats = consumerStats.getLastConsumedTimestamp();
        long startAckedTimestampInConsumerStats = consumerStats.getLastAckedTimestamp();

        // Because the message was pushed by the broker, the consumedTimestamp should not as 0.
        assertNotEquals(0, startConsumedTimestampInConsumerStats);
        // There is no consumer ack the message, so the lastAckedTimestamp still as 0.
        assertEquals(0, startAckedTimestampInConsumerStats);
        assertNotEquals(0, startConsumedFlowTimestamp);
        assertEquals(0, startAckedTimestampInSubStats);

        // c. The Consumer receives the message and acks the message.
        consumer.acknowledge(message);
        // Waiting for the ack command send to the broker.
        while (true) {
            topicStats = admin.topics().getStats(topic);
            if (topicStats.getSubscriptions().get(subscribeName).getLastAckedTimestamp() != 0) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Get the consumer stats.
        topicStats = admin.topics().getStats(topic);
        subscriptionStats = topicStats.getSubscriptions().get(subscribeName);
        long consumedFlowTimestamp = subscriptionStats.getLastConsumedFlowTimestamp();
        long ackedTimestampInSubStats = subscriptionStats.getLastAckedTimestamp();
        consumerStats = subscriptionStats.getConsumers().get(0);
        long consumedTimestamp = consumerStats.getLastConsumedTimestamp();
        long ackedTimestamp = consumerStats.getLastAckedTimestamp();

        // The lastConsumedTimestamp should same as the last time because the broker does not push any messages and the
        // consumer does not pull any messages.
        assertEquals(startConsumedTimestampInConsumerStats, consumedTimestamp);
        assertTrue(startAckedTimestampInConsumerStats < ackedTimestamp);
        assertNotEquals(0, consumedFlowTimestamp);
        assertTrue(startAckedTimestampInSubStats < ackedTimestampInSubStats);

        // d. Send another messages. The lastConsumedTimestamp should be updated.
        producer.send("message-2".getBytes(StandardCharsets.UTF_8));

        // e. Receive the message and ack it.
        message = consumer.receive();
        consumer.acknowledge(message);
        // Waiting for the ack command send to the broker.
        while (true) {
            topicStats = admin.topics().getStats(topic);
            if (topicStats.getSubscriptions().get(subscribeName).getLastAckedTimestamp() != ackedTimestampInSubStats) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Get the consumer stats again.
        topicStats = admin.topics().getStats(topic);
        subscriptionStats = topicStats.getSubscriptions().get(subscribeName);
        long lastConsumedFlowTimestamp = subscriptionStats.getLastConsumedFlowTimestamp();
        long lastConsumedTimestampInSubStats = subscriptionStats.getLastConsumedTimestamp();
        long lastAckedTimestampInSubStats = subscriptionStats.getLastAckedTimestamp();
        consumerStats = subscriptionStats.getConsumers().get(0);
        long lastConsumedTimestamp = consumerStats.getLastConsumedTimestamp();
        long lastAckedTimestamp = consumerStats.getLastAckedTimestamp();

        assertTrue(consumedTimestamp < lastConsumedTimestamp);
        assertTrue(ackedTimestamp < lastAckedTimestamp);
        assertTrue(startConsumedTimestampInConsumerStats < lastConsumedTimestamp);
        assertEquals(lastConsumedFlowTimestamp, consumedFlowTimestamp);
        assertTrue(ackedTimestampInSubStats < lastAckedTimestampInSubStats);
        assertEquals(lastConsumedTimestamp, lastConsumedTimestampInSubStats);

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testPreciseBacklog() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log";
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.send("message-1".getBytes(StandardCharsets.UTF_8));
        Message<byte[]> message = consumer.receive();
        assertNotNull(message);

        // Mock the entries added count. Default is disable the precise backlog, so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        PersistentSubscription subscription = (PersistentSubscription)pulsar.getBrokerService().getTopicReference(topic).get().getSubscription(subName);
        assertNotNull(subscription);
        ((ManagedLedgerImpl)subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);

        topicStats = admin.topics().getStats(topic, true, true);
        assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 43);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 1);
        consumer.acknowledge(message);

        // wait for ack send
        Awaitility.await().untilAsserted(() -> {
            // Consumer acks the message, so the precise backlog is 0
            TopicStats topicStats2 = admin.topics().getStats(topic, true, true);
            assertEquals(topicStats2.getSubscriptions().get(subName).getBacklogSize(), 0);
            assertEquals(topicStats2.getSubscriptions().get(subName).getMsgBacklog(), 0);
        });

        topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 9);
    }

    @Test(timeOut = 30000)
    public void testBacklogNoDelayed() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-no-delayed-" + UUID.randomUUID().toString();
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .subscriptionType(SubscriptionType.Shared)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < 10; i++) {
            if (i > 4) {
                producer.newMessage()
                    .value("message-1".getBytes(StandardCharsets.UTF_8))
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            } else {
                producer.send("message-1".getBytes(StandardCharsets.UTF_8));
            }
        }

        // Wait for messages to be tracked for delayed delivery. This happens
        // on the consumer dispatch side, so when the send() is complete we're
        // not yet guaranteed to see the stats updated.
        Awaitility.await().untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getStats(topic, true, true);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 5);
        });


        for (int i = 0; i < 5; i++) {
            consumer.acknowledge(consumer.receive());
        }

        // Wait the ack send.
        Awaitility.await().untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getStats(topic, true, true);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 5);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 0);
        });

    }

    @Test
    public void testPreciseBacklogForPartitionedTopic() throws PulsarClientException, PulsarAdminException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-for-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 2);
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        producer.send("message-1".getBytes(StandardCharsets.UTF_8));
        Message<byte[]> message = consumer.receive();
        assertNotNull(message);

        // Mock the entries added count. Default is disable the precise backlog, so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        for (int i = 0; i < 2; i++) {
            PersistentSubscription subscription = (PersistentSubscription)pulsar.getBrokerService().getTopicReference(topic + "-partition-" + i).get().getSubscription(subName);
            assertNotNull(subscription);
            ((ManagedLedgerImpl)subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        }

        TopicStats topicStats = admin.topics().getPartitionedStats(topic, false);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 20);

        topicStats = admin.topics().getPartitionedStats(topic, false, true, true);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 1);
        assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 43);
    }

    @Test(timeOut = 30000)
    public void testBacklogNoDelayedForPartitionedTopic() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://prop-xyz/ns1/precise-back-log-no-delayed-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 2);
        final String subName = "sub-name";

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subName)
            .subscriptionType(SubscriptionType.Shared)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS)
            .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < 10; i++) {
            if (i > 4) {
                producer.newMessage()
                    .value("message-1".getBytes(StandardCharsets.UTF_8))
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            } else {
                producer.send("message-1".getBytes(StandardCharsets.UTF_8));
            }
        }

        TopicStats topicStats = admin.topics().getPartitionedStats(topic, false, true, true);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 470);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 5);

        for (int i = 0; i < 5; i++) {
            consumer.acknowledge(consumer.receive());
        }
        // Wait the ack send.
        Awaitility.await().untilAsserted(() -> {
            TopicStats topicStats2 = admin.topics().getPartitionedStats(topic, false, true, true);
            assertEquals(topicStats2.getSubscriptions().get(subName).getMsgBacklog(), 5);
            assertEquals(topicStats2.getSubscriptions().get(subName).getBacklogSize(), 238);
            assertEquals(topicStats2.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 0);
        });

    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicSuccess() {
        final String topic = "persistent://prop-xyz/ns1/max-num-partitions-per-partitioned-topic-success";
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(3);

        try {
            admin.topics().createPartitionedTopic(topic, 2);
        } catch (Exception e) {
            fail("should not throw any exceptions");
        }

        // reset configuration
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(0);
    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicFailure() {
        final String topic = "persistent://prop-xyz/ns1/max-num-partitions-per-partitioned-topic-failure";
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(2);

        try {
            admin.topics().createPartitionedTopic(topic, 3);
            fail("should throw exception when number of partitions exceed than max partitions");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarAdminException);
        }

        // reset configuration
        pulsar.getConfiguration().setMaxNumPartitionsPerPartitionedTopic(0);
    }

    @Test
    public void testListOfNamespaceBundles() throws Exception {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz2", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz2/ns1", 10);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz2/ns1", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("prop-xyz2/test/ns2", 10);
        assertEquals(admin.namespaces().getBundles("prop-xyz2/ns1").getNumBundles(), 10);
        assertEquals(admin.namespaces().getBundles("prop-xyz2/test/ns2").getNumBundles(), 10);
    }

    @Test
    public void testForceDeleteNamespace() throws Exception {
        conf.setForceDeleteNamespaceAllowed(true);
        final String namespaceName = "prop-xyz2/ns1";
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz2", tenantInfo);
        admin.namespaces().createNamespace(namespaceName, 1);
        final String topic = "persistent://" + namespaceName + "/test" + UUID.randomUUID();
        pulsarClient.newProducer(Schema.DOUBLE).topic(topic).create().close();
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.schemas().getSchemaInfo(topic)));
        admin.namespaces().deleteNamespace(namespaceName, true);
        try {
            admin.schemas().getSchemaInfo(topic);
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 404);
        }
    }

    @Test
    public void testDistinguishTopicTypeWhenForceDeleteNamespace() throws Exception {
        conf.setForceDeleteNamespaceAllowed(true);
        final String ns = "prop-xyz/distinguish-topic-type-ns";
        final String exNs = "prop-xyz/ex-distinguish-topic-type-ns";
        admin.namespaces().createNamespace(ns, 2);
        admin.namespaces().createNamespace(exNs, 2);

        final String p1 = "persistent://" + ns + "/p1";
        final String p5 = "persistent://" + ns + "/p5";
        final String np = "persistent://" + ns + "/np";

        admin.topics().createPartitionedTopic(p1, 1);
        admin.topics().createPartitionedTopic(p5, 5);
        admin.topics().createNonPartitionedTopic(np);

        final String exNp = "persistent://" + exNs + "/np";
        admin.topics().createNonPartitionedTopic(exNp);
        // insert an invalid topic name
        pulsar.getLocalMetadataStore().put(
                "/managed-ledgers/" + exNs + "/persistent/", "".getBytes(), Optional.empty()).join();

        List<String> topics = pulsar.getNamespaceService().getFullListOfTopics(NamespaceName.get(ns)).get();
        List<String> exTopics = pulsar.getNamespaceService().getFullListOfTopics(NamespaceName.get(exNs)).get();

        // ensure that the topic list contains all the topics
        List<String> allTopics = new ArrayList<>(Arrays.asList(np, TopicName.get(p1).getPartition(0).toString()));
        for (int i = 0; i < 5; i++) {
            allTopics.add(TopicName.get(p5).getPartition(i).toString());
        }
        Assert.assertEquals(allTopics.stream().filter(t -> !topics.contains(t)).count(), 0);
        Assert.assertTrue(exTopics.contains("persistent://" + exNs + "/"));
        // partition num = p1 + p5 + np
        Assert.assertEquals(topics.size(), 1 + 5 + 1);
        Assert.assertEquals(exTopics.size(), 1 + 1);

        admin.namespaces().deleteNamespace(ns, true);
        Arrays.asList(p1, p5, np).forEach(t -> {
            try {
                admin.schemas().getSchemaInfo(t);
            } catch (PulsarAdminException e) {
                // all the normal topics' schemas have been deleted
                Assert.assertEquals(e.getStatusCode(), 404);
            }
        });

        try {
            admin.namespaces().deleteNamespace(exNs, true);
            fail("Should fail due to invalid topic");
        } catch (Exception e) {
            //ok
        }
    }

    @Test
    public void testUpdateClusterWithProxyUrl() throws Exception {
        ClusterData cluster = ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        String clusterName = "test2";
        admin.clusters().createCluster(clusterName, cluster);
        Assert.assertEquals(admin.clusters().getCluster(clusterName), cluster);

        // update
        cluster = ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .proxyServiceUrl("proxy")
                .proxyProtocol(ProxyProtocol.SNI)
                .build();
        admin.clusters().updateCluster(clusterName, cluster);
        Assert.assertEquals(admin.clusters().getCluster(clusterName), cluster);
    }

    @Test
    public void testMaxNamespacesPerTenant() throws Exception {
        super.internalCleanup();
        conf.setMaxNamespacesPerTenant(2);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));
        admin.namespaces().createNamespace("testTenant/ns2", Sets.newHashSet("test"));
        try {
            admin.namespaces().createNamespace("testTenant/ns3", Sets.newHashSet("test"));
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
            Assert.assertEquals(e.getHttpError(), "Exceed the maximum number of namespace in tenant :testTenant");
        }

        //unlimited
        super.internalCleanup();
        conf.setMaxNamespacesPerTenant(0);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        for (int i = 0; i < 10; i++) {
            admin.namespaces().createNamespace("testTenant/ns-" + i, Sets.newHashSet("test"));
        }
    }

    @Test
    public void testMaxTopicsPerNamespace() throws Exception {
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(10);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        // check create partitioned/non-partitioned topics
        String topic = "persistent://testTenant/ns1/test_create_topic_v";
        admin.topics().createPartitionedTopic(topic + "1", 2);
        admin.topics().createPartitionedTopic(topic + "2", 3);
        admin.topics().createPartitionedTopic(topic + "3", 4);
        admin.topics().createNonPartitionedTopic(topic + "4");
        try {
            admin.topics().createPartitionedTopic(topic + "5", 2);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
            Assert.assertEquals(e.getHttpError(), "Exceed maximum number of topics in namespace.");
        }

        //unlimited
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(0);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));
        for (int i = 0; i < 10; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 2);
            admin.topics().createNonPartitionedTopic(topic + i + i);
        }

        // check first create normal topic, then system topics, unlimited even setMaxTopicsPerNamespace
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(5);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));
        for (int i = 0; i < 5; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 1);
        }
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__change_events", 2);
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__transaction_buffer_snapshot", 2);
        admin.topics().createPartitionedTopic(
                "persistent://testTenant/ns1/__transaction_buffer_snapshot-multiTopicsReader"
                        + "-05c0ded5e9__transaction_pending_ack", 2);


        // check first create system topics, then normal topic, unlimited even setMaxTopicsPerNamespace
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(5);
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__change_events", 2);
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__transaction_buffer_snapshot", 2);
        admin.topics().createPartitionedTopic(
                "persistent://testTenant/ns1/__transaction_buffer_snapshot-multiTopicsReader"
                        + "-05c0ded5e9__transaction_pending_ack", 2);
        for (int i = 0; i < 5; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 1);
        }

        // check producer/consumer auto create partitioned topic
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(10);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreationType("partitioned");
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();
        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub").subscribe().close();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }

        // check producer/consumer auto create non-partitioned topic
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(3);
        conf.setAllowAutoTopicCreationType("non-partitioned");
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();
        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub").subscribe().close();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }

        // reset configuration
        conf.setMaxTopicsPerNamespace(0);
        conf.setDefaultNumPartitions(1);
    }

    @Test
    public void testInvalidBundleErrorResponse() throws Exception {
        try {
            admin.namespaces().deleteNamespaceBundle("prop-xyz/ns1", "invalid-bundle");
            fail("should have failed due to invalid bundle");
        } catch (PreconditionFailedException e) {
            assertTrue(e.getMessage().startsWith("Invalid bundle range"));
        }
    }

    @Test
    public void testMaxSubscriptionsPerTopic() throws Exception {
        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        final String topic = "persistent://testTenant/ns1/max-subscriptions-per-topic";

        admin.topics().createPartitionedTopic(topic, 3);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        // create subscription
        admin.topics().createSubscription(topic, "test-sub1", MessageId.earliest);
        admin.topics().createSubscription(topic, "test-sub2", MessageId.earliest);
        try {
            admin.topics().createSubscription(topic, "test-sub3", MessageId.earliest);
            Assert.fail();
        } catch (PulsarAdminException e) {
            log.info("create subscription failed. Exception: ", e);
        }

        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(0);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        admin.topics().createPartitionedTopic(topic, 3);
        producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        for (int i = 0; i < 10; ++i) {
            admin.topics().createSubscription(topic, "test-sub" + i, MessageId.earliest);
        }

        super.internalCleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        admin.topics().createPartitionedTopic(topic, 3);
        producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        Consumer consumer1 = null;
        Consumer consumer2 = null;
        Consumer consumer3 = null;

        try {
            consumer1 = pulsarClient.newConsumer().subscriptionName("test-sub1").topic(topic).subscribe();
            Assert.assertNotNull(consumer1);
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer2 = pulsarClient.newConsumer().subscriptionName("test-sub2").topic(topic).subscribe();
            Assert.assertNotNull(consumer2);
        } catch (PulsarClientException e) {
            Assert.fail();
        }

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("test-sub3").topic(topic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscription reached max subscriptions per topic");
        }

        consumer1.close();
        consumer2.close();
        admin.topics().deletePartitionedTopic(topic);
    }

    @Test(timeOut = 30000)
    public void testMaxSubPerTopicApi() throws Exception {
        final String myNamespace = "prop-xyz/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));

        assertNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace));

        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace,100);
        assertEquals(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace).intValue(),100);
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        assertNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace));

        admin.namespaces().setMaxSubscriptionsPerTopicAsync(myNamespace,200).get();
        assertEquals(admin.namespaces().getMaxSubscriptionsPerTopicAsync(myNamespace).get().intValue(),200);
        admin.namespaces().removeMaxSubscriptionsPerTopicAsync(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxSubscriptionsPerTopicAsync(myNamespace).get()));

        try {
            admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace,-100);
            fail("should fail");
        } catch (PulsarAdminException ignore) {
        }
    }

    @Test(timeOut = 30000)
    public void testMaxSubPerTopic() throws Exception {
        final String myNamespace = "prop-xyz/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxSubPerTopic";
        pulsarClient.newProducer().topic(topic).create().close();
        final int maxSub = 2;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, maxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Field field = PersistentTopic.class.getSuperclass().getDeclaredField("maxSubscriptionsPerTopic");
        field.setAccessible(true);
        Awaitility.await().until(() -> (int) field.get(persistentTopic) == maxSub);

        List<Consumer<?>> consumerList = new ArrayList<>(maxSub);
        for (int i = 0; i < maxSub; i++) {
            Consumer<?> consumer =
                    pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            consumerList.add(consumer);
        }
        //Create a client that can fail quickly
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2,TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()){
            client.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            fail("should fail");
        } catch (Exception ignore) {
        }
        //After removing the restriction, it should be able to create normally
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() -> field.get(persistentTopic) == null);
        Consumer<?> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString())
                .subscribe();
        consumerList.add(consumer);

        for (Consumer<?> c : consumerList) {
            c.close();
        }
    }

    @Test(timeOut = 30000)
    public void testMaxSubPerTopicPriority() throws Exception {
        final int brokerLevelMaxSub = 2;
        super.internalCleanup();
        mockPulsarSetup.cleanup();
        conf.setMaxSubscriptionsPerTopic(brokerLevelMaxSub);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        final String myNamespace = "prop-xyz/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxSubPerTopic";
        //Create a client that can fail quickly
        @Cleanup
        PulsarClient client = PulsarClient.builder().operationTimeout(2,TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build();
        //We can only create 2 consumers
        List<Consumer<?>> consumerList = new ArrayList<>(brokerLevelMaxSub);
        for (int i = 0; i < brokerLevelMaxSub; i++) {
            Consumer<?> consumer =
                    pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            consumerList.add(consumer);
        }
        try {
            client.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            fail("should fail");
        } catch (Exception ignore) {

        }
        //Set namespace-level policy,the limit should up to 4
        final int nsLevelMaxSub = 4;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, nsLevelMaxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Field field = PersistentTopic.class.getSuperclass().getDeclaredField("maxSubscriptionsPerTopic");
        field.setAccessible(true);
        Awaitility.await().until(() -> (int) field.get(persistentTopic) == nsLevelMaxSub);
        Consumer<?> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString())
                .subscribe();
        consumerList.add(consumer);
        assertEquals(consumerList.size(), 3);
        //After removing the restriction, it should fail again
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() -> field.get(persistentTopic) == null);
        try {
            client.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            fail("should fail");
        } catch (Exception ignore) {

        }

        for (Consumer<?> c : consumerList) {
            c.close();
        }
    }

    @Test
    public void testMaxProducersPerTopicUnlimited() throws Exception {
        final int maxProducersPerTopic = 1;
        super.internalCleanup();
        mockPulsarSetup.cleanup();
        conf.setMaxProducersPerTopic(maxProducersPerTopic);
        super.internalSetup();
        //init namespace
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        final String myNamespace = "prop-xyz/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxProducersPerTopicUnlimited";
        //the policy is set to 0, so there will be no restrictions
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 0);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == 0);
        List<Producer<byte[]>> producers = new ArrayList<>();
        for (int i = 0; i < maxProducersPerTopic + 1; i++) {
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
            producers.add(producer);
        }

        admin.namespaces().removeMaxProducersPerTopic(myNamespace);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == null);
        try {
            pulsarClient.newProducer().topic(topic).create();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max producers limit"));
        }
        //set the limit to 3
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == 3);
        // should success
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producers.add(producer);
        try {
            pulsarClient.newProducer().topic(topic).create();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max producers limit"));
        }

        //clean up
        for (Producer<byte[]> tempProducer : producers) {
            tempProducer.close();
        }
    }

    @Test
    public void testMaxConsumersPerTopicUnlimited() throws Exception {
        final int maxConsumersPerTopic = 1;
        super.internalCleanup();
        mockPulsarSetup.cleanup();
        conf.setMaxConsumersPerTopic(maxConsumersPerTopic);
        super.internalSetup();
        //init namespace
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        final String myNamespace = "prop-xyz/ns" + UUID.randomUUID();
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxConsumersPerTopicUnlimited";

        assertNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace));
        //the policy is set to 0, so there will be no restrictions
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 0);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 0);
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        for (int i = 0; i < maxConsumersPerTopic + 1; i++) {
            Consumer<byte[]> consumer =
                    pulsarClient.newConsumer().subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            consumers.add(consumer);
        }

        admin.namespaces().removeMaxConsumersPerTopic(myNamespace);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == null);
        try {
            pulsarClient.newConsumer().subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max consumers limit"));
        }
        //set the limit to 3
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 3);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 3);
        // should success
        Consumer<byte[]> consumer =
                pulsarClient.newConsumer().subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
        consumers.add(consumer);
        try {
            pulsarClient.newConsumer().subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max consumers limit"));
        }

        //clean up
        for (Consumer<byte[]> subConsumer : consumers) {
            subConsumer.close();
        }
    }

    @Test
    public void testClearBacklogForTheSubscriptionThatNoConsumers() throws Exception {
        final String topic = "persistent://prop-xyz/ns1/clear_backlog_no_consumers" + UUID.randomUUID().toString();
        final String sub = "my-sub";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, sub, MessageId.earliest);
        admin.topics().skipAllMessages(topic, sub);
    }

    @Test(timeOut = 200000)
    public void testCompactionApi() throws Exception {
        final String namespace = "prop-xyz/ns1";
        assertNull(admin.namespaces().getCompactionThreshold(namespace));
        assertEquals(pulsar.getConfiguration().getBrokerServiceCompactionThresholdInBytes(), 0);

        admin.namespaces().setCompactionThreshold(namespace, 10);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.namespaces().getCompactionThreshold(namespace)));
        assertEquals(admin.namespaces().getCompactionThreshold(namespace).intValue(), 10);

        admin.namespaces().removeCompactionThreshold(namespace);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getCompactionThreshold(namespace)));
    }

    @Test(timeOut = 200000)
    public void testCompactionPriority() throws Exception {
        cleanup();
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(10000);
        setup();
        final String topic = "persistent://prop-xyz/ns1/topic" + UUID.randomUUID();
        final String namespace = "prop-xyz/ns1";
        pulsarClient.newProducer().topic(topic).create().close();
        TopicName topicName = TopicName.get(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        PersistentTopic mockTopic = spy(persistentTopic);
        mockTopic.checkCompaction();
        // Disabled by default
        verify(mockTopic, times(0)).triggerCompaction();
        // Set namespace-level policy
        admin.namespaces().setCompactionThreshold(namespace, 1);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.namespaces().getCompactionThreshold(namespace)));
        ManagedLedger managedLedger = persistentTopic.getManagedLedger();
        Field field = managedLedger.getClass().getDeclaredField("totalSize");
        field.setAccessible(true);
        field.setLong(managedLedger, 1000L);

        mockTopic.checkCompaction();
        verify(mockTopic, times(1)).triggerCompaction();
        //Set topic-level policy
        admin.topics().setCompactionThreshold(topic, 0);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getCompactionThreshold(topic)));
        mockTopic.checkCompaction();
        verify(mockTopic, times(1)).triggerCompaction();
        // Remove topic-level policy
        admin.topics().removeCompactionThreshold(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getCompactionThreshold(topic)));
        mockTopic.checkCompaction();
        verify(mockTopic, times(2)).triggerCompaction();
        // Remove namespace-level policy
        admin.namespaces().removeCompactionThreshold(namespace);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.namespaces().getCompactionThreshold(namespace)));
        mockTopic.checkCompaction();
        verify(mockTopic, times(2)).triggerCompaction();
    }

    @Test
    public void testProperties() throws Exception {
        final String namespace = "prop-xyz/ns1";
        admin.namespaces().setProperty(namespace, "a", "a");
        assertEquals("a", admin.namespaces().getProperty(namespace, "a"));
        assertNull(admin.namespaces().getProperty(namespace, "b"));
        admin.namespaces().setProperty(namespace, "b", "b");
        assertEquals("b", admin.namespaces().getProperty(namespace, "b"));
        admin.namespaces().setProperty(namespace, "a", "a1");
        assertEquals("a1", admin.namespaces().getProperty(namespace, "a"));
        assertEquals("b", admin.namespaces().removeProperty(namespace, "b"));
        assertNull(admin.namespaces().getProperty(namespace, "b"));
        admin.namespaces().clearProperties(namespace);
        assertEquals(admin.namespaces().getProperties(namespace).size(), 0);
        Map<String, String> properties = new HashMap<>();
        properties.put("aaa", "aaa");
        properties.put("bbb", "bbb");
        admin.namespaces().setProperties(namespace, properties);
        assertEquals(admin.namespaces().getProperties(namespace), properties);
        admin.namespaces().clearProperties(namespace);
        assertEquals(admin.namespaces().getProperties(namespace).size(), 0);
    }

    @Test
    public void testGetListInBundle() throws Exception {
        final String namespace = "prop-xyz/ns11";
        admin.namespaces().createNamespace(namespace, 3);

        final String persistentTopicName = TopicName.get(
                "persistent", NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID()).toString();

        final String nonPersistentTopicName = TopicName.get(
                "non-persistent", NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID()).toString();
        admin.topics().createPartitionedTopic(persistentTopicName, 3);
        admin.topics().createPartitionedTopic(nonPersistentTopicName, 3);
        pulsarClient.newProducer().topic(persistentTopicName).create().close();
        pulsarClient.newProducer().topic(nonPersistentTopicName).create().close();

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        List<String> boundaries = bundlesData.getBoundaries();
        int topicNum = 0;
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            List<String> topic = admin.topics().getListInBundle(namespace, bundle);
            if (topic == null) {
                continue;
            }
            topicNum += topic.size();
            for (String s : topic) {
                assertFalse(TopicName.get(s).isPersistent());
            }
        }
        assertEquals(topicNum, 3);
    }

    @Test
    public void testGetTopicsWithDifferentMode() throws Exception {
        final String namespace = "prop-xyz/ns1";

        final String persistentTopicName = TopicName
                .get("persistent", NamespaceName.get(namespace), "get_topics_mode_" + UUID.randomUUID().toString())
                .toString();

        final String nonPersistentTopicName = TopicName
                .get("non-persistent", NamespaceName.get(namespace), "get_topics_mode_" + UUID.randomUUID().toString())
                .toString();

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(persistentTopicName).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(nonPersistentTopicName).create();

        List<String> topics = new ArrayList<>(admin.topics().getList(namespace));
        assertEquals(topics.size(), 2);
        assertTrue(topics.contains(persistentTopicName));
        assertTrue(topics.contains(nonPersistentTopicName));

        topics.clear();

        topics.addAll(admin.topics().getList(namespace, TopicDomain.persistent));
        assertEquals(topics.size(), 1);
        assertTrue(topics.contains(persistentTopicName));

        topics.clear();

        topics.addAll(admin.topics().getList(namespace, TopicDomain.non_persistent));
        assertEquals(topics.size(), 1);
        assertTrue(topics.contains(nonPersistentTopicName));

        try {
            admin.topics().getList(namespace, TopicDomain.getEnum("none"));
            fail("Should failed with invalid get topic mode.");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid topic domain: 'none'");
        }

        producer1.close();
        producer2.close();
    }


    @Test(dataProvider = "isV1")
    public void testNonPartitionedTopic(boolean isV1) throws Exception {
        String tenant = "prop-xyz";
        String cluster = "test";
        String namespace = tenant + "/" + (isV1 ? cluster + "/" : "") + "n1" + isV1;
        String topic = "persistent://" + namespace + "/t1" + isV1;
        admin.namespaces().createNamespace(namespace, Sets.newHashSet(cluster));
        admin.topics().createNonPartitionedTopic(topic);
        assertTrue(admin.topics().getList(namespace).contains(topic));
    }

    /**
     * Validate retring failed partitioned topic should succeed.
     * @throws Exception
     */
    @Test
    public void testFailedUpdatePartitionedTopic() throws Exception {
        final String topicName = "failed-topic";
        final String subName1 = topicName + "-my-sub-1";
        final int startPartitions = 4;
        final int newPartitions = 8;
        final String partitionedTopicName = "persistent://prop-xyz/ns1/" + topicName;

        URL pulsarUrl = new URL(pulsar.getWebServiceAddress());

        admin.topics().createPartitionedTopic(partitionedTopicName, startPartitions);
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl.toString()).build();
        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        consumer1.close();

        // validate partition topic is created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, startPartitions);

        // create a subscription for few new partition which can fail
        admin.topics().createSubscription(partitionedTopicName + "-partition-" + startPartitions, subName1,
                MessageId.earliest);

        try {
            admin.topics().updatePartitionedTopic(partitionedTopicName, newPartitions, false, false);
        } catch (PulsarAdminException.PreconditionFailedException e) {
            // Ok
        }
        admin.topics().updatePartitionedTopic(partitionedTopicName, newPartitions, false, true);
        // validate subscription is created for new partition.
        assertNotNull(admin.topics().getStats(partitionedTopicName + "-partition-" + 6).getSubscriptions().get(subName1));
    }
}
