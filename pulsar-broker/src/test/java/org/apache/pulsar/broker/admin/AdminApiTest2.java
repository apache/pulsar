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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminApiTest.MockedPulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.gson.JsonObject;

public class AdminApiTest2 extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiTest2.class);

    private MockedPulsarService mockPulsarSetup;


    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        super.internalSetup();

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();

        // Setup namespaces
        admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        PropertyAdmin propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"), Sets.newHashSet("use"));
        admin.properties().createProperty("prop-xyz", propertyAdmin);
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        mockPulsarSetup.cleanup();
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { DestinationDomain.persistent.value() },
                { DestinationDomain.non_persistent.value() } };
    }

    @DataProvider(name = "namespaceNames")
    public Object[][] namespaceNameProvider() {
        return new Object[][] { { "ns1" }, { "global" } };
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
     * @param topicName
     * @throws Exception
     */
    @Test
    public void testIncrementPartitionsOfTopic() throws Exception {
        final String topicName = "increment-partitionedTopic";
        final String subName1 = topicName + "-my-sub 1";
        final String subName2 = topicName + "-my-sub 2";
        final int startPartitions = 4;
        final int newPartitions = 8;
        final String partitionedTopicName = "persistent://prop-xyz/use/ns1/" + topicName;

        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);

        admin.persistentTopics().createPartitionedTopic(partitionedTopicName, startPartitions);
        // validate partition topic is created
        assertEquals(admin.persistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                startPartitions);

        // create consumer and subscriptions : check subscriptions
        PulsarClient client = PulsarClient.create(pulsarUrl.toString());
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = client.subscribe(partitionedTopicName, subName1, conf);
        assertEquals(admin.persistentTopics().getSubscriptions(partitionedTopicName), Lists.newArrayList(subName1));
        Consumer consumer2 = client.subscribe(partitionedTopicName, subName2, conf);
        assertEquals(Sets.newHashSet(admin.persistentTopics().getSubscriptions(partitionedTopicName)),
                Sets.newHashSet(subName1, subName2));

        // (1) update partitions
        admin.persistentTopics().updatePartitionedTopic(partitionedTopicName, newPartitions);
        // invalidate global-cache to make sure that mock-zk-cache reds fresh data
        pulsar.getGlobalZkCache().invalidateAll();
        // verify new partitions have been created
        assertEquals(admin.persistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                newPartitions);
        // (2) No Msg loss: verify new partitions have the same existing subscription names
        final String newPartitionTopicName = DestinationName.get(partitionedTopicName).getPartition(startPartitions + 1)
                .toString();

        // (3) produce messages to all partitions including newly created partitions (RoundRobin)
        ProducerConfiguration prodConf = new ProducerConfiguration();
        prodConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = client.createProducer(partitionedTopicName, prodConf);
        final int totalMessages = newPartitions * 2;
        for (int i = 0; i < totalMessages; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        // (4) verify existing subscription has not lost any message: create new consumer with sub-2: it will load all
        // newly created partition topics
        consumer2.close();
        consumer2 = client.subscribe(partitionedTopicName, subName2, conf);
        // sometime: mockZk fails to refresh ml-cache: so, invalidate the cache to get fresh data
        pulsar.getLocalZkCacheService().managedLedgerListCache().clearTree();
        assertEquals(Sets.newHashSet(admin.persistentTopics().getSubscriptions(newPartitionTopicName)),
                Sets.newHashSet(subName1, subName2));

        assertEquals(Sets.newHashSet(admin.persistentTopics().getList("prop-xyz/use/ns1")).size(), newPartitions);

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.persistentTopics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet(Lists.newArrayList(subName1, subName2)));
        assertEquals(topicStats.subscriptions.get(subName2).consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get(subName2).msgBacklog, totalMessages);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.partitions, Maps.newHashMap());

        // (5) verify: each partition should have backlog
        topicStats = admin.persistentTopics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.metadata.partitions, newPartitions);
        Set<String> partitionSet = Sets.newHashSet();
        for (int i = 0; i < newPartitions; i++) {
            partitionSet.add(partitionedTopicName + "-partition-" + i);
        }
        assertEquals(topicStats.partitions.keySet(), partitionSet);
        for (int i = 0; i < newPartitions; i++) {
            PersistentTopicStats partitionStats = topicStats.partitions
                    .get(DestinationName.get(partitionedTopicName).getPartition(i).toString());
            assertEquals(partitionStats.publishers.size(), 1);
            assertEquals(partitionStats.subscriptions.get(subName2).consumers.size(), 1);
            assertEquals(partitionStats.subscriptions.get(subName2).msgBacklog, 2, 1);
        }

        producer.close();
        consumer1.close();
        consumer2.close();
        consumer2.close();
    }

    /**
     * verifies admin api command for non-persistent topic.
     * It verifies: partitioned-topic, stats
     * @throws Exception
     */
    @Test
    public void nonPersistentTopics() throws Exception {
        final String topicName = "nonPersistentTopic";

        final String persistentTopicName = "non-persistent://prop-xyz/use/ns1/" + topicName;
        // Force to create a destination
        publishMessagesOnTopic("non-persistent://prop-xyz/use/ns1/" + topicName, 0, 0);

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe(persistentTopicName, "my-sub", conf);

        publishMessagesOnTopic("non-persistent://prop-xyz/use/ns1/" + topicName, 10, 0);

        NonPersistentTopicStats topicStats = admin.nonPersistentTopics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
        assertEquals(topicStats.getSubscriptions().get("my-sub").consumers.size(), 1);
        assertEquals(topicStats.getPublishers().size(), 0);

        PersistentTopicInternalStats internalStats = admin.nonPersistentTopics().getInternalStats(persistentTopicName);
        assertEquals(internalStats.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));

        consumer.close();
        client.close();

        topicStats = admin.nonPersistentTopics().getStats(persistentTopicName);
        assertTrue(topicStats.getSubscriptions().keySet().contains("my-sub"));
        assertEquals(topicStats.getPublishers().size(), 0);

        // test partitioned-topic
        final String partitionedTopicName = "non-persistent://prop-xyz/use/ns1/paritioned";
        assertEquals(admin.nonPersistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 0);
        admin.nonPersistentTopics().createPartitionedTopic(partitionedTopicName, 5);
        assertEquals(admin.nonPersistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 5);
    }

    private void publishMessagesOnTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer producer = pulsarClient.createProducer(topicName);

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
    public void testSetPersistencepolicies() throws Exception {

        final String namespace = "prop-xyz/use/ns2";
        admin.namespaces().createNamespace(namespace);

        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(1, 1, 1, 0.0));
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 10.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 10.0));

        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 4, 3, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 4, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        try {
            admin.namespaces().setPersistence(namespace, new PersistencePolicies(6, 3, 1, 10.0));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
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

        final String namespace = "prop-xyz/use/ns2";
        final String topicName = "persistent://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace);

        admin.namespaces().setPersistence(namespace, new PersistencePolicies(3, 3, 3, 50.0));
        assertEquals(admin.namespaces().getPersistence(namespace), new PersistencePolicies(3, 3, 3, 50.0));

        Producer producer = pulsarClient.createProducer(topicName);
        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub");

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(topicName).get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.getCursors().iterator().next();

        final double newThrottleRate = 100;
        final int newEnsembleSize = 5;
        admin.namespaces().setPersistence(namespace, new PersistencePolicies(newEnsembleSize, 3, 3, newThrottleRate));

        for (int i = 0; i < 5; i++) {
            if ((managedLedger.getConfig().getEnsembleSize() != newEnsembleSize
                    && cursor.getThrottleMarkDelete() != newThrottleRate) || i == 4) {
                Thread.sleep(200);
            }
        }

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

        final String namespace = "prop-xyz/use/ns2";
        final String topicName = topicType + "://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace);

        // create a topic by creating a producer
        Producer producer = pulsarClient.createProducer(topicName);
        producer.close();

        Topic topic = pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topic);
        final boolean isPersistentTopic = topic instanceof PersistentTopic;

        // (1) unload the topic
        unloadTopic(topicName, isPersistentTopic);
        topic = pulsar.getBrokerService().getTopicReference(topicName);
        // topic must be removed
        assertNull(topic);

        // recreation of producer will load the topic again
        producer = pulsarClient.createProducer(topicName);
        topic = pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topic);
        // unload the topic
        unloadTopic(topicName, isPersistentTopic);
        // producer will retry and recreate the topic
        for (int i = 0; i < 5; i++) {
            topic = pulsar.getBrokerService().getTopicReference(topicName);
            if (topic == null || i != 4) {
                Thread.sleep(200);
            }
        }
        // topic should be loaded by this time
        topic = pulsar.getBrokerService().getTopicReference(topicName);
        assertNotNull(topic);
    }

    private void unloadTopic(String topicName, boolean isPersistentTopic) throws Exception {
        if (isPersistentTopic) {
            admin.persistentTopics().unload(topicName);
        } else {
            admin.nonPersistentTopics().unload(topicName);
        }
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
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, totalProducedMessages, 0);

        List<Message> messages = admin.persistentTopics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        Message message = null;
        MessageIdImpl resetMessageId = null;
        int resetPositionId = 10;
        for (int i = 0; i < 20; i++) {
            message = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(message);
            if (i == resetPositionId) {
                resetMessageId = (MessageIdImpl) message.getMessageId();
            }
        }

        // close consumer which will clean up intenral-receive-queue
        consumer.close();

        // messages should still be available due to retention
        MessageIdImpl messageId = new MessageIdImpl(resetMessageId.getLedgerId(), resetMessageId.getEntryId(), -1);
        // reset position at resetMessageId
        admin.persistentTopics().resetCursor(topicName, "my-sub", messageId);

        consumer = pulsarClient.subscribe(topicName, "my-sub", conf);
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
            admin.persistentTopics().resetCursor(topicName + "invalid", "my-sub", messageId);
            fail("It should have failed due to invalid topic name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid cursor name
        try {
            admin.persistentTopics().resetCursor(topicName, "invalid-sub", messageId);
            fail("It should have failed due to invalid subscription name");
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        // invalid position
        try {
            messageId = new MessageIdImpl(0, 0, -1);
            admin.persistentTopics().resetCursor(topicName, "my-sub", messageId);
            fail("It should have failed due to invalid subscription name");
        } catch (PulsarAdminException.PreconditionFailedException e) {
            // Ok
        }

        consumer.close();
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer producer = pulsarClient.createProducer(topicName);

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
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
        PulsarService simpleLoadManager = mockPulsarSetup1.getPulsar();
        PulsarAdmin simpleLoadManagerAdmin = mockPulsarSetup1.getAdmin();
        assertNotNull(simpleLoadManagerAdmin.brokerStats().getLoadReport());

        this.conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        MockedPulsarService mockPulsarSetup2 = new MockedPulsarService(this.conf);
        mockPulsarSetup2.setup();
        PulsarService modularLoadManager = mockPulsarSetup2.getPulsar();
        PulsarAdmin modularLoadManagerAdmin = mockPulsarSetup2.getAdmin();
        assertNotNull(modularLoadManagerAdmin.brokerStats().getLoadReport());

        simpleLoadManagerAdmin.close();
        simpleLoadManager.close();
        modularLoadManagerAdmin.close();
        modularLoadManager.close();
        mockPulsarSetup1.cleanup();
        mockPulsarSetup2.cleanup();
    }
    
}
