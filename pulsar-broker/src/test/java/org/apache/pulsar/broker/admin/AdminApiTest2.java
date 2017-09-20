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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminApiTest.MockedPulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

public class AdminApiTest2 extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiTest2.class);

    private MockedPulsarService mockPulsarSetup;

    private PulsarService otherPulsar;

    private PulsarAdmin otheradmin;

    private NamespaceBundleFactory bundleFactory;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        super.internalSetup();

        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf, this.pulsar, this.admin);
        mockPulsarSetup.setup();
        otherPulsar = mockPulsarSetup.getPulsar();
        otheradmin = mockPulsarSetup.getAdmin();

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

}
