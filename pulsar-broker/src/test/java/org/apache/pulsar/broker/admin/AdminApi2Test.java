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
package org.apache.pulsar.broker.admin;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.apache.pulsar.broker.resources.LoadBalanceResources.BUNDLE_DATA_BASE_PATH;
import static org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope.all_matching;
import static org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope.changed;
import static org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope.none;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.core.Response.Status;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminApiTest.MockedPulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilter2Test;
import org.apache.pulsar.broker.service.plugin.EntryFilterDefinition;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.broker.service.plugin.EntryFilterTest;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.testcontext.MockEntryFilterProvider;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.GrantTopicPermissionOptions;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.Mode;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.RevokeTopicPermissionOptions;
import org.apache.pulsar.client.admin.Topics.QueryParam;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationPolicyUnloadScope;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApi2Test extends MockedPulsarServiceBaseTest {

    private MockedPulsarService mockPulsarSetup;
    private boolean restartClusterAfterTest;
    private int usageCount;
    private String defaultNamespace;
    private String defaultTenant;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();

        setupClusters();
    }

    @Test
    public void testExceptionOfMaxTopicsPerNamespaceCanBeHanle() throws Exception {
        super.internalCleanup();
        conf.setMaxTopicsPerNamespace(3);
        super.internalSetup();
        String topic = "persistent://testTenant/ns1/test_create_topic_v";
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet("test"));
        // check producer/consumer auto create non-partitioned topic
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Sets.newHashSet("test"));

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();
        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub")
                    .subscribeAsync().get(5, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.NotAllowedException);
        }

        // reset configuration
        conf.setMaxTopicsPerNamespace(0);
        conf.setDefaultNumPartitions(1);
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        configureDefaults(conf);
        return conf;
    }

    void configureDefaults(ServiceConfiguration conf) {
        conf.setForceDeleteNamespaceAllowed(true);
        conf.setLoadBalancerEnabled(true);
        conf.setAllowOverrideEntryFilters(true);
        conf.setEntryFilterNames(List.of());
        conf.setMaxNumPartitionsPerPartitionedTopic(0);
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        if (mockPulsarSetup != null) {
            mockPulsarSetup.cleanup();
            mockPulsarSetup = null;
        }
        resetConfig();
    }

    @AfterMethod(alwaysRun = true)
    public void resetClusters() throws Exception {
        if (restartClusterAfterTest) {
            restartClusterAndResetUsageCount();
        } else {
            try {
                cleanupCluster();
            } catch (Exception e) {
                log.error("Failed to clean up state by deleting namespaces and tenants after test. "
                        + "Restarting the test broker.", e);
                restartClusterAndResetUsageCount();
            }
        }
    }

    private void cleanupCluster() throws Exception {
        pulsar.getConfiguration().setForceDeleteTenantAllowed(true);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);
        for (String tenant : admin.tenants().getTenants()) {
            for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                deleteNamespaceWithRetry(namespace, true, admin);
            }
            try {
                admin.tenants().deleteTenant(tenant, true);
            } catch (Exception e) {
                log.error("Failed to delete tenant {} after test", tenant, e);
                String zkDirectory = "/managed-ledgers/" + tenant;
                try {
                    log.info("Listing {} to see if existing keys are preventing deletion.", zkDirectory);
                    pulsar.getPulsarResources().getLocalMetadataStore().get().getChildren(zkDirectory)
                            .get(5, TimeUnit.SECONDS).forEach(key -> log.info("Child key '{}'", key));
                } catch (Exception ignore) {
                    log.error("Failed to list tenant {} ZK directory {} after test", tenant, zkDirectory, e);
                }
                throw e;
            }
        }

        for (String cluster : admin.clusters().getClusters()) {
            admin.clusters().deleteCluster(cluster);
        }

        configureDefaults(conf);
        setupClusters();
    }

    private void restartClusterAfterTest() {
        restartClusterAfterTest = true;
    }

    private void restartClusterAndResetUsageCount() throws Exception {
        cleanup();
        restartClusterAfterTest = false;
        usageCount = 0;
        setup();
    }

    private void restartClusterIfReused() throws Exception {
        if (usageCount > 1) {
            restartClusterAndResetUsageCount();
        }
    }

    @BeforeMethod
    public void increaseUsageCount() {
        usageCount++;
    }

    private void setupClusters() throws PulsarAdminException {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        defaultTenant = newUniqueName("prop-xyz");
        admin.tenants().createTenant(defaultTenant, tenantInfo);
        defaultNamespace = defaultTenant + "/ns1";
        admin.namespaces().createNamespace(defaultNamespace, Set.of("test"));
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
     * It verifies http error code when updating partitions to ensure compatibility.
     */
    @Test
    public void testUpdatePartitionsErrorCode() {
        final String nonPartitionedTopicName = "non-partitioned-topic-name" + UUID.randomUUID();
        try {
            // Update a non-partitioned topic
            admin.topics().updatePartitionedTopic(nonPartitionedTopicName, 2);
            Assert.fail("Expect conflict exception.");
        } catch (PulsarAdminException ex) {
            Assert.assertEquals(ex.getStatusCode(), 409 /*Conflict*/);
            Assert.assertTrue(ex instanceof PulsarAdminException.ConflictException);
        }
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
        final String partitionedTopicName = "persistent://" + defaultNamespace + "/" + topicName;

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
        assertEquals(new HashSet<>(admin.topics().getSubscriptions(partitionedTopicName)),
                Set.of(subName1, subName2));

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
        assertEquals(new HashSet<>(admin.topics().getSubscriptions(newPartitionTopicName)),
                Set.of(subName1, subName2));

        assertEquals(new HashSet<>(admin.topics().getList(defaultNamespace)).size(), newPartitions);

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.getSubscriptions().keySet(), new TreeSet<>(Lists.newArrayList(subName1, subName2)));
        assertEquals(topicStats.getSubscriptions().get(subName2).getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get(subName2).getMsgBacklog(), totalMessages);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.getPartitions(), new HashMap<>());

        // (5) verify: each partition should have backlog
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.getMetadata().partitions, newPartitions);
        Set<String> partitionSet = new HashSet<>();
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
        restartClusterAfterTest();

        //setup cluster with 3 broker
        admin.clusters().updateCluster("test",
                ClusterData.builder().serviceUrl((pulsar.getWebServiceAddress()
                        + ",localhost:1026," + "localhost:2050")).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        String tenantName = newUniqueName("prop-xyz2");
        admin.tenants().createTenant(tenantName, tenantInfo);
        admin.namespaces().createNamespace(tenantName + "/ns1", Set.of("test"));
        ServiceConfiguration config2 = super.getDefaultConf();
        @Cleanup
        PulsarTestContext pulsarTestContext2 = createAdditionalPulsarTestContext(config2);
        PulsarService pulsar2 = pulsarTestContext2.getPulsarService();
        ServiceConfiguration config3 = super.getDefaultConf();
        @Cleanup
        PulsarTestContext pulsarTestContext3 = createAdditionalPulsarTestContext(config3);
        PulsarService pulsar3 = pulsarTestContext.getPulsarService();
        @Cleanup
        PulsarAdmin admin2 = PulsarAdmin.builder().serviceHttpUrl(pulsar2.getWebServiceAddress()).build();
        @Cleanup
        PulsarAdmin admin3 = PulsarAdmin.builder().serviceHttpUrl(pulsar3.getWebServiceAddress()).build();

        //for partitioned topic, we can get topic policies from every broker
        final String topic = "persistent://" + tenantName + "/ns1/" + newUniqueName("test");
        int partitionNum = 3;
        admin.topics().createPartitionedTopic(topic, partitionNum);
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub").subscribe().close();

        setTopicPoliciesAndValidate(admin2, admin3, topic);
        //for non-partitioned topic, we can get topic policies from every broker
        final String topic2 = "persistent://" + tenantName + "/ns1/" + newUniqueName("test");
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

        final String nonPersistentTopicName = "non-persistent://" + defaultNamespace + "/" + topicName;
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
        assertEquals(topicStats.getSubscriptions().keySet(), Set.of("my-sub"));
        assertEquals(topicStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgDropRate(), 0);
        assertEquals(topicStats.getPublishers().size(), 0);
        assertEquals(topicStats.getMsgDropRate(), 0);
        assertEquals(topicStats.getOwnerBroker(), pulsar.getBrokerId());

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(nonPersistentTopicName, false);
        assertEquals(internalStats.cursors.keySet(), Set.of("my-sub"));

        consumer.close();
        topicStats = (NonPersistentTopicStats) admin.topics().getStats(nonPersistentTopicName);
        assertFalse(topicStats.getSubscriptions().containsKey("my-sub"));
        assertEquals(topicStats.getPublishers().size(), 0);
        // test partitioned-topic
        final String partitionedTopicName = "non-persistent://" + defaultNamespace + "/paritioned";
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
     * verifies validation on persistent-policies.
     *
     * @throws Exception
     */
    @Test
    public void testSetPersistencePolicies() throws Exception {

        final String namespace = newUniqueName(defaultTenant + "/ns2");
        admin.namespaces().createNamespace(namespace, Set.of("test"));

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
     * validates update of persistent-policies reflects on managed-ledger and managed-cursor.
     *
     * @throws Exception
     */
    @Test
    public void testUpdatePersistencePolicyUpdateManagedCursor() throws Exception {

        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = "persistent://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Set.of("test"));

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
     * Verify unloading topic.
     *
     * @throws Exception
     */
    @Test(dataProvider = "topicType")
    public void testUnloadTopic(final String topicType) throws Exception {

        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = topicType + "://" + namespace + "/topic1";
        admin.namespaces().createNamespace(namespace, Set.of("test"));

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
        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topicName).create();
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
    @Test(dataProvider = "namespaceNames", timeOut = 30000)
    public void testResetCursorOnPosition(String namespaceName) throws Exception {
        restartClusterAfterTest();
        final String topicName = "persistent://" + defaultTenant + "/use/" + namespaceName + "/resetPosition";
        final int totalProducedMessages = 50;

        // set retention
        admin.namespaces().setRetention(defaultNamespace, new RetentionPolicies(10, 10));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), List.of("my-sub"));

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
            assertTrue(e.getMessage().contains(topicName));
            // Ok
        }

        // invalid cursor name
        try {
            admin.topics().resetCursor(topicName, "invalid-sub", messageId);
            fail("It should have failed due to invalid subscription name");
        } catch (PulsarAdminException.NotFoundException e) {
            assertTrue(e.getMessage().contains("invalid-sub"));
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

    @Test
    public void shouldNotSupportResetOnPartitionedTopic() throws PulsarAdminException, PulsarClientException {
        final String partitionedTopicName = "persistent://" + defaultNamespace + "/" + newUniqueName("parttopic");
        admin.topics().createPartitionedTopic(partitionedTopicName, 4);
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(partitionedTopicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared).subscribe();
        try {
            admin.topics().resetCursor(partitionedTopicName, "my-sub", MessageId.earliest);
            fail();
        } catch (PulsarAdminException.NotAllowedException e) {
            assertTrue(e.getMessage().contains("Reset-cursor at position is not allowed for partitioned-topic"),
                    "Condition doesn't match. Actual message:" + e.getMessage());
        }
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
        final String namespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(namespace, Set.of("test"));

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
     * It verifies that pulsar with different load-manager generates different load-report and returned by admin-api.
     *
     * @throws Exception
     */
    @Test
    public void testLoadReportApi() throws Exception {
        restartClusterAfterTest();
        this.conf.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        @Cleanup("cleanup")
        MockedPulsarService mockPulsarSetup1 = new MockedPulsarService(this.conf);
        mockPulsarSetup1.setup();
        PulsarAdmin simpleLoadManagerAdmin = mockPulsarSetup1.getAdmin();
        assertNotNull(simpleLoadManagerAdmin.brokerStats().getLoadReport());

        this.conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        @Cleanup("cleanup")
        MockedPulsarService mockPulsarSetup2 = new MockedPulsarService(this.conf);
        mockPulsarSetup2.setup();
        PulsarAdmin modularLoadManagerAdmin = mockPulsarSetup2.getAdmin();
        assertNotNull(modularLoadManagerAdmin.brokerStats().getLoadReport());
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

        admin.clusters().updatePeerClusterNames("us-west1", new LinkedHashSet<>(List.of("us-west2")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(), Set.of("us-west2"));
        assertNull(admin.clusters().getCluster("us-west2").getPeerClusterNames());
        // update cluster with duplicate peer-clusters in the list
        admin.clusters().updatePeerClusterNames("us-west1",
                new LinkedHashSet<>(List.of("us-west2", "us-east1", "us-west2", "us-east1", "us-west2", "us-east1")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                List.of("us-west2", "us-east1"));
        admin.clusters().updatePeerClusterNames("us-west1", null);
        assertNull(admin.clusters().getCluster("us-west1").getPeerClusterNames());

        // Check name validation
        try {
            admin.clusters().updatePeerClusterNames("us-west1",
                    new LinkedHashSet<>(List.of("invalid-cluster")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // Cluster itself can't be part of peer-list
        try {
            admin.clusters().updatePeerClusterNames("us-west1", new LinkedHashSet<>(List.of("us-west1")));
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    /**
     * It validates that peer-cluster can't coexist in replication-cluster list.
     *
     * @throws Exception
     */
    @Test
    public void testReplicationPeerCluster() throws Exception {
        restartClusterAfterTest();

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
                List.of("test", "us-east1", "us-east2", "us-west1", "us-west2", "us-west3", "us-west4"));

        final String property = newUniqueName("peer-prop");
        Set<String> allowedClusters = Set.of("us-west1", "us-west2", "us-west3", "us-west4", "us-east1",
                "us-east2", "global");
        TenantInfoImpl propConfig = new TenantInfoImpl(Set.of("test"), allowedClusters);
        admin.tenants().createTenant(property, propConfig);

        final String namespace = property + "/global/conflictPeer";
        admin.namespaces().createNamespace(namespace);

        admin.clusters().updatePeerClusterNames("us-west1",
                new LinkedHashSet<>(List.of("us-west2", "us-west3")));
        assertEquals(admin.clusters().getCluster("us-west1").getPeerClusterNames(),
                List.of("us-west2", "us-west3"));

        // (1) no conflicting peer
        Set<String> clusterIds = Set.of("us-east1", "us-east2");
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        // (2) conflicting peer
        clusterIds = Set.of("us-west2", "us-west3", "us-west1");
        try {
            admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
            fail("Peer-cluster can't coexist in replication cluster list");
        } catch (PulsarAdminException.ConflictException e) {
            // Ok
        }

        clusterIds = Set.of("us-west2", "us-west3");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);

        clusterIds = Set.of("us-west1", "us-west4");
        // no peer coexist in replication clusters
        admin.namespaces().setNamespaceReplicationClusters(namespace, clusterIds);
    }

    @Test
    public void clusterFailureDomain() throws PulsarAdminException {

        final String cluster = pulsar.getConfiguration().getClusterName();
        // create
        FailureDomain domain = FailureDomain.builder()
                .brokers(Set.of("b1", "b2", "b3"))
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
        final String namespace = defaultNamespace;
        final String antiAffinityGroup = "group";
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));
        admin.namespaces().setNamespaceAntiAffinityGroup(namespace, antiAffinityGroup);
        assertEquals(admin.namespaces().getNamespaceAntiAffinityGroup(namespace), antiAffinityGroup);
        admin.namespaces().deleteNamespaceAntiAffinityGroup(namespace);
        assertTrue(isBlank(admin.namespaces().getNamespaceAntiAffinityGroup(namespace)));

        final String ns1 = defaultTenant + "/antiAG1";
        final String ns2 = defaultTenant + "/antiAG2";
        final String ns3 = defaultTenant + "/antiAG3";
        admin.namespaces().createNamespace(ns1, Set.of("test"));
        admin.namespaces().createNamespace(ns2, Set.of("test"));
        admin.namespaces().createNamespace(ns3, Set.of("test"));
        admin.namespaces().setNamespaceAntiAffinityGroup(ns1, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns2, antiAffinityGroup);
        admin.namespaces().setNamespaceAntiAffinityGroup(ns3, antiAffinityGroup);

        Set<String> namespaces = new HashSet<>(
                admin.namespaces().getAntiAffinityNamespaces(defaultTenant, "test", antiAffinityGroup));
        assertEquals(namespaces.size(), 3);
        assertTrue(namespaces.contains(ns1));
        assertTrue(namespaces.contains(ns2));
        assertTrue(namespaces.contains(ns3));

        List<String> namespaces2 = admin.namespaces().getAntiAffinityNamespaces(defaultTenant, "test", "invalid-group");
        assertEquals(namespaces2.size(), 0);
    }

    @Test
    public void testPersistentTopicList() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = "non-persistent://" + namespace + "/bundle-topic";
        admin.namespaces().createNamespace(namespace, 20);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Set.of("test"));
        int totalTopics = 100;

        Set<String> topicNames = new HashSet<>();
        for (int i = 0; i < totalTopics; i++) {
            topicNames.add(topicName + i);
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName + i).create();
            producer.close();
        }

        Set<String> topics = new HashSet<>();
        String bundle = pulsar.getNamespaceService().getNamespaceBundleFactory()
                .getBundle(TopicName.get(topicName + "0")).getBundleRange();
        for (int i = 0; i < totalTopics; i++) {
            Topic topic = pulsar.getBrokerService().getTopicReference(topicName + i).get();
            if (bundle.equals(pulsar.getNamespaceService().getNamespaceBundleFactory()
                    .getBundle(TopicName.get(topicName + i)).getBundleRange())) {
                topics.add(topic.getName());
            }
        }

        Set<String> topicsInNs = Sets
                .newHashSet(
                        admin.topics().getList(namespace, null, Collections.singletonMap(QueryParam.Bundle, bundle)));
        assertEquals(topicsInNs.size(), topics.size());
        topicsInNs.removeAll(topics);
        assertEquals(topicsInNs.size(), 0);
    }

    @Test
    public void testCreateAndGetTopicProperties() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String nonPartitionedTopicName = "persistent://" + namespace + "/non-partitioned-TopicProperties";
        admin.namespaces().createNamespace(namespace, 20);
        Map<String, String> nonPartitionedTopicProperties = new HashMap<>();
        nonPartitionedTopicProperties.put("key1", "value1");
        admin.topics().createNonPartitionedTopic(nonPartitionedTopicName, nonPartitionedTopicProperties);
        Map<String, String> properties11 = admin.topics().getProperties(nonPartitionedTopicName);
        Assert.assertNotNull(properties11);
        Assert.assertEquals(properties11.get("key1"), "value1");

        final String partitionedTopicName = "persistent://" + namespace + "/partitioned-TopicProperties";
        Map<String, String> partitionedTopicProperties = new HashMap<>();
        partitionedTopicProperties.put("key2", "value2");
        admin.topics().createPartitionedTopic(partitionedTopicName, 2, partitionedTopicProperties);
        Map<String, String> properties22 = admin.topics().getProperties(partitionedTopicName);
        Assert.assertNotNull(properties22);
        Assert.assertEquals(properties22.get("key2"), "value2");
    }

    @Test
    public void testUpdatePartitionedTopicProperties() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = "persistent://" + namespace + "/testUpdatePartitionedTopicProperties";
        final String topicNameTwo = "persistent://" + namespace + "/testUpdatePartitionedTopicProperties2";
        admin.namespaces().createNamespace(namespace, 20);

        // create partitioned topic without properties
        admin.topics().createPartitionedTopic(topicName, 2);
        Map<String, String> properties = admin.topics().getProperties(topicName);
        Assert.assertNull(properties);
        Map<String, String> topicProperties = new HashMap<>();
        topicProperties.put("key1", "value1");
        admin.topics().updateProperties(topicName, topicProperties);
        properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.get("key1"), "value1");

        // update with new key, old properties should keep
        topicProperties = new HashMap<>();
        topicProperties.put("key2", "value2");
        admin.topics().updateProperties(topicName, topicProperties);
        properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.size(), 2);
        Assert.assertEquals(properties.get("key1"), "value1");
        Assert.assertEquals(properties.get("key2"), "value2");

        // override old values
        topicProperties = new HashMap<>();
        topicProperties.put("key1", "value11");
        admin.topics().updateProperties(topicName, topicProperties);
        properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.size(), 2);
        Assert.assertEquals(properties.get("key1"), "value11");
        Assert.assertEquals(properties.get("key2"), "value2");

        // create topic without properties
        admin.topics().createPartitionedTopic(topicNameTwo, 2);
        properties = admin.topics().getProperties(topicNameTwo);
        Assert.assertNull(properties);
        // remove key of properties on this topic
        admin.topics().removeProperties(topicNameTwo, "key1");
        properties = admin.topics().getProperties(topicNameTwo);
        Assert.assertNull(properties);
        Map<String, String> topicProp = new HashMap<>();
        topicProp.put("key1", "value1");
        topicProp.put("key2", "value2");
        admin.topics().updateProperties(topicNameTwo, topicProp);
        properties = admin.topics().getProperties(topicNameTwo);
        Assert.assertEquals(properties, topicProp);
        admin.topics().removeProperties(topicNameTwo, "key1");
        topicProp.remove("key1");
        properties = admin.topics().getProperties(topicNameTwo);
        Assert.assertEquals(properties, topicProp);
    }

    @Test
    public void testUpdateNonPartitionedTopicProperties() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = "persistent://" + namespace + "/testUpdateNonPartitionedTopicProperties";
        admin.namespaces().createNamespace(namespace, 20);

        // create non-partitioned topic with properties
        Map<String, String> topicProperties = new HashMap<>();
        topicProperties.put("key1", "value1");
        admin.topics().createNonPartitionedTopic(topicName, topicProperties);
        Map<String, String> properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.get("key1"), "value1");

        // update with new key, old properties should keep
        topicProperties = new HashMap<>();
        topicProperties.put("key2", "value2");
        admin.topics().updateProperties(topicName, topicProperties);
        properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.size(), 2);
        Assert.assertEquals(properties.get("key1"), "value1");
        Assert.assertEquals(properties.get("key2"), "value2");

        // override old values
        topicProperties = new HashMap<>();
        topicProperties.put("key1", "value11");
        admin.topics().updateProperties(topicName, topicProperties);
        properties = admin.topics().getProperties(topicName);
        Assert.assertNotNull(properties);
        Assert.assertEquals(properties.size(), 2);
        Assert.assertEquals(properties.get("key1"), "value11");
        Assert.assertEquals(properties.get("key2"), "value2");
    }

    @Test
    public void testNonPersistentTopics() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns2");
        final String topicName = "non-persistent://" + namespace + "/topic";
        admin.namespaces().createNamespace(namespace, 20);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Set.of("test"));
        int totalTopics = 100;

        Set<String> topicNames = new HashSet<>();
        for (int i = 0; i < totalTopics; i++) {
            topicNames.add(topicName + i);
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName + i).create();
            producer.close();
        }

        for (int i = 0; i < totalTopics; i++) {
            Topic topic = pulsar.getBrokerService().getTopicReference(topicName + i).get();
            assertNotNull(topic);
        }

        Set<String> topicsInNs = new HashSet<>(admin.topics().getList(namespace));
        assertEquals(topicsInNs.size(), totalTopics);
        topicsInNs.removeAll(topicNames);
        assertEquals(topicsInNs.size(), 0);
    }

    @Test
    public void testPublishConsumerStats() throws Exception {
        final String topicName = "statTopic";
        final String subscriberName = topicName + "-my-sub-1";
        final String topic = "persistent://" + defaultNamespace + "/" + topicName;
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
        restartClusterAfterTest();

        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("prop_xyz", tenantInfo);

        admin.namespaces().createNamespace("prop_xyz/my-namespace", Set.of("test"));

        String topic = "persistent://prop_xyz/use/my-namespace/my-topic";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .create();

        TopicStats stats = admin.topics().getStats(topic);
        assertEquals(stats.getPublishers().size(), 1);
    }

    @Test
    public void testTenantNameWithInvalidCharacters() {
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));

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

        Set<String> allowedClusters = Set.of("cluster-non-existing");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), allowedClusters);

        // If we try to create tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().createTenant("test-tenant", tenantInfo);
            fail("Should have failed");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        assertFalse(admin.tenants().getTenants().contains("test-tenant"));

        // Check existing tenant
        assertTrue(admin.tenants().getTenants().contains(defaultTenant));

        // If we try to update existing tenant with nonexistent clusters, it should fail immediately
        try {
            admin.tenants().updateTenant(defaultTenant, tenantInfo);
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
        String brokerAddress = pulsar.getBrokerId();

        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");

        NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList(namespaceRegex))
                .primary(Collections.singletonList(brokerName))
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
        assertEquals(brokerIsolationDataList.get(0).getPolicyName(), policyName1);

        BrokerNamespaceIsolationDataImpl brokerIsolationData = (BrokerNamespaceIsolationDataImpl) admin.clusters()
                .getBrokerWithNamespaceIsolationPolicy(cluster, brokerAddress);
        assertEquals(brokerIsolationData.getBrokerName(), brokerAddress);
        assertEquals(brokerIsolationData.getNamespaceRegex().size(), 1);
        assertEquals(brokerIsolationData.getNamespaceRegex().get(0), namespaceRegex);

        BrokerNamespaceIsolationDataImpl isolationData = (BrokerNamespaceIsolationDataImpl) admin.clusters()
                .getBrokerWithNamespaceIsolationPolicy(cluster, "invalid-broker");
        assertFalse(isolationData.isPrimary());

        admin.clusters().deleteNamespaceIsolationPolicy(cluster, policyName1);
    }

    // create 1 namespace:
    //  0. without isolation policy configured, lookup will success.
    //  1. with matched isolation broker configured and matched, lookup will success.
    //  2. update isolation policy, without broker matched, lookup will fail.
    @Test
    public void brokerNamespaceIsolationPoliciesUpdateOnTime() throws Exception {
        String brokerName = pulsar.getAdvertisedAddress();
        String ns1Name = defaultTenant + "/test_ns1_iso_" + System.currentTimeMillis();
        admin.namespaces().createNamespace(ns1Name, Set.of("test"));

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

        final List<String> primaryList = new ArrayList<>();
        primaryList.add(brokerName + ".*");
        NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList(ns1Name))
                .primary(primaryList)
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
        nsPolicyData1.getPrimary().clear();
        nsPolicyData1.getPrimary().add(brokerName + "not_match");
        admin.clusters().updateNamespaceIsolationPolicyAsync(cluster, policyName1, nsPolicyData1).get();

        try {
            admin.lookups().lookupTopic(ns1Name + "/topic3");
            fail();
        } catch (Exception e) {
            // expected lookup fail, because no brokers matched the policy.
            log.info(" 2 expected fail lookup");
        }

        try {
            admin.lookups().lookupTopic(ns1Name + "/topic1");
            fail();
        } catch (Exception e) {
            // expected lookup fail, because no brokers matched the policy.
            log.info(" 22 expected fail lookup");
        }

        admin.clusters().deleteNamespaceIsolationPolicy(cluster, policyName1);
    }

    @Test
    public void clustersList() throws PulsarAdminException {
        final String cluster = pulsar.getConfiguration().getClusterName();
        admin.clusters().createCluster("global", ClusterData.builder()
                .serviceUrl("http://localhost:6650").build());

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.clusters().getClusters(), List.of(cluster));
    }
    /**
     * verifies cluster has been set before create topic.
     *
     * @throws PulsarAdminException
     */
    @Test
    public void testClusterIsReadyBeforeCreateTopic() throws Exception {
        restartClusterAfterTest();
        final String topicName = "partitionedTopic";
        final int partitions = 4;
        final String persistentPartitionedTopicName = "persistent://" + defaultTenant + "/ns2/" + topicName;
        final String nonPersistentPartitionedTopicName = "non-persistent://" + defaultTenant + "/ns2/" + topicName;

        admin.namespaces().createNamespace(defaultTenant + "/ns2");
        // By default the cluster will configure as configuration file. So the create topic operation
        // will never throw exception except there is no cluster.
        admin.namespaces().setNamespaceReplicationClusters(defaultTenant + "/ns2", Sets.newHashSet(configClusterName));

        admin.topics().createPartitionedTopic(persistentPartitionedTopicName, partitions);
        admin.topics().createPartitionedTopic(nonPersistentPartitionedTopicName, partitions);
    }

    @Test
    public void testCreateNamespaceWithNoClusters() throws PulsarAdminException {
        String localCluster = pulsar.getConfiguration().getClusterName();
        String namespace = newUniqueName(defaultTenant + "/test-ns-with-no-clusters");
        admin.namespaces().createNamespace(namespace);

        // Global cluster, if there, should be omitted from the results
        assertEquals(admin.namespaces().getNamespaceReplicationClusters(namespace),
                Collections.singletonList(localCluster));
    }

    @Test(timeOut = 30000)
    public void testConsumerStatsLastTimestamp() throws PulsarClientException, PulsarAdminException,
            InterruptedException {
        long timestamp = System.currentTimeMillis();
        final String topicName = "consumer-stats-" + timestamp;
        final String subscribeName = topicName + "-test-stats-sub";
        final String topic = "persistent://" + defaultNamespace + "/" + topicName;
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
    public void testPreciseBacklog() throws Exception {
        restartClusterIfReused();

        final String topic = "persistent://" + defaultNamespace + "/precise-back-log";
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

        // Mock the entries added count. Default is disable the precise backlog,
        // so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        assertNotNull(subscription);
        ((ManagedLedgerImpl) subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);

        topicStats = admin.topics().getStats(topic, true, true);
        assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 40);
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

    @Test
    public void testDeleteTenant() throws Exception {
        restartClusterAfterTest();
        // Disabled conf: systemTopicEnabled. see: https://github.com/apache/pulsar/pull/17070
        boolean originalSystemTopicEnabled = conf.isSystemTopicEnabled();
        if (originalSystemTopicEnabled) {
            cleanup();
            conf.setSystemTopicEnabled(false);
            setup();
        }
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

        String tenant = newUniqueName("test-tenant-1");
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test")));
        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace
        String namespace = tenant + "/test-ns-1";
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        assertEquals(admin.namespaces().getNamespaces(tenant), List.of(namespace));

        // create topic
        String topic = namespace + "/test-topic-1";
        admin.topics().createPartitionedTopic(topic, 10);
        assertFalse(admin.topics().getList(namespace).isEmpty());

        try {
            admin.namespaces().deleteNamespace(namespace, false);
            fail("should have failed due to namespace not empty");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete non-empty tenant
        }

        // delete topic
        admin.topics().deletePartitionedTopic(topic);
        assertTrue(admin.topics().getList(namespace).isEmpty());

        // delete namespace
        deleteNamespaceWithRetry(namespace, false);
        assertFalse(admin.namespaces().getNamespaces(tenant).contains(namespace));
        assertTrue(admin.namespaces().getNamespaces(tenant).isEmpty());

        // delete tenant
        admin.tenants().deleteTenant(tenant);
        assertFalse(admin.tenants().getTenants().contains(tenant));

        final String managedLedgersPath = "/managed-ledgers/" + tenant;
        final String partitionedTopicPath = "/admin/partitioned-topics/" + tenant;
        final String localPoliciesPath = "/admin/local-policies/" + tenant;
        final String bundleDataPath = BUNDLE_DATA_BASE_PATH + "/" + tenant;
        assertFalse(pulsar.getLocalMetadataStore().exists(managedLedgersPath).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(partitionedTopicPath).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(localPoliciesPath).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(bundleDataPath).join());
    }

    @Data
    @AllArgsConstructor
    private static class NamespaceAttr {
        private boolean systemTopicEnabled;
        private TopicType autoTopicCreationType;
        private int defaultNumPartitions;
        private boolean forceDeleteNamespaceAllowed;
    }

    @DataProvider(name = "namespaceAttrs")
    public Object[][] namespaceAttributes(){
        return new Object[][]{
                {new NamespaceAttr(false, TopicType.NON_PARTITIONED, 0, false)},
                {new NamespaceAttr(true, TopicType.NON_PARTITIONED, 0, false)},
                {new NamespaceAttr(true, TopicType.PARTITIONED, 3, false)}
        };
    }

    private NamespaceAttr markOriginalNamespaceAttr(){
        return new NamespaceAttr(conf.isSystemTopicEnabled(), conf.getAllowAutoTopicCreationType(),
                conf.getDefaultNumPartitions(), conf.isForceDeleteNamespaceAllowed());
    }

    private void setNamespaceAttr(NamespaceAttr namespaceAttr){
        conf.setSystemTopicEnabled(namespaceAttr.systemTopicEnabled);
        conf.setAllowAutoTopicCreationType(namespaceAttr.autoTopicCreationType);
        conf.setDefaultNumPartitions(namespaceAttr.defaultNumPartitions);
        conf.setForceDeleteNamespaceAllowed(namespaceAttr.forceDeleteNamespaceAllowed);
    }

    @Test(dataProvider = "namespaceAttrs")
    public void testDeleteNamespace(NamespaceAttr namespaceAttr) throws Exception {
        restartClusterAfterTest();

        // Set conf.
        cleanup();
        setNamespaceAttr(namespaceAttr);
        this.conf.setMetadataStoreUrl("127.0.0.1:2181");
        this.conf.setConfigurationMetadataStoreUrl("127.0.0.1:2182");
        setup();

        String tenant = newUniqueName("test-tenant");
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test")));
        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace
        String namespace = tenant + "/test-ns";
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        assertEquals(admin.namespaces().getNamespaces(tenant), List.of(namespace));

        // create topic
        String topic = namespace + "/test-topic";
        admin.topics().createPartitionedTopic(topic, 10);
        assertFalse(admin.topics().getList(namespace).isEmpty());

        final String managedLedgersPath = "/managed-ledgers/" + namespace;
        final String bundleDataPath = BUNDLE_DATA_BASE_PATH + "/" + namespace;
        // Trigger bundle owned by brokers.
        pulsarClient.newProducer().topic(topic).create().close();
        // Trigger bundle data write to ZK.
        Awaitility.await().untilAsserted(() -> {
            boolean bundleDataWereWriten = false;
            for (PulsarService ps : new PulsarService[]{pulsar, mockPulsarSetup.getPulsar()}) {
                ModularLoadManagerWrapper loadManager = (ModularLoadManagerWrapper) ps.getLoadManager().get();
                ModularLoadManagerImpl loadManagerImpl = (ModularLoadManagerImpl) loadManager.getLoadManager();
                ps.getBrokerService().updateRates();
                loadManagerImpl.updateLocalBrokerData();
                loadManagerImpl.writeBundleDataOnZooKeeper();
                bundleDataWereWriten = bundleDataWereWriten || ps.getLocalMetadataStore().exists(bundleDataPath).join();
            }
            assertTrue(bundleDataWereWriten);
        });

        // assert znode exists in metadata store
        assertTrue(pulsar.getLocalMetadataStore().exists(bundleDataPath).join());
        assertTrue(pulsar.getLocalMetadataStore().exists(managedLedgersPath).join());

        try {
            admin.namespaces().deleteNamespace(namespace, false);
            fail("should have failed due to namespace not empty");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete non-empty tenant
        }

        // delete topic
        admin.topics().deletePartitionedTopic(topic);
        assertTrue(admin.topics().getList(namespace).isEmpty());

        // delete namespace
        deleteNamespaceWithRetry(namespace, false);
        assertFalse(admin.namespaces().getNamespaces(tenant).contains(namespace));
        assertTrue(admin.namespaces().getNamespaces(tenant).isEmpty());

        // assert znode deleted in metadata store
        assertFalse(pulsar.getLocalMetadataStore().exists(managedLedgersPath).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(bundleDataPath).join());
    }

    @Test
    public void testDeleteNamespaceWithTopicPolicies() throws Exception {
        String tenant = newUniqueName("test-tenant");
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test")));
        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace2
        String namespace = tenant + "/test-ns2";
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        admin.topics().createNonPartitionedTopic(namespace + "/tobedeleted");
        // verify namespace can be deleted even without topic policy events
        admin.namespaces().deleteNamespace(namespace, true);

        Awaitility.await().untilAsserted(() -> {
            final CompletableFuture<Optional<Topic>> eventTopicFuture =
                    pulsar.getBrokerService().getTopics().get("persistent://test-tenant/test-ns2/__change_events");
            assertNull(eventTopicFuture);
        });
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        // create topic
        String topic = namespace + "/test-topic2";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producer.send("test".getBytes(StandardCharsets.UTF_8));
        BacklogQuota backlogQuota = BacklogQuotaImpl
                .builder()
                .limitTime(1000)
                .limitSize(1000)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();
        admin.topicPolicies().setBacklogQuota(topic, backlogQuota);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topicPolicies()
                    .getBacklogQuotaMap(topic)
                    .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota);
        });
        producer.close();
        admin.topics().delete(topic);
        deleteNamespaceWithRetry(namespace, false);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(admin.namespaces().getNamespaces(tenant).isEmpty());
        });
    }


    @Test(timeOut = 30000)
    public void testBacklogNoDelayed() throws PulsarClientException, PulsarAdminException, InterruptedException {
        final String topic = "persistent://" + defaultNamespace + "/precise-back-log-no-delayed-"
                + UUID.randomUUID().toString();
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
    public void testPartitionedTopicStatsIncludeConsumerName() throws PulsarClientException, PulsarAdminException {
        final String topic = "persistent://" + defaultNamespace + "/" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 2);
        final String subName = "sub-name";
        final String consumerName = "consumer-name";

        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getWebServiceAddress()).build();

        @Cleanup
        final Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subName)
                .consumerName(consumerName)
                .subscribe();

        final TopicStats topicStats = admin.topics().getPartitionedStats(topic, false);

        assertEquals(topicStats.getSubscriptions().get(subName).getConsumers().get(0).getConsumerName(), consumerName);
    }

    @Test
    public void testPreciseBacklogForPartitionedTopic() throws PulsarClientException, PulsarAdminException {
        final String topic = "persistent://" + defaultNamespace + "/precise-back-log-for-partitioned-topic";
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

        // Mock the entries added count. Default is disable the precise backlog,
        // so the backlog is entries added count - consumed count
        // Since message have not acked, so the backlog is 10
        for (int i = 0; i < 2; i++) {
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService().
                    getTopicReference(topic + "-partition-" + i).get().getSubscription(subName);
            assertNotNull(subscription);
            ((ManagedLedgerImpl) subscription.getCursor().getManagedLedger()).setEntriesAddedCounter(10L);
        }

        TopicStats topicStats = admin.topics().getPartitionedStats(topic, false);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 20);

        topicStats = admin.topics().getPartitionedStats(topic, false, true, true, true);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 1);
        assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 40);
    }

    @Test(timeOut = 30000)
    public void testBacklogNoDelayedForPartitionedTopic() throws PulsarClientException,
            PulsarAdminException, InterruptedException {
        final String topic = "persistent://" + defaultNamespace + "/precise-back-log-no-delayed-partitioned-topic";
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

        long start1 = 0;
        long start2 = 0;
        @Cleanup
        Producer<byte[]> producer = client.newProducer()
            .topic(topic)
            .enableBatching(false)
            .create();

        for (int i = 0; i < 10; i++) {
            if (i == 0) {
                start1 = Clock.systemUTC().millis();
            }
            if (i == 5) {
                start2 = Clock.systemUTC().millis();
            }
            if (i > 4) {
                producer.newMessage()
                    .value("message-1".getBytes(StandardCharsets.UTF_8))
                    .deliverAfter(10, TimeUnit.SECONDS)
                    .send();
            } else {
                producer.send("message-1".getBytes(StandardCharsets.UTF_8));
            }
        }
        // wait until the message add to delay queue.
        long finalStart1 = start1;
        Awaitility.await().untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getPartitionedStats(topic, false, true, true, true);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);
            assertEquals(topicStats.getSubscriptions().get(subName).getBacklogSize(), 440);
            assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 5);
            assertTrue(topicStats.getSubscriptions().get(subName).getEarliestMsgPublishTimeInBacklog() >= finalStart1);
        });

        for (int i = 0; i < 5; i++) {
            consumer.acknowledge(consumer.receive());
        }
        // Wait the ack send.
        long finalStart2 = start2;
        Awaitility.await().timeout(1, MINUTES).untilAsserted(() -> {
            TopicStats topicStats2 = admin.topics().getPartitionedStats(topic, false, true, true, true);
            assertEquals(topicStats2.getSubscriptions().get(subName).getMsgBacklog(), 5);
            assertEquals(topicStats2.getSubscriptions().get(subName).getBacklogSize(), 223);
            assertEquals(topicStats2.getSubscriptions().get(subName).getMsgBacklogNoDelayed(), 0);
            assertTrue(topicStats2.getSubscriptions().get(subName).getEarliestMsgPublishTimeInBacklog() >= finalStart2);
        });

    }

    @Test
    public void testMaxNumPartitionsPerPartitionedTopicSuccess() {
        restartClusterAfterTest();
        final String topic = "persistent://" + defaultNamespace + "/max-num-partitions-per-partitioned-topic-success";
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
        restartClusterAfterTest();
        final String topic = "persistent://" + defaultNamespace + "/max-num-partitions-per-partitioned-topic-failure";
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
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        String tenantName = newUniqueName("prop-xyz2");
        admin.tenants().createTenant(tenantName, tenantInfo);
        admin.namespaces().createNamespace(tenantName + "/ns1", 10);
        admin.namespaces().setNamespaceReplicationClusters(tenantName + "/ns1", Set.of("test"));
        admin.namespaces().createNamespace(tenantName + "/test/ns2", 10);
        assertEquals(admin.namespaces().getBundles(tenantName + "/ns1").getNumBundles(), 10);
        assertEquals(admin.namespaces().getBundles(tenantName + "/test/ns2").getNumBundles(), 10);

        admin.namespaces().deleteNamespace(tenantName + "/test/ns2");
    }

    @Test
    public void testForceDeleteNamespace() throws Exception {
        restartClusterAfterTest();
        String tenantName = newUniqueName("prop-xyz2");
        final String namespaceName = tenantName + "/ns1";
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(tenantName, tenantInfo);
        admin.namespaces().createNamespace(namespaceName, 1);
        final String topic = "persistent://" + namespaceName + "/test" + UUID.randomUUID();
        pulsarClient.newProducer(Schema.DOUBLE).topic(topic).create().close();
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.schemas().getSchemaInfo(topic)));
        deleteNamespaceWithRetry(namespaceName, true);
        try {
            admin.schemas().getSchemaInfo(topic);
            Assert.fail("fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 404);
        }
    }

    @Test
    public void testForceDeleteNamespaceWithAutomaticTopicCreation() throws Exception {
        conf.setForceDeleteNamespaceAllowed(true);
        String tenantName = newUniqueName("prop-xyz2");
        final String namespaceName = tenantName + "/ns1";
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(tenantName, tenantInfo);
        admin.namespaces().createNamespace(namespaceName, 1);
        admin.namespaces().setAutoTopicCreation(namespaceName,
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("partitioned")
                        .defaultNumPartitions(20)
                        .build());
        final String topic = "persistent://" + namespaceName + "/test" + UUID.randomUUID();

        // start a consumer, that creates the topic
        try (Consumer<Double> consumer = pulsarClient.newConsumer(Schema.DOUBLE).topic(topic)
                .subscriptionName("test").autoUpdatePartitions(true).subscribe()) {

            // wait for the consumer to settle
            Awaitility.await().ignoreExceptions().untilAsserted(() ->
                    assertNotNull(admin.topics().getSubscriptions(topic).contains("test")));

            // verify that the partitioned topic is created
            assertEquals(20, admin.topics().getPartitionedTopicMetadata(topic).partitions);

            // the consumer will race with the deletion
            // the consumer will try to re-create the partitions
            admin.namespaces().deleteNamespace(namespaceName, true);

            assertFalse(admin.namespaces().getNamespaces(tenantName).contains("ns1"));
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
                .proxyServiceUrl("pulsar://example.com")
                .proxyProtocol(ProxyProtocol.SNI)
                .build();
        admin.clusters().updateCluster(clusterName, cluster);
        Assert.assertEquals(admin.clusters().getCluster(clusterName), cluster);
    }

    @Test
    public void testMaxNamespacesPerTenant() throws Exception {
        restartClusterAfterTest();
        cleanup();
        conf.setMaxNamespacesPerTenant(2);
        setup();
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));
        admin.namespaces().createNamespace("testTenant/ns2", Set.of("test"));
        try {
            admin.namespaces().createNamespace("testTenant/ns3", Set.of("test"));
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
            Assert.assertEquals(e.getHttpError(), "Exceed the maximum number of namespace in tenant :testTenant");
        }
    }

    @Test
    public void testAutoTopicCreationOverrideWithMaxNumPartitionsLimit() throws Exception{
        restartClusterAfterTest();
        cleanup();
        conf.setMaxNumPartitionsPerPartitionedTopic(10);
        setup();
        TenantInfoImpl tenantInfo = new TenantInfoImpl(
                Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        // test non-partitioned
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));
        AutoTopicCreationOverride overridePolicy = AutoTopicCreationOverride
                .builder().allowAutoTopicCreation(true)
                .topicType("non-partitioned")
                .build();
        admin.namespaces().setAutoTopicCreation("testTenant/ns1", overridePolicy);
        AutoTopicCreationOverride newOverridePolicy =
                admin.namespaces().getAutoTopicCreation("testTenant/ns1");
        assertEquals(overridePolicy, newOverridePolicy);
        // test partitioned
        AutoTopicCreationOverride partitionedOverridePolicy = AutoTopicCreationOverride
                .builder().allowAutoTopicCreation(true)
                .topicType("partitioned")
                .defaultNumPartitions(10)
                .build();
        admin.namespaces().setAutoTopicCreation("testTenant/ns1", partitionedOverridePolicy);
        AutoTopicCreationOverride partitionedNewOverridePolicy =
                admin.namespaces().getAutoTopicCreation("testTenant/ns1");
        assertEquals(partitionedOverridePolicy, partitionedNewOverridePolicy);
        // test partitioned with error
        AutoTopicCreationOverride partitionedWrongOverridePolicy = AutoTopicCreationOverride
                .builder().allowAutoTopicCreation(true)
                .topicType("partitioned")
                .defaultNumPartitions(123)
                .build();
        try {
            admin.namespaces().setAutoTopicCreation("testTenant/ns1", partitionedWrongOverridePolicy);
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof NotAcceptableException);
        }
    }
    @Test
    public void testMaxTopicsPerNamespace() throws Exception {
        restartClusterAfterTest();
        cleanup();
        conf.setMaxTopicsPerNamespace(10);
        setup();
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

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
        cleanup();
        conf.setMaxTopicsPerNamespace(0);
        setup();
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));
        for (int i = 0; i < 10; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 2);
            admin.topics().createNonPartitionedTopic(topic + i + i);
        }

        // check first create normal topic, then system topics, unlimited even setMaxTopicsPerNamespace
        cleanup();
        conf.setMaxTopicsPerNamespace(5);
        setup();
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));
        for (int i = 0; i < 5; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 1);
        }
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__change_events", 6);


        // check first create system topics, then normal topic, unlimited even setMaxTopicsPerNamespace
        cleanup();
        conf.setMaxTopicsPerNamespace(5);
        setup();
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));
        admin.topics().createPartitionedTopic("persistent://testTenant/ns1/__change_events", 6);
        for (int i = 0; i < 5; ++i) {
            admin.topics().createPartitionedTopic(topic + i, 1);
        }

        // check producer/consumer auto create partitioned topic
        cleanup();
        conf.setMaxTopicsPerNamespace(10);
        conf.setDefaultNumPartitions(3);
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        setup();
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

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
        cleanup();
        conf.setMaxTopicsPerNamespace(3);
        conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        setup();
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

        pulsarClient.newProducer().topic(topic + "1").create().close();
        pulsarClient.newProducer().topic(topic + "2").create().close();
        pulsarClient.newConsumer().topic(topic + "3").subscriptionName("test_sub").subscribe().close();
        try {
            pulsarClient.newConsumer().topic(topic + "4").subscriptionName("test_sub").subscribe().close();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Exception: ", e);
        }
    }

    @Test
    public void testInvalidBundleErrorResponse() throws Exception {
        try {
            admin.namespaces().deleteNamespaceBundle(defaultNamespace, "invalid-bundle");
            fail("should have failed due to invalid bundle");
        } catch (PreconditionFailedException e) {
            assertTrue(e.getMessage().startsWith("Invalid bundle range"));
        }
    }

    @Test
    public void testMaxSubscriptionsPerTopic() throws Exception {
        restartClusterAfterTest();
        cleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        setup();

        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

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

        cleanup();
        conf.setMaxSubscriptionsPerTopic(0);
        setup();

        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

        admin.topics().createPartitionedTopic(topic, 3);
        producer = pulsarClient.newProducer().topic(topic).create();
        producer.close();

        for (int i = 0; i < 10; ++i) {
            admin.topics().createSubscription(topic, "test-sub" + i, MessageId.earliest);
        }

        cleanup();
        conf.setMaxSubscriptionsPerTopic(2);
        setup();

        admin.tenants().createTenant("testTenant", tenantInfo);
        admin.namespaces().createNamespace("testTenant/ns1", Set.of("test"));

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
        final String myNamespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));

        assertNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace));

        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, 100);
        assertEquals(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace).intValue(), 100);
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        assertNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace));

        admin.namespaces().setMaxSubscriptionsPerTopicAsync(myNamespace, 200).get();
        assertEquals(admin.namespaces().getMaxSubscriptionsPerTopicAsync(myNamespace).get().intValue(),
                200);
        admin.namespaces().removeMaxSubscriptionsPerTopicAsync(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getMaxSubscriptionsPerTopicAsync(myNamespace).get()));

        try {
            admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, -100);
            fail("should fail");
        } catch (PulsarAdminException ignore) {
        }
    }

    @Test(timeOut = 60000)
    public void testSetNamespaceEntryFilters() throws Exception {
        restartClusterAfterTest();
        restartClusterIfReused();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(conf);

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        final EntryFilterProvider oldEntryFilterProvider = pulsar.getBrokerService().getEntryFilterProvider();
        FieldUtils.writeField(pulsar.getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);

        try {
            EntryFilters entryFilters = new EntryFilters("test");

            final String myNamespace = newUniqueName(defaultTenant + "/ns");
            admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
            final String topicName = myNamespace + "/topic";
            admin.topics().createNonPartitionedTopic(topicName);

            assertNull(admin.namespaces().getNamespaceEntryFilters(myNamespace));
            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(topicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 0);

            admin.namespaces().setNamespaceEntryFilters(myNamespace, entryFilters);
            assertEquals(admin.namespaces().getNamespaceEntryFilters(myNamespace), entryFilters);
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(topicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "test");
            });

            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(topicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 1);
            admin.namespaces().removeNamespaceEntryFilters(myNamespace);
            assertNull(admin.namespaces().getNamespaceEntryFilters(myNamespace));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(topicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "");
            });

            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(topicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 0);
        } finally {
            FieldUtils.writeField(pulsar.getBrokerService(),
                    "entryFilterProvider", oldEntryFilterProvider, true);
        }
    }

    @Test(dataProvider = "topicType")
    public void testSetTopicLevelEntryFilters(String topicType) throws Exception {
        restartClusterAfterTest();
        restartClusterIfReused();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(conf);

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        final EntryFilterProvider oldEntryFilterProvider = pulsar.getBrokerService().getEntryFilterProvider();
        FieldUtils.writeField(pulsar.getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);
        try {
            EntryFilters entryFilters = new EntryFilters("test");
            final String topic = topicType + "://" + defaultNamespace + "/test-schema-validation-enforced";
            admin.topics().createPartitionedTopic(topic, 1);
            final String fullTopicName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 0;
            @Cleanup
            Producer<byte[]> producer1 = pulsarClient.newProducer()
                    .topic(fullTopicName)
                    .create();
            assertNull(admin.topicPolicies().getEntryFiltersPerTopic(topic, false));
            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(fullTopicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 0);
            admin.topicPolicies().setEntryFiltersPerTopic(topic, entryFilters);
            Awaitility.await().untilAsserted(() -> assertEquals(admin.topicPolicies().getEntryFiltersPerTopic(topic,
                    false), entryFilters));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(fullTopicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "test");
            });
            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(fullTopicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 1);
            admin.topicPolicies().removeEntryFiltersPerTopic(topic);
            assertNull(admin.topicPolicies().getEntryFiltersPerTopic(topic, false));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(fullTopicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "");
            });
            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(fullTopicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 0);
        } finally {
            FieldUtils.writeField(pulsar.getBrokerService(),
                    "entryFilterProvider", oldEntryFilterProvider, true);
        }
    }

    @Test(timeOut = 60000)
    public void testSetEntryFiltersHierarchy() throws Exception {
        restartClusterAfterTest();
        restartClusterIfReused();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(conf);
        testEntryFilterProvider.setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ), new EntryFilterDefinition(
                        "test1",
                        null,
                        EntryFilter2Test.class.getName()
                ));
        final EntryFilterProvider oldEntryFilterProvider = pulsar.getBrokerService().getEntryFilterProvider();
        FieldUtils.writeField(pulsar.getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);
        conf.setEntryFilterNames(List.of("test", "test1"));
        conf.setAllowOverrideEntryFilters(true);
        try {

            final String topic = "persistent://" + defaultNamespace + "/test-schema-validation-enforced";
            admin.topics().createPartitionedTopic(topic, 1);
            final String fullTopicName = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 0;
            @Cleanup
            Producer<byte[]> producer1 = pulsarClient.newProducer()
                    .topic(fullTopicName)
                    .create();
            assertNull(admin.topicPolicies().getEntryFiltersPerTopic(topic, false));
            Awaitility.await().untilAsserted(() ->
                    assertEquals(admin.topicPolicies().getEntryFiltersPerTopic(topic, true),
                            new EntryFilters("test,test1")));
            assertEquals(pulsar
                    .getBrokerService()
                    .getTopic(fullTopicName, false)
                    .get()
                    .get()
                    .getEntryFilters()
                    .size(), 2);

            EntryFilters nsEntryFilters = new EntryFilters("test");
            admin.namespaces().setNamespaceEntryFilters(defaultNamespace, nsEntryFilters);
            assertEquals(admin.namespaces().getNamespaceEntryFilters(defaultNamespace), nsEntryFilters);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(admin.topicPolicies().getEntryFiltersPerTopic(topic, true),
                            new EntryFilters("test")));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(fullTopicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "test");
            });

            Awaitility.await().untilAsserted(() -> {
                final List<EntryFilter> entryFilters = pulsar
                        .getBrokerService()
                        .getTopic(fullTopicName, false)
                        .get()
                        .get()
                        .getEntryFilters();
                assertEquals(entryFilters.size(), 1);
                assertEquals(((EntryFilterWithClassLoader) entryFilters.get(0))
                        .getEntryFilter().getClass(), EntryFilterTest.class);

            });


            EntryFilters topicEntryFilters = new EntryFilters("test1");
            admin.topicPolicies().setEntryFiltersPerTopic(topic, topicEntryFilters);
            Awaitility.await().untilAsserted(() -> assertEquals(admin.topicPolicies().getEntryFiltersPerTopic(topic,
                    false), topicEntryFilters));
            Awaitility.await().untilAsserted(() ->
                    assertEquals(admin.topicPolicies().getEntryFiltersPerTopic(topic, true),
                            new EntryFilters("test1")));
            Awaitility.await().untilAsserted(() -> {
                assertEquals(pulsar
                        .getBrokerService()
                        .getTopic(fullTopicName, false)
                        .get()
                        .get()
                        .getEntryFiltersPolicy()
                        .getEntryFilterNames(), "test1");
            });
            final List<EntryFilter> entryFilters = pulsar
                    .getBrokerService()
                    .getTopic(fullTopicName, false)
                    .get()
                    .get()
                    .getEntryFilters();
            assertEquals(entryFilters.size(), 1);
            assertEquals(((EntryFilterWithClassLoader) entryFilters.get(0))
                    .getEntryFilter().getClass(), EntryFilter2Test.class);

        } finally {
            FieldUtils.writeField(pulsar.getBrokerService(),
                    "entryFilterProvider", oldEntryFilterProvider, true);
        }
    }

    @Test(timeOut = 60000)
    public void testValidateNamespaceEntryFilters() throws Exception {
        restartClusterAfterTest();
        restartClusterIfReused();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(conf);

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        final EntryFilterProvider oldEntryFilterProvider = pulsar.getBrokerService().getEntryFilterProvider();
        FieldUtils.writeField(pulsar.getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);

        try {
            final String myNamespace = newUniqueName(defaultTenant + "/ns");
            admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
            try {
                admin.namespaces().setNamespaceEntryFilters(myNamespace, new EntryFilters("notexists"));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "Entry filter 'notexists' not found");
            }
            try {
                admin.namespaces().setNamespaceEntryFilters(myNamespace, new EntryFilters(""));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "entryFilterNames can't be empty. "
                        + "To remove entry filters use the remove method.");
            }
            try {
                admin.namespaces().setNamespaceEntryFilters(myNamespace, new EntryFilters(","));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "entryFilterNames can't be empty. "
                        + "To remove entry filters use the remove method.");
            }
            try {
                admin.namespaces().setNamespaceEntryFilters(myNamespace, new EntryFilters("test,notexists"));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "Entry filter 'notexists' not found");
            }
            assertNull(admin.namespaces().getNamespaceEntryFilters(myNamespace));
        } finally {
            FieldUtils.writeField(pulsar.getBrokerService(),
                    "entryFilterProvider", oldEntryFilterProvider, true);
        }
    }

    @Test(timeOut = 60000)
    public void testValidateTopicEntryFilters() throws Exception {
        restartClusterAfterTest();
        restartClusterIfReused();
        @Cleanup
        final MockEntryFilterProvider testEntryFilterProvider =
                new MockEntryFilterProvider(conf);

        testEntryFilterProvider
                .setMockEntryFilters(new EntryFilterDefinition(
                        "test",
                        null,
                        EntryFilterTest.class.getName()
                ));
        final EntryFilterProvider oldEntryFilterProvider = pulsar.getBrokerService().getEntryFilterProvider();
        FieldUtils.writeField(pulsar.getBrokerService(),
                "entryFilterProvider", testEntryFilterProvider, true);

        try {
            final String myNamespace = newUniqueName(defaultTenant + "/ns");
            admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
            final String topicName = myNamespace + "/topic";
            admin.topics().createNonPartitionedTopic(topicName);
            @Cleanup
            Producer<byte[]> producer1 = pulsarClient.newProducer()
                    .topic(topicName)
                    .create();
            try {
                admin.topicPolicies().setEntryFiltersPerTopic(topicName, new EntryFilters("notexists"));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "Entry filter 'notexists' not found");
            }
            try {
                admin.topicPolicies().setEntryFiltersPerTopic(topicName, new EntryFilters(""));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "entryFilterNames can't be empty. "
                        + "To remove entry filters use the remove method.");
            }
            try {
                admin.topicPolicies().setEntryFiltersPerTopic(topicName, new EntryFilters(","));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "entryFilterNames can't be empty. "
                        + "To remove entry filters use the remove method.");
            }
            try {
                admin.topicPolicies().setEntryFiltersPerTopic(topicName, new EntryFilters("test,notexists"));
                fail();
            } catch (PulsarAdminException e) {
                assertEquals(e.getStatusCode(), 400);
                assertEquals(e.getMessage(), "Entry filter 'notexists' not found");
            }
            assertNull(admin.topicPolicies().getEntryFiltersPerTopic(topicName, false));
        } finally {
            FieldUtils.writeField(pulsar.getBrokerService(),
                    "entryFilterProvider", oldEntryFilterProvider, true);
        }
    }

    @Test(timeOut = 30000)
    public void testMaxSubPerTopic() throws Exception {
        restartClusterAfterTest();
        pulsar.getConfiguration().setMaxSubscriptionsPerTopic(0);
        final String myNamespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxSubPerTopic";
        pulsarClient.newProducer().topic(topic).create().close();
        final int maxSub = 2;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, maxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().get();
        Awaitility.await().until(() ->
                persistentTopic.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().get() == maxSub);

        List<Consumer<?>> consumerList = new ArrayList<>(maxSub);
        for (int i = 0; i < maxSub; i++) {
            Consumer<?> consumer =
                    pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            consumerList.add(consumer);
        }
        //Create a client that can fail quickly
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()){
            @Cleanup
            Consumer<byte[]> subscribe =
                    client.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            fail("should fail");
        } catch (Exception ignore) {
        }
        //After removing the restriction, it should be able to create normally
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() ->
                persistentTopic.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().get() == 0);
        Consumer<?> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString())
                .subscribe();
        consumerList.add(consumer);

        for (Consumer<?> c : consumerList) {
            c.close();
        }
    }

    @Test(timeOut = 30000)
    public void testMaxSubPerTopicPriority() throws Exception {
        restartClusterAfterTest();
        final int brokerLevelMaxSub = 2;
        cleanup();
        conf.setMaxSubscriptionsPerTopic(brokerLevelMaxSub);
        setup();

        final String myNamespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxSubPerTopic";
        //Create a client that can fail quickly
        @Cleanup
        PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build();
        //We can only create 2 consumers
        List<Consumer<?>> consumerList = new ArrayList<>(brokerLevelMaxSub);
        for (int i = 0; i < brokerLevelMaxSub; i++) {
            Consumer<?> consumer =
                    pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            consumerList.add(consumer);
        }
        try {
            @Cleanup
            Consumer<byte[]> subscribe =
                    client.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            fail("should fail");
        } catch (Exception ignore) {

        }
        //Set namespace-level policy,the limit should up to 4
        final int nsLevelMaxSub = 4;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, nsLevelMaxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().get();
        Awaitility.await().until(() -> persistentTopic.getHierarchyTopicPolicies()
                .getMaxSubscriptionsPerTopic().get() == nsLevelMaxSub);
        Consumer<?> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(UUID.randomUUID().toString())
                .subscribe();
        consumerList.add(consumer);
        assertEquals(consumerList.size(), 3);
        //After removing the restriction, it should fail again
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() -> persistentTopic.getHierarchyTopicPolicies()
                .getMaxSubscriptionsPerTopic().get() == brokerLevelMaxSub);
        try {
            @Cleanup
            Consumer<byte[]> subscribe =
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
        restartClusterAfterTest();
        final int maxProducersPerTopic = 1;
        cleanup();
        conf.setMaxProducersPerTopic(maxProducersPerTopic);
        setup();
        //init namespace
        final String myNamespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxProducersPerTopicUnlimited";
        admin.topics().createNonPartitionedTopic(topic);
        AtomicInteger schemaOpsCounter = injectSchemaCheckCounterForTopic(topic);
        //the policy is set to 0, so there will be no restrictions
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 0);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == 0);
        List<Producer<String>> producers = new ArrayList<>();
        for (int i = 0; i < maxProducersPerTopic + 1; i++) {
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
            producers.add(producer);
        }
        assertEquals(schemaOpsCounter.get(), maxProducersPerTopic + 1);

        admin.namespaces().removeMaxProducersPerTopic(myNamespace);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == null);

        try {
            @Cleanup
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
            fail("should fail");
        } catch (PulsarClientException e) {
            String expectMsg = "Topic '" + topic + "' reached max producers limit";
            assertTrue(e.getMessage().contains(expectMsg));
            assertEquals(schemaOpsCounter.get(), maxProducersPerTopic + 1);
        }
        //set the limit to 3
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxProducersPerTopic(myNamespace) == 3);
        // should success
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producers.add(producer);
        assertEquals(schemaOpsCounter.get(), maxProducersPerTopic + 2);
        try {
            @Cleanup
            Producer<byte[]> producer1 = pulsarClient.newProducer().topic(topic).create();
            fail("should fail");
        } catch (PulsarClientException e) {
            String expectMsg = "Topic '" + topic + "' reached max producers limit";
            assertTrue(e.getMessage().contains(expectMsg));
            assertEquals(schemaOpsCounter.get(), maxProducersPerTopic + 2);
        }

        //clean up
        for (Producer<String> tempProducer : producers) {
            tempProducer.close();
        }
    }

    private AtomicInteger injectSchemaCheckCounterForTopic(String topicName) {
        final var topics = pulsar.getBrokerService().getTopics();
        AbstractTopic topic = (AbstractTopic) topics.get(topicName).join().get();
        AbstractTopic spyTopic = Mockito.spy(topic);
        AtomicInteger counter = new AtomicInteger();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.incrementAndGet();
                return invocation.callRealMethod();
            }
        }).when(spyTopic).addSchema(any(SchemaData.class));
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.incrementAndGet();
                return invocation.callRealMethod();
            }
        }).when(spyTopic).addSchemaIfIdleOrCheckCompatible(any(SchemaData.class));
        topics.put(topicName, CompletableFuture.completedFuture(Optional.of(spyTopic)));
        return counter;
    }

    @Test
    public void testMaxConsumersPerTopicUnlimited() throws Exception {
        restartClusterAfterTest();
        final int maxConsumersPerTopic = 1;
        cleanup();
        conf.setMaxConsumersPerTopic(maxConsumersPerTopic);
        setup();
        //init namespace
        final String myNamespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(myNamespace, Set.of("test"));
        final String topic = "persistent://" + myNamespace + "/testMaxConsumersPerTopicUnlimited";
        admin.topics().createNonPartitionedTopic(topic);
        AtomicInteger schemaOpsCounter = injectSchemaCheckCounterForTopic(topic);

        assertNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace));
        //the policy is set to 0, so there will be no restrictions
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 0);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 0);
        List<Consumer<String>> consumers = new ArrayList<>();
        for (int i = 0; i < maxConsumersPerTopic + 1; i++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            consumers.add(consumer);
        }
        assertEquals(schemaOpsCounter.get(), maxConsumersPerTopic + 2);

        admin.namespaces().removeMaxConsumersPerTopic(myNamespace);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == null);
        try {
            @Cleanup
            Consumer<String> subscribe = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max consumers limit"));
            assertEquals(schemaOpsCounter.get(), maxConsumersPerTopic + 2);
        }
        //set the limit to 3
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 3);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxConsumersPerTopic(myNamespace) == 3);
        // should success
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
        consumers.add(consumer);
        assertEquals(schemaOpsCounter.get(), maxConsumersPerTopic + 3);
        try {
            @Cleanup
            Consumer<String> subscribe = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Topic reached max consumers limit"));
            assertEquals(schemaOpsCounter.get(), maxConsumersPerTopic + 3);
        }

        //clean up
        for (Consumer<String> subConsumer : consumers) {
            subConsumer.close();
        }
    }

    @Test
    public void testClearBacklogForTheSubscriptionThatNoConsumers() throws Exception {
        final String topic = "persistent://" + defaultNamespace + "/clear_backlog_no_consumers"
                + UUID.randomUUID().toString();
        final String sub = "my-sub";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, sub, MessageId.earliest);
        admin.topics().skipAllMessages(topic, sub);
    }

    @Test(timeOut = 200000)
    public void testCompactionApi() throws Exception {
        final String namespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(namespace, Set.of("test"));


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
        restartClusterAfterTest();
        cleanup();
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(10000);
        setup();
        final String namespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        final String topic = "persistent://" + namespace + "/topic" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        TopicName topicName = TopicName.get(topic);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().get();
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
        final String namespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(namespace, Set.of("test"));
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
        final String namespace = defaultTenant + "/ns11";
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
        final String namespace = newUniqueName(defaultTenant + "/ns");
        admin.namespaces().createNamespace(namespace, Set.of("test"));

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
        restartClusterAfterTest();
        String tenant = defaultTenant;
        String cluster = "test";
        String namespace = tenant + "/" + (isV1 ? cluster + "/" : "") + "n1" + isV1;
        String topic = "persistent://" + namespace + "/t1" + isV1;
        admin.namespaces().createNamespace(namespace, Set.of(cluster));
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
        final String partitionedTopicName = "persistent://" + defaultNamespace + "/" + topicName;

        URL pulsarUrl = new URL(pulsar.getWebServiceAddress());

        admin.topics().createPartitionedTopic(partitionedTopicName, startPartitions);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl.toString()).build();
        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName(subName1)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        consumer1.close();

        // validate partition topic is created
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, startPartitions);

        // create a subscription for few new partition which can fail
        try {
            admin.topics().createSubscription(partitionedTopicName + "-partition-" + startPartitions, subName1,
                    MessageId.earliest);
            fail("Unexpected behaviour");
        } catch (PulsarAdminException.PreconditionFailedException ex) {
            // OK
        }

        admin.topics().updatePartitionedTopic(partitionedTopicName, newPartitions, false, true);
        // validate subscription is created for new partition.
        for (int i = startPartitions; i < newPartitions; i++) {
            assertNotNull(
                    admin.topics().getStats(partitionedTopicName + "-partition-" + i).getSubscriptions().get(subName1));
        }

        // validate update partition is success
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, newPartitions);
    }

    /**
     * Validate retring failed partitioned topic should succeed.
     * @throws Exception
     */
    @Test
    public void testTopicStatsWithEarliestTimeInBacklogIfNoBacklog() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final String subscriptionName = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);

        // Send one message.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false)
                .create();
        MessageIdImpl messageId = (MessageIdImpl) producer.send("123");
        // Catch up.
        admin.topics().skipAllMessages(topicName, subscriptionName);
        // Get topic stats with earliestTimeInBacklog
        TopicStats topicStats = admin.topics().getStats(topicName, false, false, true);
        assertEquals(topicStats.getSubscriptions().get(subscriptionName).getEarliestMsgPublishTimeInBacklog(), -1L);

        // cleanup.
        producer.close();
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "topicType")
    public void testPartitionedStatsAggregationByProducerName(String topicType) throws Exception {
        restartClusterIfReused();
        conf.setAggregatePublisherStatsByProducerName(true);
        final String topic = topicType + "://" + defaultNamespace
                + "/test-partitioned-stats-aggregation-by-producer-name";
        admin.topics().createPartitionedTopic(topic, 10);

        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return msg.hasKey() ? Integer.parseInt(msg.getKey()) : 0;
                    }
                })
                .accessMode(ProducerAccessMode.Shared)
                .create();

        @Cleanup
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topic)
                .enableLazyStartPartitionedProducers(true)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(new MessageRouter() {
                    @Override
                    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                        return msg.hasKey() ? Integer.parseInt(msg.getKey()) : 5;
                    }
                })
                .accessMode(ProducerAccessMode.Shared)
                .create();

        for (int i = 0; i < 10; i++) {
            producer1.newMessage()
                    .key(String.valueOf(i % 5))
                    .value(("message".getBytes(StandardCharsets.UTF_8)))
                    .send();
            producer2.newMessage()
                    .key(String.valueOf(i % 5 + 5))
                    .value(("message".getBytes(StandardCharsets.UTF_8)))
                    .send();
        }

        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(topic, true);
        assertEquals(topicStats.getPartitions().size(), 10);
        assertEquals(topicStats.getPartitions().values().stream().mapToInt(e -> e.getPublishers().size()).sum(), 10);
        assertEquals(topicStats.getPartitions().values().stream().map(e -> e.getPublishers()
                .get(0).getProducerName()).distinct().count(), 2);
        assertEquals(topicStats.getPublishers().size(), 2);
        topicStats.getPublishers().forEach(p -> assertTrue(p.isSupportsPartialProducer()));
    }

    @Test(dataProvider = "topicType")
    public void testPartitionedStatsAggregationByProducerNamePerPartition(String topicType) throws Exception {
        restartClusterIfReused();
        conf.setAggregatePublisherStatsByProducerName(true);
        final String topic = topicType + "://" + defaultNamespace
                + "/test-partitioned-stats-aggregation-by-producer-name-per-pt";
        admin.topics().createPartitionedTopic(topic, 2);

        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 0)
                .create();

        @Cleanup
        Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 1)
                .create();

        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(topic, true);
        assertEquals(topicStats.getPartitions().size(), 2);
        assertEquals(topicStats.getPartitions().values().stream().mapToInt(e -> e.getPublishers().size()).sum(), 2);
        assertEquals(topicStats.getPartitions().values().stream().map(e -> e.getPublishers()
                .get(0).getProducerName()).distinct().count(), 2);
        assertEquals(topicStats.getPublishers().size(), 2);
        topicStats.getPublishers().forEach(p -> assertTrue(p.isSupportsPartialProducer()));
    }

    @Test(dataProvider = "topicType")
    public void testSchemaValidationEnforced(String topicType) throws Exception {
        final String topic = topicType + "://" + defaultNamespace + "/test-schema-validation-enforced";
        admin.topics().createPartitionedTopic(topic, 1);
        @Cleanup
        Producer<byte[]> producer1 = pulsarClient.newProducer()
                .topic(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 0)
                .create();
        boolean schemaValidationEnforced = admin.topics().getSchemaValidationEnforced(topic, false);
        assertEquals(schemaValidationEnforced, false);
        admin.topics().setSchemaValidationEnforced(topic, true);
        Awaitility.await().untilAsserted(() ->
            assertEquals(admin.topics().getSchemaValidationEnforced(topic, false), true)
        );
    }

    @Test
    public void testGetNamespaceTopicList() throws Exception {
        final String persistentTopic = "persistent://" + defaultNamespace + "/testGetNamespaceTopicList";
        final String nonPersistentTopic = "non-persistent://" + defaultNamespace + "/non-testGetNamespaceTopicList";
        final String eventTopic = "persistent://" + defaultNamespace + "/__change_events";
        admin.topics().createNonPartitionedTopic(persistentTopic);
        Awaitility.await().untilAsserted(() ->
                admin.namespaces().getTopics(defaultNamespace,
                ListNamespaceTopicsOptions.builder().mode(Mode.PERSISTENT).includeSystemTopic(true).build())
                        .contains(eventTopic));
        List<String> notIncludeSystemTopics = admin.namespaces().getTopics(defaultNamespace,
                ListNamespaceTopicsOptions.builder().includeSystemTopic(false).build());
        Assert.assertFalse(notIncludeSystemTopics.contains(eventTopic));
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(nonPersistentTopic)
                .create();
        List<String> notPersistentTopics = admin.namespaces().getTopics(defaultNamespace,
                ListNamespaceTopicsOptions.builder().mode(Mode.NON_PERSISTENT).build());
        Assert.assertTrue(notPersistentTopics.contains(nonPersistentTopic));
    }

    @Test
    private void testTerminateSystemTopic() throws Exception {
        final String topic = "persistent://" + defaultNamespace + "/testTerminateSystemTopic";
        admin.topics().createNonPartitionedTopic(topic);
        final String eventTopic = "persistent://" + defaultNamespace + "/__change_events";
        admin.topicPolicies().setMaxConsumers(topic, 2);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topicPolicies().getMaxConsumers(topic), Integer.valueOf(2));
        });
        PulsarAdminException ex = expectThrows(PulsarAdminException.class,
                () -> admin.topics().terminateTopic(eventTopic));
        assertTrue(ex instanceof PulsarAdminException.NotAllowedException);
    }

    @Test
    private void testDeleteNamespaceForciblyWithManyTopics() throws Exception {
        final String ns = defaultTenant + "/ns-testDeleteNamespaceForciblyWithManyTopics";
        admin.namespaces().createNamespace(ns, 2);
        for (int i = 0; i < 100; i++) {
            admin.topics().createPartitionedTopic(String.format("persistent://%s", ns + "/topic" + i), 3);
        }
        admin.namespaces().deleteNamespace(ns, true);
        Assert.assertFalse(admin.namespaces().getNamespaces(defaultTenant).contains(ns));
    }

    @Test
    private void testSetBacklogQuotasNamespaceLevelIfRetentionExists() throws Exception {
        final String ns = defaultTenant + "/ns-testSetBacklogQuotasNamespaceLevel";
        final long backlogQuotaLimitSize = 100000002;
        final int backlogQuotaLimitTime = 2;
        admin.namespaces().createNamespace(ns, 2);
        // create retention.
        admin.namespaces().setRetention(ns, new RetentionPolicies(1800, 10000));
        // set backlog quota.
        admin.namespaces().setBacklogQuota(ns, BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                .limitSize(backlogQuotaLimitSize).limitTime(backlogQuotaLimitTime).build());
        // Verify result.
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> map = admin.namespaces().getBacklogQuotaMap(ns);
        assertEquals(map.size(), 1);
        assertTrue(map.containsKey(BacklogQuota.BacklogQuotaType.destination_storage));
        BacklogQuota backlogQuota = map.get(BacklogQuota.BacklogQuotaType.destination_storage);
        assertEquals(backlogQuota.getLimitSize(), backlogQuotaLimitSize);
        assertEquals(backlogQuota.getLimitTime(), backlogQuotaLimitTime);
        assertEquals(backlogQuota.getPolicy(), BacklogQuota.RetentionPolicy.producer_request_hold);
        // cleanup.
        admin.namespaces().deleteNamespace(ns);
    }

    @Test
    private void testAnalyzeSubscriptionBacklogNotCauseStuck() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topic);
        // Send 10 messages.
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription)
                .receiverQueueSize(0).subscribe();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send(i + "");
        }

        // Verify consumer can receive all messages after calling "analyzeSubscriptionBacklog".
        admin.topics().analyzeSubscriptionBacklog(topic, subscription, Optional.of(MessageIdImpl.earliest));
        for (int i = 0; i < 10; i++) {
            Awaitility.await().untilAsserted(() -> {
                Message m = consumer.receive();
                assertNotNull(m);
                consumer.acknowledge(m);
            });
        }

        // cleanup.
        consumer.close();
        producer.close();
        admin.topics().delete(topic);
    }

    @Test
    public void testGetStatsIfPartitionNotExists() throws Exception {
        // create topic.
        final String partitionedTp = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp");
        admin.topics().createPartitionedTopic(partitionedTp, 1);
        TopicName partition0 = TopicName.get(partitionedTp).getPartition(0);
        boolean topicExists1 = pulsar.getBrokerService().getTopic(partition0.toString(), false).join().isPresent();
        assertTrue(topicExists1);
        // Verify topics-stats works.
        TopicStats topicStats = admin.topics().getStats(partition0.toString());
        assertNotNull(topicStats);

        // Delete partition and call topic-stats again.
        admin.topics().delete(partition0.toString());
        boolean topicExists2 = pulsar.getBrokerService().getTopic(partition0.toString(), false).join().isPresent();
        assertFalse(topicExists2);
        // Verify: respond 404.
        try {
            admin.topics().getStats(partition0.toString());
            fail("Should respond 404 after the partition was deleted");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Topic partitions were not yet created"));
        }

        // cleanup.
        admin.topics().deletePartitionedTopic(partitionedTp);
    }

    private NamespaceIsolationData createPolicyData(NamespaceIsolationPolicyUnloadScope scope, List<String> namespaces,
                                                    List<String> primaryBrokers
    ) {
        // setup ns-isolation-policy in both the clusters.
        Map<String, String> parameters1 = new HashMap<>();
        parameters1.put("min_limit", "1");
        parameters1.put("usage_threshold", "100");
        List<String> nsRegexList = new ArrayList<>(namespaces);

        NamespaceIsolationData.Builder build = NamespaceIsolationData.builder()
                // "prop-ig/ns1" is present in test cluster, policy set on test2 should work
                .namespaces(nsRegexList)
                .primary(primaryBrokers)
                .secondary(Collections.singletonList(""))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters1)
                        .build());
        if (scope != null) {
            build.unloadScope(scope);
        }
        return build.build();
    }

    private boolean allTopicsUnloaded(List<String> topics) {
        for (String topic : topics) {
            if (pulsar.getBrokerService().getTopicReference(topic).isPresent()) {
                return false;
            }
        }
        return true;
    }

    private void loadTopics(List<String> topics) throws PulsarClientException,
            ExecutionException, InterruptedException {
        // create a topic by creating a producer so that the topic is present on the broker
        for (String topic : topics) {
            Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
            producer.close();
            pulsar.getBrokerService().getTopicIfExists(topic).get();
        }

        // All namespaces are loaded onto broker. Assert that
        for (String topic : topics) {
            assertTrue(pulsar.getBrokerService().getTopicReference(topic).isPresent());
        }
    }

    /**
     * Validates that the namespace isolation policy set and update is unloading only the relevant namespaces based on
     * the unload scope provided.
     *
     * @param topicType persistent or non persistent.
     * @param policyName policy name.
     * @param nsPrefix unique namespace prefix.
     * @param totalNamespaces total namespaces to create. Only the end part. Each namespace also gets a topic t1.
     * @param initialScope unload scope while creating the policy.
     * @param initialNamespaceRegex namespace regex while creating the policy.
     * @param initialLoadedNS expected namespaces to be still loaded after the policy create call. Remaining namespaces
     *                        will be asserted to be unloaded within 20 seconds.
     * @param updatedScope unload scope while updating the policy.
     * @param updatedNamespaceRegex namespace regex while updating the policy.
     * @param updatedLoadedNS expected namespaces to be loaded after policy update call. Remaining namespaces will be
     *                        asserted to be unloaded within 20 seconds.
     * @throws PulsarAdminException
     * @throws PulsarClientException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void testIsolationPolicyUnloadsNSWithScope(String topicType, String policyName, String nsPrefix,
                                                       List<String> totalNamespaces,
                                                       NamespaceIsolationPolicyUnloadScope initialScope,
                                                       List<String> initialNamespaceRegex, List<String> initialLoadedNS,
                                                       NamespaceIsolationPolicyUnloadScope updatedScope,
                                                       List<String> updatedNamespaceRegex, List<String> updatedLoadedNS,
                                                       List<String> updatedBrokerRegex)
            throws PulsarAdminException, PulsarClientException, ExecutionException, InterruptedException {

        // Create all namespaces
        List<String> allTopics = new ArrayList<>();
        for (String namespacePart: totalNamespaces) {
            admin.namespaces().createNamespace(nsPrefix + namespacePart, Set.of("test"));
            allTopics.add(topicType + "://" + nsPrefix + namespacePart + "/t1");
        }
        // Load all topics so that they are present. Assume topic t1 under each namespace
        loadTopics(allTopics);

        // Create the policy
        NamespaceIsolationData nsPolicyData1 = createPolicyData(
                initialScope, initialNamespaceRegex, Collections.singletonList(".*")
        );
        admin.clusters().createNamespaceIsolationPolicy("test", policyName, nsPolicyData1);

        List<String> initialLoadedTopics = new ArrayList<>();
        for (String namespacePart: initialLoadedNS) {
            initialLoadedTopics.add(topicType + "://" + nsPrefix + namespacePart + "/t1");
        }

        List<String> initialUnloadedTopics = new ArrayList<>(allTopics);
        initialUnloadedTopics.removeAll(initialLoadedTopics);

        // Assert that all topics (and thus ns) not under initialLoadedNS namespaces are unloaded
        if (initialUnloadedTopics.isEmpty()) {
            // Just wait a bit to ensure we don't miss lazy unloading of topics we expect not to unload
            TimeUnit.SECONDS.sleep(5);
        } else {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> allTopicsUnloaded(initialUnloadedTopics));
        }
        // Assert that all topics under initialLoadedNS are still present
        initialLoadedTopics.forEach(t -> assertTrue(pulsar.getBrokerService().getTopicReference(t).isPresent()));

        // Load the topics again
        loadTopics(allTopics);

        // Update policy using updatedScope with updated namespace regex
        nsPolicyData1 = createPolicyData(updatedScope, updatedNamespaceRegex, updatedBrokerRegex);
        admin.clusters().updateNamespaceIsolationPolicy("test", policyName, nsPolicyData1);

        List<String> updatedLoadedTopics = new ArrayList<>();
        for (String namespacePart : updatedLoadedNS) {
            updatedLoadedTopics.add(topicType + "://" + nsPrefix + namespacePart + "/t1");
        }

        List<String> updatedUnloadedTopics = new ArrayList<>(allTopics);
        updatedUnloadedTopics.removeAll(updatedLoadedTopics);

        // Assert that all topics (and thus ns) not under updatedLoadedNS namespaces are unloaded
        if (updatedUnloadedTopics.isEmpty()) {
            // Just wait a bit to ensure we don't miss lazy unloading of topics we expect not to unload
            TimeUnit.SECONDS.sleep(5);
        } else {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> allTopicsUnloaded(updatedUnloadedTopics));
        }
        // Assert that all topics under updatedLoadedNS are still present
        updatedLoadedTopics.forEach(t -> assertTrue(pulsar.getBrokerService().getTopicReference(t).isPresent()));

    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithAllScope(final String topicType) throws Exception {
        String nsPrefix = newUniqueName(defaultTenant + "/") + "-unload-test-";
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-all", nsPrefix, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*"), List.of("b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-c.*"), List.of("b1", "b2"),
                Collections.singletonList(".*")
        );
    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithChangedScope1(final String topicType) throws Exception {
        String nsPrefix1 = newUniqueName(defaultTenant + "/") + "-unload-test-";
        // Addition case
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-changed1", nsPrefix1, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*"), List.of("b1", "b2", "c1"),
                changed, List.of(".*-unload-test-a.*", ".*-unload-test-c.*"), List.of("a1", "a2", "b1", "b2"),
                Collections.singletonList(".*")
        );
    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithChangedScope2(final String topicType) throws Exception {
        String nsPrefix2 = newUniqueName(defaultTenant + "/") + "-unload-test-";
        // removal case
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-changed2", nsPrefix2, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*", ".*-unload-test-c.*"), List.of("b1", "b2"),
                changed, List.of(".*-unload-test-c.*"), List.of("b1", "b2", "c1"),
                Collections.singletonList(".*")
        );
    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithScopeMissing(final String topicType) throws Exception {
        String nsPrefix = newUniqueName(defaultTenant + "/") + "-unload-test-";
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-changed", nsPrefix, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*"), List.of("b1", "b2", "c1"),
                null, List.of(".*-unload-test-a.*", ".*-unload-test-c.*"), List.of("a1", "a2", "b1", "b2"),
                Collections.singletonList(".*")
        );
    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithNoneScope(final String topicType) throws Exception {
        String nsPrefix = newUniqueName(defaultTenant + "/") + "-unload-test-";
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-none", nsPrefix, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*"), List.of("b1", "b2", "c1"),
                none, List.of(".*-unload-test-a.*", ".*-unload-test-c.*"), List.of("a1", "a2", "b1", "b2", "c1"),
                Collections.singletonList(".*")
        );
    }

    @Test(dataProvider = "topicType")
    public void testIsolationPolicyUnloadsNSWithPrimaryChanged(final String topicType) throws Exception {
        String nsPrefix = newUniqueName(defaultTenant + "/") + "-unload-test-";
        // As per changed flag, only c1 should unload, but due to primary change, both a* and c* will.
        testIsolationPolicyUnloadsNSWithScope(
                topicType, "policy-primary-changed", nsPrefix, List.of("a1", "a2", "b1", "b2", "c1"),
                all_matching, List.of(".*-unload-test-a.*"), List.of("b1", "b2", "c1"),
                changed, List.of(".*-unload-test-a.*", ".*-unload-test-c.*"), List.of("b1", "b2"),
                List.of(".*", "broker.*")
        );
    }

    @Test
    public void testGrantAndRevokePermissions() throws Exception {

        String namespace = newUniqueName(defaultTenant + "/") + "-unload-test-";
        String namespace2 = newUniqueName(defaultTenant + "/") + "-unload-test-";
        admin.namespaces().createNamespace(namespace, Set.of("test"));
        admin.namespaces().createNamespace(namespace2, Set.of("test"));
        //
        final String topic1 = "persistent://" + namespace + "/test1";
        final String topic2 = "persistent://" + namespace + "/test2";
        final String topic3 = "non-persistent://" + namespace + "/test3";
        final String topic4 = "persistent://" + namespace2 + "/test4";

        admin.topics().createPartitionedTopic(topic1, 3);
        admin.topics().createPartitionedTopic(topic2, 3);
        admin.topics().createPartitionedTopic(topic3, 3);
        admin.topics().createPartitionedTopic(topic4, 3);
        pulsarClient.newProducer().topic(topic1).create().close();
        pulsarClient.newProducer().topic(topic2).create().close();
        pulsarClient.newProducer().topic(topic3).create().close();
        pulsarClient.newProducer().topic(topic4).create().close();

        List<GrantTopicPermissionOptions> grantPermissionOptions = new ArrayList<>();
        grantPermissionOptions.add(GrantTopicPermissionOptions.builder().topic(topic1).role("role1")
                .actions(Set.of(AuthAction.produce)).build());
        grantPermissionOptions.add(GrantTopicPermissionOptions.builder().topic(topic4).role("role4")
                .actions(Set.of(AuthAction.produce)).build());
        try {
            admin.namespaces().grantPermissionOnTopics(grantPermissionOptions);
            fail("Should go here, because there are two namespaces");
        } catch (Exception ex) {
            Assert.assertTrue(ex != null);
        }
        grantPermissionOptions.clear();
        grantPermissionOptions.add(GrantTopicPermissionOptions.builder().topic(topic1).role("role1")
                .actions(Set.of(AuthAction.produce)).build());
        grantPermissionOptions.add(GrantTopicPermissionOptions.builder().topic(topic2).role("role2")
                .actions(Set.of(AuthAction.consume)).build());
        grantPermissionOptions.add(GrantTopicPermissionOptions.builder().topic(topic3).role("role3")
                .actions(Set.of(AuthAction.produce, AuthAction.consume)).build());
        admin.namespaces().grantPermissionOnTopics(grantPermissionOptions);

        final Map<String, Set<AuthAction>> permissions1 = admin.topics().getPermissions(topic1);
        final Map<String, Set<AuthAction>> permissions2 = admin.topics().getPermissions(topic2);
        final Map<String, Set<AuthAction>> permissions3 = admin.topics().getPermissions(topic3);

        Assert.assertEquals(permissions1.get("role1"), Set.of(AuthAction.produce));
        Assert.assertEquals(permissions2.get("role2"), Set.of(AuthAction.consume));
        Assert.assertEquals(permissions3.get("role3"), Set.of(AuthAction.produce, AuthAction.consume));
        //
        List<RevokeTopicPermissionOptions> revokePermissionOptions = new ArrayList<>();
        revokePermissionOptions.add(RevokeTopicPermissionOptions.builder().topic(topic1).role("role1").build());
        revokePermissionOptions.add(RevokeTopicPermissionOptions.builder().topic(topic2).role("role2").build());
        admin.namespaces().revokePermissionOnTopics(revokePermissionOptions);

        final Map<String, Set<AuthAction>> permissions11 = admin.topics().getPermissions(topic1);
        final Map<String, Set<AuthAction>> permissions22 = admin.topics().getPermissions(topic2);

        Assert.assertTrue(permissions11.isEmpty());
        Assert.assertTrue(permissions22.isEmpty());
    }

    @Test
    public void testDeletePatchyPartitionedTopic() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(defaultNamespace + "/tp");
        admin.topics().createPartitionedTopic(topic, 2);
        Producer producer = pulsarClient.newProducer().topic(TopicName.get(topic).getPartition(0).toString())
                .create();
        // Mock a scenario that "-partition-1" has been removed due to topic GC.
        pulsar.getBrokerService().getTopic(TopicName.get(topic).getPartition(1).toString(), false)
                .get().get().delete().join();
        // Verify: delete partitioned topic.
        producer.close();
        admin.topics().deletePartitionedTopicAsync(topic, false).get();
    }
}
