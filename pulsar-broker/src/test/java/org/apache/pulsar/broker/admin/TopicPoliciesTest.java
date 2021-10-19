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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.ConfigHelper;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SubscribeRateLimiter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class TopicPoliciesTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    private final String persistenceTopic = "persistent://" + myNamespace + "/test-set-persistence";

    private final String topicPolicyEventsTopic = "persistent://" + myNamespace + "/__change_events";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        this.conf.setMaxMessageSizeCheckIntervalInSeconds(1);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();
        waitForZooKeeperWatchers();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        this.resetConfig();
    }

    @Test
    public void testSetSizeBasedBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetTimeBasedBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(1000)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.message_age);

        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveSizeBasedBacklogQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().removeBacklogQuota(testTopic, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage)));

        backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota {} in backlog quota manager on topic: {} after remove", backlogQuotaInManager,
                testTopic);
        Assert.assertEquals(backlogQuotaManager.getDefaultQuota(), backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveTimeBasedBacklogQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(1000)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.message_age);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().removeBacklogQuota(testTopic, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age)));

        backlogQuotaInManager = backlogQuotaManager
                .getBacklogQuota(TopicName.get(testTopic), BacklogQuota.BacklogQuotaType.message_age);
        Assert.assertEquals(backlogQuotaManager.getDefaultQuota(), backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckSizeBasedBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace), retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024 + 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024 - 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);

        BacklogQuota finalBacklogQuota = backlogQuota;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), finalBacklogQuota));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckTimeBasedBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace), retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60 + 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60 - 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);

        BacklogQuota finalBacklogQuota = backlogQuota;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), finalBacklogQuota));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 20000)
    public void testGetSizeBasedBacklogQuotaApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertEquals(admin.topics().getBacklogQuotaMap(topic), Maps.newHashMap());
        assertEquals(admin.namespaces().getBacklogQuotaMap(myNamespace), Maps.newHashMap());
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> brokerQuotaMap = ConfigHelper.backlogQuotaMap(conf);
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), brokerQuotaMap);
        BacklogQuota namespaceQuota = BacklogQuota.builder()
                .limitSize(30)
                .limitTime(10)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();

        admin.namespaces().setBacklogQuota(myNamespace, namespaceQuota);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.namespaces().getBacklogQuotaMap(myNamespace).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> namespaceQuotaMap = Maps.newHashMap();
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, namespaceQuota);
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold).build());
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), namespaceQuotaMap);

        BacklogQuota topicQuota = BacklogQuota.builder()
                .limitSize(40)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        admin.topics().setBacklogQuota(topic, topicQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topics().getBacklogQuotaMap(topic).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> topicQuotaMap = Maps.newHashMap();
        topicQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, topicQuota);
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), topicQuotaMap);

        admin.namespaces().removeBacklogQuota(myNamespace);
        admin.topics().removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.namespaces().getBacklogQuotaMap(myNamespace)
                .get(BacklogQuota.BacklogQuotaType.destination_storage) == null));
        Awaitility.await().untilAsserted(() -> assertTrue(admin.topics().getBacklogQuotaMap(topic).isEmpty()));
        assertTrue(admin.topics().getBacklogQuotaMap(topic, true)
                .get(BacklogQuota.BacklogQuotaType.destination_storage) == null);
    }

    @Test(timeOut = 20000)
    public void testGetTimeBasedBacklogQuotaApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertEquals(admin.topics().getBacklogQuotaMap(topic), Maps.newHashMap());
        assertEquals(admin.namespaces().getBacklogQuotaMap(myNamespace), Maps.newHashMap());
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> brokerQuotaMap = ConfigHelper.backlogQuotaMap(conf);
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), brokerQuotaMap);
        BacklogQuota namespaceQuota = BacklogQuota.builder()
                .limitTime(30)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();

        admin.namespaces().setBacklogQuota(myNamespace, namespaceQuota, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.namespaces().getBacklogQuotaMap(myNamespace).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> namespaceQuotaMap = Maps.newHashMap();
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, namespaceQuota);
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold).build());
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), namespaceQuotaMap);

        BacklogQuota topicQuota = BacklogQuota.builder()
                .limitTime(40)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        admin.topics().setBacklogQuota(topic, topicQuota, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topics().getBacklogQuotaMap(topic).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> topicQuotaMap = Maps.newHashMap();
        topicQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, topicQuota);
        assertEquals(admin.topics().getBacklogQuotaMap(topic, true), topicQuotaMap);

        admin.namespaces().removeBacklogQuota(myNamespace, BacklogQuota.BacklogQuotaType.message_age);
        admin.topics().removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.namespaces().getBacklogQuotaMap(myNamespace)
                .get(BacklogQuota.BacklogQuotaType.message_age) == null));
        Awaitility.await().untilAsserted(() -> assertTrue(admin.topics().getBacklogQuotaMap(topic).isEmpty()));
        assertTrue(admin.topics().getBacklogQuotaMap(topic, true)
                .get(BacklogQuota.BacklogQuotaType.message_age) == null);
    }

    @Test
    public void testCheckBacklogQuotaFailed() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace), retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }
        //Ensure that the cache has not been updated after a long time
        Awaitility.await().atLeast(1, TimeUnit.SECONDS);
        assertNull(admin.topics().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage));
    }

    @Test
    public void testCheckRetentionSizeBasedQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(10, 9);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(10, 12);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setRetention(testTopic, retention);

        RetentionPolicies finalRetention = retention;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), finalRetention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckRetentionTimeBasedQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topics().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(9, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topics().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(12, 10);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setRetention(testTopic, retention);

        RetentionPolicies finalRetention = retention;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), finalRetention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetRetention() throws Exception {
        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), retention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveRetention() throws Exception {

        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), retention));

        admin.topics().removeRetention(testTopic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getRetention(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 10000)
    public void testRetentionAppliedApi() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        RetentionPolicies brokerPolicies =
                new RetentionPolicies(conf.getDefaultRetentionTimeInMinutes(), conf.getDefaultRetentionSizeInMB());
        assertEquals(admin.topics().getRetention(topic, true), brokerPolicies);

        RetentionPolicies namespacePolicies = new RetentionPolicies(10, 20);
        admin.namespaces().setRetention(myNamespace, namespacePolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getRetention(topic, true), namespacePolicies));

        RetentionPolicies topicPolicies = new RetentionPolicies(20,30);
        admin.topics().setRetention(topic, topicPolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getRetention(topic, true), topicPolicies));

        admin.topics().removeRetention(topic);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getRetention(topic, true), namespacePolicies));

        admin.namespaces().removeRetention(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getRetention(topic, true), brokerPolicies));
    }

    @Test(timeOut = 20000)
    public void testGetSubDispatchRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getSubscriptionDispatchRate(topic));
        assertNull(admin.namespaces().getSubscriptionDispatchRate(myNamespace));

        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(conf.getDispatchThrottlingRatePerSubscriptionInMsg())
                .dispatchThrottlingRateInByte(conf.getDispatchThrottlingRatePerSubscriptionInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topics().getSubscriptionDispatchRate(topic, true), brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getSubscriptionDispatchRate(myNamespace)));
        assertEquals(admin.topics().getSubscriptionDispatchRate(topic, true), namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(12)
                .build();
        admin.topics().setSubscriptionDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getSubscriptionDispatchRate(topic)));
        assertEquals(admin.topics().getSubscriptionDispatchRate(topic, true), topicDispatchRate);

        admin.namespaces().removeSubscriptionDispatchRate(myNamespace);
        admin.topics().removeSubscriptionDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getSubscriptionDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getSubscriptionDispatchRate(topic)));
        assertEquals(admin.topics().getSubscriptionDispatchRate(topic, true), brokerDispatchRate);
    }

    @Test(timeOut = 20000)
    public void testRetentionPriority() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getRetention(topic));
        assertNull(admin.namespaces().getRetention(myNamespace));

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Method shouldTopicBeRetained = PersistentTopic.class.getDeclaredMethod("shouldTopicBeRetained");
        shouldTopicBeRetained.setAccessible(true);
        Field lastActive = PersistentTopic.class.getSuperclass().getDeclaredField("lastActive");
        lastActive.setAccessible(true);
        //set last active to 2 minutes ago
        lastActive.setLong(persistentTopic, System.nanoTime() - TimeUnit.MINUTES.toNanos(2));
        //the default value of the broker-level is 0, so it is not retained by default
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //set namespace-level policy
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        admin.namespaces().setRetention(myNamespace, retentionPolicies);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // set topic-level policy
        admin.topics().setRetention(topic, new RetentionPolicies(3, 1));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getRetention(topic)));
        assertTrue((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //topic-level disabled
        admin.topics().setRetention(topic, new RetentionPolicies(0, 0));
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getRetention(topic).getRetentionSizeInMB(), 0));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // remove topic-level policy
        admin.topics().removeRetention(topic);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getRetention(topic)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //namespace-level disabled
        admin.namespaces().setRetention(myNamespace, new RetentionPolicies(0, 0));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //change namespace-level policy
        admin.namespaces().setRetention(myNamespace, new RetentionPolicies(1, 1));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // remove namespace-level policy
        admin.namespaces().removeRetention(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getRetention(myNamespace)));
        //the default value of the broker-level is 0, so it is not retained by default
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
    }

    @Test(timeOut = 20000)
    public void testGetPersistenceApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getPersistence(topic));
        assertNull(admin.namespaces().getPersistence(myNamespace));
        PersistencePolicies brokerPolicy
                = new PersistencePolicies(pulsar.getConfiguration().getManagedLedgerDefaultEnsembleSize(),
                pulsar.getConfiguration().getManagedLedgerDefaultWriteQuorum(),
                pulsar.getConfiguration().getManagedLedgerDefaultAckQuorum(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit());
        assertEquals(admin.topics().getPersistence(topic, true), brokerPolicy);
        PersistencePolicies namespacePolicy
                = new PersistencePolicies(5,4,3,2);

        admin.namespaces().setPersistence(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getPersistence(myNamespace)));
        assertEquals(admin.topics().getPersistence(topic, true), namespacePolicy);

        PersistencePolicies topicPolicy = new PersistencePolicies(4, 3, 2, 1);
        admin.topics().setPersistence(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getPersistence(topic)));
        assertEquals(admin.topics().getPersistence(topic, true), topicPolicy);

        admin.namespaces().removePersistence(myNamespace);
        admin.topics().removePersistence(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getPersistence(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getPersistence(topic)));
        assertEquals(admin.topics().getPersistence(topic, true), brokerPolicy);
    }

    @Test
    public void testCheckPersistence() throws Exception {
        PersistencePolicies persistencePolicies = new PersistencePolicies(6, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 6, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 2, 6, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(1, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topics().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetPersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace), persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.topics().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPersistence(persistenceTopic), persistencePolicies));
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(persistenceTopic)
                .subscriptionName("test")
                .subscribe();
        admin.topics().unload(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
            assertEquals(managedLedgerConfig.getEnsembleSize(), 3);
            assertEquals(managedLedgerConfig.getWriteQuorumSize(), 3);
            assertEquals(managedLedgerConfig.getAckQuorumSize(), 3);
            assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.1);
        });

        PersistencePolicies getPersistencePolicies = admin.topics().getPersistence(persistenceTopic);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        Assert.assertEquals(getPersistencePolicies, persistencePolicies);
        consumer.close();
    }

    @Test
    public void testGetDispatchRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getDispatchRate(topic));
        assertNull(admin.namespaces().getDispatchRate(myNamespace));
        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(conf.getDispatchThrottlingRatePerTopicInMsg())
                .dispatchThrottlingRateInByte(conf.getDispatchThrottlingRatePerTopicInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topics().getDispatchRate(topic, true), brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getDispatchRate(myNamespace)));
        assertEquals(admin.topics().getDispatchRate(topic, true), namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(22)
                .build();
        admin.topics().setDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getDispatchRate(topic)));
        assertEquals(admin.topics().getDispatchRate(topic, true), topicDispatchRate);

        admin.namespaces().removeDispatchRate(myNamespace);
        admin.topics().removeDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getDispatchRate(topic)));
        assertEquals(admin.topics().getDispatchRate(topic, true), brokerDispatchRate);
    }

    @Test
    public void testRemovePersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace), persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.topics().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPersistence(persistenceTopic), persistencePolicies));

        admin.topics().removePersistence(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getPersistence(persistenceTopic)));

        admin.lookups().lookupTopic(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        assertEquals(managedLedgerConfig.getEnsembleSize(), 2);
        assertEquals(managedLedgerConfig.getWriteQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getAckQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.3);
    }

    @Test
    public void testCheckMaxProducers() throws Exception {
        int maxProducers = -1;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topics().setMaxProducers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetMaxProducerApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getMaxProducers(topic));
        assertNull(admin.namespaces().getMaxProducersPerTopic(myNamespace));
        assertEquals(admin.topics().getMaxProducers(topic, true).intValue(), conf.getMaxProducersPerTopic());

        admin.namespaces().setMaxProducersPerTopic(myNamespace, 7);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getMaxProducersPerTopic(myNamespace)));
        assertEquals(admin.topics().getMaxProducers(topic, true).intValue(), 7);

        admin.topics().setMaxProducers(topic, 1000);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getMaxProducers(topic)));
        assertEquals(admin.topics().getMaxProducers(topic, true).intValue(), 1000);

        admin.namespaces().removeMaxProducersPerTopic(myNamespace);
        admin.topics().removeMaxProducers(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getMaxProducersPerTopic(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getMaxProducers(topic)));
        assertEquals(admin.topics().getMaxProducers(topic, true).intValue(), conf.getMaxProducersPerTopic());
    }

    @Test
    public void testSetMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topics().setMaxProducers(persistenceTopic, maxProducers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxProducers(persistenceTopic), maxProducers));

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer3 = null;

        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topics().setMaxProducers(persistenceTopic, maxProducers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxProducers(persistenceTopic), maxProducers));

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer3 = null;
        Producer<byte[]> producer4 = null;

        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);

        admin.topics().removeMaxProducers(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getMaxProducers(persistenceTopic)));

        producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Assert.assertNotNull(producer3);
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getMaxProducersPerTopic(myNamespace).intValue(), 3));

        log.info("MaxProducers: {} will set to the namespace: {}", 3, myNamespace);
        try {
            producer4 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on namespace level.");
        }
        Assert.assertNull(producer4);

        producer1.close();
        producer2.close();
        producer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }


    @Test
    public void testGetSetDispatchRate() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getDispatchRate(testTopic), dispatchRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveDispatchRate() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getDispatchRate(testTopic), dispatchRate));

        admin.topics().removeDispatchRate(testTopic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getDispatchRate(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 20000)
    public void testPolicyOverwrittenByNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(20000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getDispatchRate(myNamespace).getDispatchThrottlingRateInMsg(), 200));

        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(10000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        admin.topics().setDispatchRate(topic, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNotNull(admin.topics().getDispatchRate(topic)));

        //1 Set ns level policy, topic level should not be overwritten
        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(300)
                .dispatchThrottlingRateInByte(30000)
                .ratePeriodInSecond(2)
                .relativeToPublishRate(true)
                .build();
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> {
                    DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get().getDispatchRateLimiter().get();
                    Assert.assertEquals(limiter.getDispatchRateOnByte(), 10000);
                    Assert.assertEquals(limiter.getDispatchRateOnMsg(), 100);
                });

        admin.topics().removeDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getDispatchRate(topic)));

        //2 Remove level policy ,DispatchRateLimiter should us ns level policy
        Awaitility.await()
                .untilAsserted(() -> {
                    DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get().getDispatchRateLimiter().get();
                    Assert.assertEquals(limiter.getDispatchRateOnByte(), 30000);
                    Assert.assertEquals(limiter.getDispatchRateOnMsg(), 300);
                });
    }

    @Test(timeOut = 20000)
    public void testRestart() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        // set namespace level inactive topic policies
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up,100,true);
        admin.namespaces().setInactiveTopicPolicies(myNamespace, inactiveTopicPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getInactiveTopicPolicies(myNamespace).getInactiveTopicDeleteMode(),
                        InactiveTopicDeleteMode.delete_when_subscriptions_caught_up));

        // set namespace retention policies
        final RetentionPolicies retentionPolicies = new RetentionPolicies(10, -1);
        admin.namespaces().setRetention(myNamespace, retentionPolicies);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(myNamespace),
                        retentionPolicies));

        // set topic level inactive topic policies
        inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,200,false);
        admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies);

        InactiveTopicPolicies finalInactiveTopicPolicies = inactiveTopicPolicies;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getInactiveTopicPolicies(topic),
                        finalInactiveTopicPolicies));

        // set topic level retention policies
        final RetentionPolicies finalRetentionPolicies = new RetentionPolicies(20, -1);
        admin.topics().setRetention(topic, finalRetentionPolicies);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(topic), finalRetentionPolicies));

        // restart broker, policy should still take effect
        restartBroker();

        // Trigger the cache init.
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        // check inactive topic policies and retention policies.
        Awaitility.await()
                .untilAsserted(() -> {
                    PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
                    ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
                    Assert.assertEquals(persistentTopic.getInactiveTopicPolicies(), finalInactiveTopicPolicies);
                    Assert.assertEquals(managedLedgerConfig.getRetentionSizeInMB(),
                            finalRetentionPolicies.getRetentionSizeInMB());
                    Assert.assertEquals(managedLedgerConfig.getRetentionTimeMillis(),
                            TimeUnit.MINUTES.toMillis(finalRetentionPolicies.getRetentionTimeInMinutes()));
                });

        producer.close();
    }

    @Test
    public void testGetSetSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetSubscriptionDispatchRateAfterTopicLoaded() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testRemoveSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.getDispatchThrottlingRateInMsg());

        // remove subscription dispatch rate
        admin.topics().removeSubscriptionDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getSubscriptionDispatchRate(topic)));

        Awaitility.await().untilAsserted(() -> {
            DispatchRateLimiter drl = pulsar.getBrokerService().getTopicIfExists(topic)
                    .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
            Assert.assertNotEquals(drl.getDispatchRateOnMsg(), dispatchRate.getDispatchThrottlingRateInMsg());
            Assert.assertNotEquals(drl.getDispatchRateOnByte(), dispatchRate.getDispatchThrottlingRateInByte());
        });

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testSubscriptionDispatchRatePolicyOverwrittenNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        // set namespace level subscription dispatch rate
        DispatchRate namespaceDispatchRate =                     DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();;
        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(myNamespace), namespaceDispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        // get subscription dispatch Rate limiter
        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.getDispatchThrottlingRateInMsg());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.getDispatchThrottlingRateInByte());

        // set topic level subscription dispatch rate
        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(2 * 1024 * 1024)
                .ratePeriodInSecond(1)
                .build();;
        admin.topics().setSubscriptionDispatchRate(topic, topicDispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), topicDispatchRate));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), topicDispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), topicDispatchRate.getDispatchThrottlingRateInMsg());

        // remove topic level subscription dispatch rate limiter
        admin.topics().removeSubscriptionDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getSubscriptionDispatchRate(topic)));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getCompactionThreshold(testTopic), compactionThreshold));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getCompactionThreshold(testTopic), compactionThreshold));

        admin.topics().removeCompactionThreshold(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getCompactionThreshold(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topics().removeMaxConsumersPerSubscription(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getMaxConsumersPerSubscription(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetPublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPublishRate(testTopic), publishRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemovePublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPublishRate(testTopic), publishRate));

        admin.topics().removePublishRate(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getPublishRate(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckMaxConsumers() throws Exception {
        Integer maxProducers = new Integer(-1);
        log.info("MaxConsumers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topics().setMaxConsumers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }
    @Test
    public void testGetMaxConsumersApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getMaxConsumers(topic));
        assertNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace));
        assertEquals(admin.topics().getMaxConsumers(topic, true).intValue(), conf.getMaxConsumersPerTopic());

        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 7);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace)));
        assertEquals(admin.topics().getMaxConsumers(topic, true).intValue(), 7);

        admin.topics().setMaxConsumers(topic, 1000);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getMaxConsumers(topic)));
        assertEquals(admin.topics().getMaxConsumers(topic, true).intValue(), 1000);

        admin.namespaces().removeMaxConsumersPerTopic(myNamespace);
        admin.topics().removeMaxConsumers(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getMaxConsumers(topic)));
        assertEquals(admin.topics().getMaxConsumers(topic, true).intValue(), conf.getMaxConsumersPerTopic());
    }

    @Test
    public void testSetMaxConsumers() throws Exception {
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 1);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getMaxConsumersPerTopic(myNamespace).intValue(), 1));
        log.info("MaxConsumers: {} will set to the namespace: {}", 1, myNamespace);
        Integer maxConsumers = 2;
        log.info("MaxConsumers: {} will set to the topic: {}", maxConsumers, persistenceTopic);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumers(persistenceTopic), maxConsumers));

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().subscriptionName("sub1").topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().subscriptionName("sub2").topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);
        consumer1.close();
        consumer2.close();

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumers() throws Exception {
        Integer maxConsumers = 2;
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumers(persistenceTopic), maxConsumers));

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().subscriptionName("sub1").topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().subscriptionName("sub2").topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);

        admin.topics().removeMaxConsumers(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topics().getMaxConsumers(persistenceTopic)));

        consumer3 = pulsarClient.newConsumer().subscriptionName("sub3").topic(persistenceTopic).subscribe();
        Assert.assertNotNull(consumer3);
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 3);
        log.info("MaxConsumers: {} will set to the namespace: {}", 3, myNamespace);

        Consumer<byte[]> consumer4 = null;
        try {
            consumer4 = pulsarClient.newConsumer().subscriptionName("sub4").topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit on namespace level.");
        }
        Assert.assertNull(consumer4);

        consumer1.close();
        consumer2.close();
        consumer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testDisableSubscribeRate() throws Exception {
        assertEquals(pulsar.getConfiguration().getSubscribeThrottlingRatePerConsumer(), 0);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.lookups().lookupTopic(persistenceTopic);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(persistenceTopic).get().get();
        Field field = PersistentTopic.class.getDeclaredField("subscribeRateLimiter");
        field.setAccessible(true);
        Optional<SubscribeRateLimiter> limiter = (Optional<SubscribeRateLimiter>) field.get(topic);
        // sub rate limiter should be null by default
        assertFalse(limiter.isPresent());

        // Enable / Disable subscribe rate in namespace-level
        final SubscribeRate subscribeRate = new SubscribeRate(1, 100);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter1 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertTrue(limiter1.isPresent());
        });
        admin.namespaces().removeSubscribeRate(myNamespace);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter2 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertFalse(limiter2.isPresent());
        });

        // Enable / Disable subscribe rate in topic-level
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter1 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertTrue(limiter1.isPresent());
        });
        admin.topics().removeSubscribeRate(persistenceTopic);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter2 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertFalse(limiter2.isPresent());
        });
    }

    @Test
    public void testGetSetSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);

        SubscribeRate subscribeRate1 = new SubscribeRate(1, 30);
        log.info("Subscribe Rate: {} will be set to the namespace: {}", subscribeRate1, myNamespace);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate1);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getSubscribeRate(myNamespace), subscribeRate1));

        SubscribeRate subscribeRate2 =  new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate2, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate2);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscribeRate(persistenceTopic), subscribeRate2));

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer1);
        consumer1.close();
        pulsarClient1.shutdown();

        Consumer consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer2);
        consumer2.close();
        pulsarClient2.shutdown();

        Consumer consumer3 = null;

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }

        Assert.assertNull(consumer3);
        pulsarClient3.shutdown();

        admin.topics().deletePartitionedTopic(testTopic, true);
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
    }

    @Test(timeOut = 20000)
    public void testGetSubscribeRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topics().getSubscribeRate(topic));
        assertNull(admin.namespaces().getSubscribeRate(myNamespace));
        SubscribeRate brokerPolicy = new SubscribeRate(
                pulsar.getConfiguration().getSubscribeThrottlingRatePerConsumer(),
                pulsar.getConfiguration().getSubscribeRatePeriodPerConsumerInSecond()
        );
        assertEquals(admin.topics().getSubscribeRate(topic, true), brokerPolicy);
        SubscribeRate namespacePolicy = new SubscribeRate(10, 11);

        admin.namespaces().setSubscribeRate(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getSubscribeRate(myNamespace)));
        assertEquals(admin.topics().getSubscribeRate(topic, true), namespacePolicy);

        SubscribeRate topicPolicy = new SubscribeRate(20, 21);
        admin.topics().setSubscribeRate(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topics().getSubscribeRate(topic)));
        assertEquals(admin.topics().getSubscribeRate(topic, true), topicPolicy);

        admin.namespaces().removeSubscribeRate(myNamespace);
        admin.topics().removeSubscribeRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getSubscribeRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getSubscribeRate(topic)));
        assertEquals(admin.topics().getSubscribeRate(topic, true), brokerPolicy);
    }

    @Test(timeOut = 30000)
    public void testPriorityAndDisableMaxConsumersOnSub() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        int maxConsumerInBroker = 1;
        int maxConsumerInNs = 2;
        int maxConsumerInTopic = 4;
        String mySub = "my-sub";
        conf.setMaxConsumersPerSubscription(maxConsumerInBroker);
        pulsarClient.newProducer().topic(topic).create().close();
        List<Consumer<String>> consumerList = new ArrayList<>();
        ConsumerBuilder<String> builder = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic).subscriptionName(mySub);
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }

        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, maxConsumerInNs);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace)));
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }
        //disabled
        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, 0);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.namespaces().getMaxConsumersPerSubscription(myNamespace).intValue(), 0));
        consumerList.add(builder.subscribe());
        //set topic-level
        admin.topics().setMaxConsumersPerSubscription(topic, maxConsumerInTopic);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.topics().getMaxConsumersPerSubscription(topic)));
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }
        //remove topic policies
        admin.topics().removeMaxConsumersPerSubscription(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.topics().getMaxConsumersPerSubscription(topic)));
        consumerList.add(builder.subscribe());
        //remove namespace policies, then use broker-level
        admin.namespaces().removeMaxConsumersPerSubscription(myNamespace);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace)));
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }

        for (Consumer<String> consumer : consumerList) {
            consumer.close();
        }
    }

    @Test
    public void testRemoveSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        pulsarClient.newProducer().topic(persistenceTopic).create().close();
        SubscribeRate subscribeRate = new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

         Awaitility.await()
                 .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscribeRate(persistenceTopic), subscribeRate));

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer<byte[]> consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer1);
         consumer1.close();
         pulsarClient1.shutdown();

        Consumer<byte[]> consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer2);
         consumer2.close();
         pulsarClient2.shutdown();

        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }
        Assert.assertNull(consumer3);

        admin.topics().removeSubscribeRate(persistenceTopic);
        Awaitility.await()
                 .untilAsserted(() -> Assert.assertNull(admin.topics().getSubscribeRate(persistenceTopic)));

        admin.topics().unload(persistenceTopic);

        PulsarClient pulsarClient4 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient5 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient6 = newPulsarClient(lookupUrl.toString(), 0);

         consumer3 = pulsarClient3.newConsumer().subscriptionName("sub2")
                 .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer3);
         consumer3.close();
         pulsarClient3.shutdown();

         Consumer<byte[]> consumer4 = pulsarClient4.newConsumer().subscriptionName("sub2")
                 .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer4);
         consumer4.close();
         pulsarClient4.shutdown();

         Consumer<byte[]> consumer5 = pulsarClient5.newConsumer().subscriptionName("sub2")
                 .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer5);
         consumer5.close();
         pulsarClient5.shutdown();

         Consumer<byte[]> consumer6 = pulsarClient6.newConsumer().subscriptionName("sub2")
                 .topic(persistenceTopic).consumerName("test").subscribe();
         Assert.assertNotNull(consumer6);
         consumer6.close();
         pulsarClient6.shutdown();

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testPublishRateInDifferentLevelPolicy() throws Exception {
        cleanup();
        conf.setMaxPublishRatePerTopicInMessages(5);
        conf.setMaxPublishRatePerTopicInBytes(50L);
        setup();

        final String topicName = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topicName).create().close();
        Field publishMaxMessageRate = PublishRateLimiterImpl.class.getDeclaredField("publishMaxMessageRate");
        publishMaxMessageRate.setAccessible(true);
        Field publishMaxByteRate = PublishRateLimiterImpl.class.getDeclaredField("publishMaxByteRate");
        publishMaxByteRate.setAccessible(true);

        //1 use broker-level policy by default
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        PublishRateLimiterImpl publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 5);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 50L);

        //2 set namespace-level policy
        PublishRate publishMsgRate = new PublishRate(10, 100L);
        admin.namespaces().setPublishRate(myNamespace, publishMsgRate);

        Awaitility.await()
                .until(() -> {
                    PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
                    return (int)publishMaxMessageRate.get(limiter) == 10;
                });

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 10);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 100L);

        //3 set topic-level policy, namespace-level policy should be overwritten
        PublishRate publishMsgRate2 = new PublishRate(11, 101L);
        admin.topics().setPublishRate(topicName, publishMsgRate2);

        Awaitility.await()
                .until(() -> admin.topics().getPublishRate(topicName) != null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 11);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 101L);

        //4 remove topic-level policy, namespace-level policy will take effect
        admin.topics().removePublishRate(topicName);

        Awaitility.await()
                .until(() -> admin.topics().getPublishRate(topicName) == null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 10);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 100L);

        //5 remove namespace-level policy, broker-level policy will take effect
        admin.namespaces().removePublishRate(myNamespace);

        Awaitility.await()
                .until(() -> {
                    PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
                    return (int)publishMaxMessageRate.get(limiter) == 5;
                });

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 5);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 50L);
    }

    @Test(timeOut = 20000)
    public void testTopicMaxMessageSizeApi() throws Exception{
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        assertNull(admin.topics().getMaxMessageSize(persistenceTopic));
        admin.topics().setMaxMessageSize(persistenceTopic,10);
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(persistenceTopic)) != null);
        assertEquals(admin.topics().getMaxMessageSize(persistenceTopic).intValue(),10);

        admin.topics().removeMaxMessageSize(persistenceTopic);
        assertNull(admin.topics().getMaxMessageSize(persistenceTopic));

        try {
            admin.topics().setMaxMessageSize(persistenceTopic,Integer.MAX_VALUE);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(),412);
        }
        try {
            admin.topics().setMaxMessageSize(persistenceTopic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(),412);
        }
    }

    @Test(timeOut = 20000)
    public void testTopicMaxMessageSize() throws Exception{
        doTestTopicMaxMessageSize(true);
        doTestTopicMaxMessageSize(false);
    }

    private void doTestTopicMaxMessageSize(boolean isPartitioned) throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }
        // init cache
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        assertNull(admin.topics().getMaxMessageSize(topic));
        // set msg size
        admin.topics().setMaxMessageSize(topic, 10);
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)) != null);
        assertEquals(admin.topics().getMaxMessageSize(topic).intValue(), 10);

        try {
            producer.send(new byte[1024]);
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.NotAllowedException);
        }

        admin.topics().removeMaxMessageSize(topic);
        assertNull(admin.topics().getMaxMessageSize(topic));

        try {
            admin.topics().setMaxMessageSize(topic, Integer.MAX_VALUE);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        try {
            admin.topics().setMaxMessageSize(topic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }

        Awaitility.await().untilAsserted(() -> {
            try {
                MessageId messageId = producer.send(new byte[1024]);
                assertNotNull(messageId);
            } catch (PulsarClientException e) {
                fail("failed to send message");
            }
        });
        producer.close();
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsFailFast() throws Exception {
        doTestMaxSubscriptionsFailFast(SubscriptionMode.Durable);
        doTestMaxSubscriptionsFailFast(SubscriptionMode.NonDurable);
    }

    private void doTestMaxSubscriptionsFailFast(SubscriptionMode subMode) throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        int maxSubInNamespace = 2;
        List<Consumer> consumers = new ArrayList<>();
        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer().subscriptionMode(subMode)
                .subscriptionType(SubscriptionType.Shared).topic(topic);
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, maxSubInNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace)));
        for (int i = 0; i < maxSubInNamespace; i++) {
            consumers.add(consumerBuilder.subscriptionName("sub" + i).subscribe());
        }
        long start = System.currentTimeMillis();
        try {
            consumerBuilder.subscriptionName("sub").subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.NotAllowedException);
        }
        //fail fast
        assertTrue(System.currentTimeMillis() - start < 3000);
        //clean
        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopicApi() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();

        assertNull(admin.topics().getMaxSubscriptionsPerTopic(topic));
        // set max subscriptions
        admin.topics().setMaxSubscriptionsPerTopic(topic, 10);
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)) != null);
        assertEquals(admin.topics().getMaxSubscriptionsPerTopic(topic).intValue(), 10);
        // remove max subscriptions
        admin.topics().removeMaxSubscriptionsPerTopic(topic);
        assertNull(admin.topics().getMaxSubscriptionsPerTopic(topic));
        // set invalidate value
        try {
            admin.topics().setMaxMessageSize(topic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopicWithExistingSubs() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        // Set topic-level max subscriptions
        final int topicLevelMaxSubNum = 2;
        admin.topics().setMaxSubscriptionsPerTopic(topic, topicLevelMaxSubNum);
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)) != null);
        List<Consumer<String>> consumerList = new ArrayList<>();
        String subName = "my-sub-";
        for (int i = 0; i < topicLevelMaxSubNum; i++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(subName + i)
                    .topic(topic).subscribe();
            consumerList.add(consumer);
        }
        // should fail
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        //create a consumer with the same subscription name, it should succeed
        pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName + "0")
                .topic(topic).subscribe().close();

        //Clean up
        for (Consumer<String> c : consumerList) {
            c.close();
        }
    }

    @Test
    public void testMaxUnackedMessagesOnSubscriptionPriority() throws Exception {
        cleanup();
        conf.setMaxUnackedMessagesPerSubscription(30);
        setup();
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        @Cleanup
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        //default value is null
        assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace));
        int msgNum = 100;
        int maxUnackedMsgOnTopic = 10;
        int maxUnackedMsgNumOnNamespace = 5;
        int defaultMaxUnackedMsgOnBroker = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        produceMsg(producer, msgNum);
        //set namespace-level policy, the restriction should take effect
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace, maxUnackedMsgNumOnNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace).intValue(), maxUnackedMsgNumOnNamespace));
        List<Message<?>> messages;
        String subName = "sub-" + UUID.randomUUID();
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topic)
                .subscriptionName(subName).receiverQueueSize(1)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared);
        @Cleanup
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        messages = getMsgReceived(consumer1, msgNum);
        assertEquals(messages.size(), maxUnackedMsgNumOnNamespace);
        ackMessages(consumer1, messages);
        //disable namespace-level policy, should unlimited
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace).intValue(), 0));
        messages = getMsgReceived(consumer1, 40);
        assertEquals(messages.size(), 40);
        ackMessages(consumer1, messages);

        //set topic-level and namespace-level policy, topic-level should has higher priority
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace, maxUnackedMsgNumOnNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace).intValue()
                , maxUnackedMsgNumOnNamespace));
        admin.topics().setMaxUnackedMessagesOnSubscription(topic, maxUnackedMsgOnTopic);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topics().getMaxUnackedMessagesOnSubscription(topic)));
        //check the value applied
        PersistentTopic persistentTopic = (PersistentTopic)pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        assertEquals(persistentTopic.getMaxUnackedMessagesOnSubscription(), maxUnackedMsgOnTopic);
        messages = getMsgReceived(consumer1, Integer.MAX_VALUE);
        assertEquals(messages.size(), maxUnackedMsgOnTopic);
        ackMessages(consumer1, messages);

        //remove both namespace-level and topic-level policy, broker-level should take effect
        admin.namespaces().removeMaxUnackedMessagesPerSubscription(myNamespace);
        admin.topics().removeMaxUnackedMessagesOnSubscription(topic);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace) == null
                        && admin.topics().getMaxUnackedMessagesOnSubscription(topic) == null);
        messages = getMsgReceived(consumer1, Integer.MAX_VALUE);
        assertEquals(messages.size(), defaultMaxUnackedMsgOnBroker);
    }

    private void produceMsg(Producer producer, int msgNum) throws Exception{
        for (int i = 0; i < msgNum; i++) {
            producer.send("msg".getBytes());
        }
    }

    private List<Message<?>> getMsgReceived(Consumer<byte[]> consumer1, int msgNum) throws PulsarClientException {
        List<Message<?>> messages = new ArrayList<>();
        for (int i = 0; i < msgNum; i++) {
            Message<?> message = consumer1.receive(1000, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            messages.add(message);
        }
        return messages;
    }

    private void ackMessages(Consumer<?> consumer, List<Message<?>> messages) throws Exception{
        for (Message<?> message : messages) {
            consumer.acknowledge(message);
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopic() throws Exception {
        int brokerLevelMaxSub = 4;
        conf.setMaxSubscriptionsPerTopic(4);
        restartBroker();
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        // Set topic-level max subscriptions
        final int topicLevelMaxSubNum = 2;
        admin.topics().setMaxSubscriptionsPerTopic(topic, topicLevelMaxSubNum);
        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)) != null);

        List<Consumer<String>> consumerList = new ArrayList<>();
        for (int i = 0; i < topicLevelMaxSubNum; i++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe();
            consumerList.add(consumer);
        }
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        // Set namespace-level policy, but will not take effect
        final int namespaceLevelMaxSub = 3;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, namespaceLevelMaxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Field field = PersistentTopic.class.getSuperclass().getDeclaredField("maxSubscriptionsPerTopic");
        field.setAccessible(true);
        Awaitility.await().until(() -> (int) field.get(persistentTopic) == namespaceLevelMaxSub);

        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        //Removed topic-level policy, namespace-level should take effect
        admin.topics().removeMaxSubscriptionsPerTopic(topic);
        consumerList.add(pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe());
        assertEquals(consumerList.size(), namespaceLevelMaxSub);
        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), namespaceLevelMaxSub);
        }
        //Removed namespace-level policy, broker-level should take effect
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() -> field.get(persistentTopic) == null);
        consumerList.add(pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe());
        assertEquals(consumerList.size(), brokerLevelMaxSub);
        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), brokerLevelMaxSub);
        }
        //Clean up
        for (Consumer<String> c : consumerList) {
            c.close();
        }
    }

    @Test(timeOut = 30000)
    public void testReplicatorRateApi() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();

        assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic));

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(200)
                .ratePeriodInSecond(10)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, dispatchRate);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic), dispatchRate));

        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic)));
    }

    @Test(timeOut = 20000)
    public void testGetReplicatorRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic));
        assertNull(admin.namespaces().getReplicatorDispatchRate(myNamespace));
        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(pulsar.getConfiguration().getDispatchThrottlingRatePerReplicatorInMsg())
                .dispatchThrottlingRateInByte(pulsar.getConfiguration().getDispatchThrottlingRatePerReplicatorInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setReplicatorDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getReplicatorDispatchRate(myNamespace)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(22)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getReplicatorDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), topicDispatchRate);

        admin.namespaces().removeReplicatorDispatchRate(myNamespace);
        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getReplicatorDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true), brokerDispatchRate);
    }

    @Test(timeOut = 30000)
    public void testAutoCreationDisabled() throws Exception {
        cleanup();
        conf.setAllowAutoTopicCreation(false);
        setup();
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        pulsarClient.newProducer().topic(topic).create().close();
        //should not fail
        assertNull(admin.topicPolicies().getMessageTTL(topic));
    }

    @Test
    public void testSubscriptionTypesWithPartitionedTopic() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);
        pulsarClient.newConsumer().topic(topic).subscriptionName("test").subscribe().close();
        Set<SubscriptionType> subscriptionTypeSet = new HashSet<>();
        subscriptionTypeSet.add(SubscriptionType.Key_Shared);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getSubscriptionTypesEnabled(topic)));

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(TopicName.get(topic).getPartition(0).toString()).get();
        Set<String> old = new HashSet<>(pulsar.getConfiguration().getSubscriptionTypesEnabled());
        try {
            pulsar.getConfiguration().getSubscriptionTypesEnabled().clear();
            assertTrue(persistentTopic.checkSubscriptionTypesEnable(CommandSubscribe.SubType.Key_Shared));
        } finally {
            //restore
            pulsar.getConfiguration().getSubscriptionTypesEnabled().addAll(old);
        }
    }

    @Test(timeOut = 30000)
    public void testSubscriptionTypesEnabled() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        // use broker.conf
        pulsarClient.newConsumer().topic(topic).subscriptionName("test").subscribe().close();
        assertNull(admin.topicPolicies().getSubscriptionTypesEnabled(topic));
        // set enable failover sub type
        Set<SubscriptionType> subscriptionTypeSet = new HashSet<>();
        subscriptionTypeSet.add(SubscriptionType.Failover);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);

        Awaitility.await().until(()
                -> pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)) != null);
        subscriptionTypeSet = admin.topicPolicies().getSubscriptionTypesEnabled(topic);
        assertTrue(subscriptionTypeSet.contains(SubscriptionType.Failover));
        assertFalse(subscriptionTypeSet.contains(SubscriptionType.Shared));
        assertEquals(subscriptionTypeSet.size(), 1);
        try {
            pulsarClient.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Shared).subscriptionName("test").subscribe();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }

        // add shared type
        subscriptionTypeSet.add(SubscriptionType.Shared);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);
        pulsarClient.newConsumer().topic(topic)
                .subscriptionType(SubscriptionType.Shared).subscriptionName("test").subscribe().close();

        // test namespace and topic policy
        subscriptionTypeSet.add(SubscriptionType.Shared);
        admin.namespaces().setSubscriptionTypesEnabled(myNamespace, subscriptionTypeSet);

        subscriptionTypeSet.clear();
        subscriptionTypeSet.add(SubscriptionType.Failover);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);

        try {
            pulsarClient.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Shared).subscriptionName("test").subscribe();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }
    }

    @Test(timeOut = 20000)
    public void testNonPersistentMaxConsumerOnSub() throws Exception {
        int maxConsumerPerSubInBroker = 1;
        int maxConsumerPerSubInNs = 2;
        int maxConsumerPerSubInTopic = 3;
        conf.setMaxConsumersPerSubscription(maxConsumerPerSubInBroker);
        final String topic = "non-persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        final String subName = "my-sub";
        ConsumerBuilder builder = pulsarClient.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName).topic(topic);
        Consumer consumer = builder.subscribe();

        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }
        // set namespace policy
        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, maxConsumerPerSubInNs);
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace));
            assertEquals(admin.namespaces().getMaxConsumersPerSubscription(myNamespace).intValue(), maxConsumerPerSubInNs);
        });
        Consumer consumer2 = builder.subscribe();
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }

        //set topic policy
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumerPerSubInTopic);
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.topicPolicies().getMaxConsumersPerSubscription(topic));
            assertEquals(admin.topicPolicies().getMaxConsumersPerSubscription(topic).intValue(), maxConsumerPerSubInTopic);
        });
        Consumer consumer3 = builder.subscribe();
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }
        consumer.close();
        consumer2.close();
        consumer3.close();
        producer.close();
    }

    @Test(timeOut = 20000)
    public void testGetCompactionThresholdApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        assertNull(admin.namespaces().getCompactionThreshold(myNamespace));
        long brokerPolicy = pulsar.getConfiguration().getBrokerServiceCompactionThresholdInBytes();
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(), brokerPolicy);
        long namespacePolicy = 10L;

        admin.namespaces().setCompactionThreshold(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getCompactionThreshold(myNamespace)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(), namespacePolicy);

        long topicPolicy = 20L;
        admin.topicPolicies().setCompactionThreshold(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getCompactionThreshold(topic)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(), topicPolicy);

        admin.namespaces().removeCompactionThreshold(myNamespace);
        admin.topicPolicies().removeCompactionThreshold(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getCompactionThreshold(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getCompactionThreshold(topic)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(), brokerPolicy);
    }

    @Test(timeOut = 30000)
    public void testProduceConsumeOnTopicPolicy() {
        final String msg = "send message ";
        int numMsg = 10;
        try {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(persistenceTopic)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("test").subscribe();

            Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(persistenceTopic).create();

            for (int i = 0; i < numMsg; ++i) {
                producer.newMessage().value(msg + i).send();
            }

            for (int i = 0; i < numMsg; ++i) {
                Message<String> message = consumer.receive(100, TimeUnit.MILLISECONDS);
                Assert.assertEquals(message.getValue(), msg + i);
            }
        } catch (PulsarClientException e) {
            log.error("Failed to send/produce message, ", e);
            Assert.fail();
        }
    }

    @Test(timeOut = 30000)
    public void testSystemTopicShouldBeCompacted() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> {
                    TopicStats stats = admin.topics().getStats(topicPolicyEventsTopic);
                    Assert.assertTrue(stats.getSubscriptions().containsKey("__compaction"));
                });

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicPolicyEventsTopic);
        long previousCompactedLedgerId = internalStats.compactedLedger.ledgerId;

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        pulsar.getBrokerService().checkCompaction();

        Awaitility.await()
                .untilAsserted(() -> {
                    PersistentTopicInternalStats iStats = admin.topics().getInternalStats(topicPolicyEventsTopic);
                    Assert.assertTrue(iStats.compactedLedger.ledgerId != previousCompactedLedgerId);
                });
    }

    @Test(timeOut = 30000)
    public void testTopicRetentionPolicySetInManagedLedgerConfig() throws Exception {
        RetentionPolicies nsRetentionPolicies = new RetentionPolicies(1, -1);
        TopicName topicName = TopicName.get(testTopic);

        // set and check retention policy on namespace level
        admin.namespaces().setRetention(myNamespace, nsRetentionPolicies);
        ManagedLedgerConfig managedLedgerConfig = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        Assert.assertEquals(managedLedgerConfig.getRetentionTimeMillis(),
                TimeUnit.MINUTES.toMillis(nsRetentionPolicies.getRetentionTimeInMinutes()));
        Assert.assertEquals(managedLedgerConfig.getRetentionSizeInMB(), nsRetentionPolicies.getRetentionSizeInMB());

        // set and check retention policy on topic level
        RetentionPolicies topicRetentionPolicies = new RetentionPolicies(2, -1);
        admin.topicPolicies().setRetention(testTopic, topicRetentionPolicies);
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerConfig config = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
            Assert.assertEquals(config.getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(topicRetentionPolicies.getRetentionTimeInMinutes()));
            Assert.assertEquals(config.getRetentionSizeInMB(), topicRetentionPolicies.getRetentionSizeInMB());
        });
    }

    @Test
    public void testPolicyIsDeleteTogetherManually() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNull());

        int maxConsumersPerSubscription = 10;
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumersPerSubscription);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false).get().isPresent()).isTrue());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNotNull());

        admin.topics().delete(topic);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false).get().isPresent()).isFalse());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNull());
    }

    @Test
    public void testPoliciesCanBeDeletedWithTopic() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        final String topic2 = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        pulsarClient.newProducer().topic(topic2).create().close();

        Awaitility.await().untilAsserted(() -> {
            Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic))).isNull();
            Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic2))).isNull();
        });
        // Init Topic Policies. Send 4 messages in a row, there should be only 2 messages left after compression
        admin.topics().setMaxConsumersPerSubscription(topic, 1);
        admin.topics().setMaxConsumersPerSubscription(topic2, 2);
        admin.topics().setMaxConsumersPerSubscription(topic, 3);
        admin.topics().setMaxConsumersPerSubscription(topic2, 4);
        Awaitility.await().untilAsserted(() -> {
            Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic))).isNotNull();
            Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic2))).isNotNull();
        });
        String topicPoliciesTopic = "persistent://" + myNamespace + "/" + EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicPoliciesTopic).get().get();
        // Trigger compaction and make sure it is finished.
        persistentTopic.triggerCompaction();
        Field field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);
        CompletableFuture<Long> future = (CompletableFuture<Long>)field.get(persistentTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(future.isDone()));

        Consumer consumer = pulsarClient.newConsumer()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .topic(topicPoliciesTopic).subscriptionName("sub").subscribe();
        int count = 0;
        while (true) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                count++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        consumer.close();
        assertEquals(count, 2);

        // Delete topic, there should be only 1 message left after compression
        admin.topics().delete(topic, true);

        Awaitility.await().untilAsserted(() ->
                assertNull(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic))));
        persistentTopic.triggerCompaction();
        field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);
        CompletableFuture<Long> future2 = (CompletableFuture<Long>)field.get(persistentTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(future2.isDone()));

        consumer = pulsarClient.newConsumer()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .topic(topicPoliciesTopic).subscriptionName("sub").subscribe();
        count = 0;
        while (true) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                count++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        consumer.close();
        assertEquals(count, 1);

    }

    @Test
    public void testPolicyIsDeleteTogetherAutomatically() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNull());

        int maxConsumersPerSubscription = 10;
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumersPerSubscription);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false).get().isPresent()).isTrue());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNotNull());

        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 3, true);
        admin.topicPolicies().setInactiveTopicPolicies(topic, inactiveTopicPolicies);

        Thread.sleep(4_000L);

        pulsar.getBrokerService().checkGC();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false).get().isPresent()).isFalse());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getTopicPoliciesService().getTopicPolicies(TopicName.get(topic)))
                        .isNull());
    }

    @Test
    public void testDoNotCreateSystemTopicForHeartbeatNamespace() {
        assertTrue(pulsar.getBrokerService().getTopics().size() > 0);
        pulsar.getBrokerService().getTopics().forEach((k, v) -> {
            TopicName topicName = TopicName.get(k);
            assertNull(NamespaceService.checkHeartbeatNamespace(topicName.getNamespaceObject()));
            assertNull(NamespaceService.checkHeartbeatNamespaceV2(topicName.getNamespaceObject()));
        });
    }

}
