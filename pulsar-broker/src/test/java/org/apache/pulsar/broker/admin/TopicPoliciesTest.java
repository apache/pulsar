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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Slf4j
public class TopicPoliciesTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    private final String persistenceTopic = "persistent://" + myNamespace + "/test-set-persistence";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(testTopic, 2);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();
        waitForZooKeeperWatchers();
        return;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        this.resetConfig();
    }

    @Test
    public void testSetBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = new BacklogQuota(1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveBacklogQuota() throws Exception {
        BacklogQuota backlogQuota = new BacklogQuota(1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        BacklogQuotaManager backlogQuotaManager = pulsar.getBrokerService().getBacklogQuotaManager();
        BacklogQuota backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInManager, testTopic);
        Assert.assertEquals(backlogQuota, backlogQuotaInManager);

        admin.topics().removeBacklogQuota(testTopic);
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage)));

        backlogQuotaInManager = backlogQuotaManager.getBacklogQuota(TopicName.get(testTopic));
        log.info("Backlog quota {} in backlog quota manager on topic: {} after remove", backlogQuotaInManager,
                testTopic);
        Assert.assertEquals(backlogQuotaManager.getDefaultQuota(), backlogQuotaInManager);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace), retentionPolicies));

        BacklogQuota backlogQuota =
                new BacklogQuota(10 * 1024 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota =
                new BacklogQuota(10 * 1024 * 1024 + 1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topics().setBacklogQuota(testTopic, backlogQuota);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota =
                new BacklogQuota(10 * 1024 * 1024 - 1, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topics().setBacklogQuota(testTopic, backlogQuota);

        BacklogQuota finalBacklogQuota = backlogQuota;
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), finalBacklogQuota));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckRetention() throws Exception {
        BacklogQuota backlogQuota =
                new BacklogQuota(10 * 1024 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setBacklogQuota(testTopic, backlogQuota);
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), finalRetention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetRetention() throws Exception {
        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), retention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveRetention() throws Exception {

        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getRetention(testTopic), retention));

        admin.topics().removeRetention(testTopic);
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getRetention(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace), persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPersistence(persistenceTopic), persistencePolicies));

        admin.lookups().lookupTopic(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        assertEquals(managedLedgerConfig.getEnsembleSize(), 3);
        assertEquals(managedLedgerConfig.getWriteQuorumSize(), 3);
        assertEquals(managedLedgerConfig.getAckQuorumSize(), 3);
        assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.1);

        PersistencePolicies getPersistencePolicies = admin.topics().getPersistence(persistenceTopic);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        Assert.assertEquals(getPersistencePolicies, persistencePolicies);
    }

    @Test
    public void testRemovePersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace), persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPersistence(persistenceTopic), persistencePolicies));

        admin.topics().removePersistence(persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
    public void testSetMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxProducers(persistenceTopic, maxProducers);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxProducers(persistenceTopic), maxProducers));

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxProducers(persistenceTopic, maxProducers);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxProducers(persistenceTopic), maxProducers));

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getMaxProducers(persistenceTopic)));

        producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Assert.assertNotNull(producer3);
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
        DispatchRate dispatchRate = new DispatchRate(100, 10000, 1, true);
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getDispatchRate(testTopic), dispatchRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveDispatchRate() throws Exception {
        DispatchRate dispatchRate = new DispatchRate(100, 10000, 1, true);
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getDispatchRate(testTopic), dispatchRate));

        admin.topics().removeDispatchRate(testTopic);
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getDispatchRate(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 20000)
    public void testPolicyOverwrittenByNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        DispatchRate dispatchRate = new DispatchRate(200, 20000, 1, true);
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getDispatchRate(myNamespace).dispatchThrottlingRateInMsg, 200));

        dispatchRate = new DispatchRate(100, 10000, 1, true);
        admin.topics().setDispatchRate(topic, dispatchRate);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNotNull(admin.topics().getDispatchRate(topic)));

        //1 Set ns level policy, topic level should not be overwritten
        dispatchRate = new DispatchRate(300, 30000, 2, true);
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get().getDispatchRateLimiter().get();
                    Assert.assertEquals(limiter.getDispatchRateOnByte(), 10000);
                    Assert.assertEquals(limiter.getDispatchRateOnMsg(), 100);
                });

        admin.topics().removeDispatchRate(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getDispatchRate(topic)));

        //2 Remove level policy ,DispatchRateLimiter should us ns level policy
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up,100,true);
        admin.namespaces().setInactiveTopicPolicies(myNamespace, inactiveTopicPolicies);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getInactiveTopicPolicies(myNamespace).getInactiveTopicDeleteMode(),
                        InactiveTopicDeleteMode.delete_when_subscriptions_caught_up));

        inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,200,false);
        admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNotNull(admin.topics().getInactiveTopicPolicies(topic)));

        // restart broker, policy should still take effect
        restartBroker();

        // Trigger the cache init.
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        InactiveTopicPolicies finalInactiveTopicPolicies = inactiveTopicPolicies;
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
                    Assert.assertEquals(persistentTopic.getInactiveTopicPolicies(), finalInactiveTopicPolicies);
                });

        producer.close();
    }

    @Test
    public void testGetSetSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        DispatchRate dispatchRate = new DispatchRate(1000,
                1024 * 1024, 1);
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetSubscriptionDispatchRateAfterTopicLoaded() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        DispatchRate dispatchRate = new DispatchRate(1000,
                1024 * 1024, 1);
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testRemoveSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        DispatchRate dispatchRate = new DispatchRate(1000,
                1024 * 1024, 1);
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topics().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);

        // remove subscription dispatch rate
        admin.topics().removeSubscriptionDispatchRate(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getSubscriptionDispatchRate(topic)));

        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate.dispatchThrottlingRateInMsg);
        Assert.assertNotEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate.dispatchThrottlingRateInByte);

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testSubscriptionDispatchRatePolicyOverwrittenNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));

        // set namespace level subscription dispatch rate
        DispatchRate namespaceDispatchRate = new DispatchRate(100, 1024 * 1024, 1);
        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getSubscriptionDispatchRate(myNamespace), namespaceDispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName).topic(topic).subscribe();

        // get subscription dispatch Rate limiter
        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.dispatchThrottlingRateInMsg);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.dispatchThrottlingRateInByte);

        // set topic level subscription dispatch rate
        DispatchRate topicDispatchRate = new DispatchRate(200, 2 * 1024 * 1024, 1);
        admin.topics().setSubscriptionDispatchRate(topic, topicDispatchRate);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getSubscriptionDispatchRate(topic), topicDispatchRate));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), topicDispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), topicDispatchRate.dispatchThrottlingRateInMsg);

        // remove topic level subscription dispatch rate limiter
        admin.topics().removeSubscriptionDispatchRate(topic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getSubscriptionDispatchRate(topic)));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate.dispatchThrottlingRateInByte);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate.dispatchThrottlingRateInMsg);

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getCompactionThreshold(testTopic), compactionThreshold));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getCompactionThreshold(testTopic), compactionThreshold));

        admin.topics().removeCompactionThreshold(testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getCompactionThreshold(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topics().removeMaxConsumersPerSubscription(testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertNull(admin.topics().getMaxConsumersPerSubscription(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetPublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPublishRate(testTopic), publishRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemovePublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getPublishRate(testTopic), publishRate));

        admin.topics().removePublishRate(testTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
    public void testSetMaxConsumers() throws Exception {
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 1);
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getMaxConsumersPerTopic(myNamespace), 1));
        log.info("MaxConsumers: {} will set to the namespace: {}", 1, myNamespace);
        Integer maxConsumers = 2;
        log.info("MaxConsumers: {} will set to the topic: {}", maxConsumers, persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumers(persistenceTopic), maxConsumers));

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
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
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        admin.topics().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.topics().getMaxConsumers(persistenceTopic), maxConsumers));

        admin.topics().createPartitionedTopic(persistenceTopic, 2);
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
    public void testGetSetSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

        SubscribeRate subscribeRate1 = new SubscribeRate(1, 30);
        log.info("Subscribe Rate: {} will be set to the namespace: {}", subscribeRate1, myNamespace);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate1);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getSubscribeRate(myNamespace), subscribeRate1));

        SubscribeRate subscribeRate2 =  new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate2, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate2);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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

    @Test
    public void testRemoveSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
             .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(persistenceTopic)));

        SubscribeRate subscribeRate = new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, persistenceTopic);
        admin.topics().setSubscribeRate(persistenceTopic, subscribeRate);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

         Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(testTopic)));

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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> admin.topics().getPublishRate(topicName) != null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 11);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 101L);

        //4 remove topic-level policy, namespace-level policy will take effect
        admin.topics().removePublishRate(topicName);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .until(() -> admin.topics().getPublishRate(topicName) == null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishMaxMessageRate.get(publishRateLimiter), 10);
        Assert.assertEquals(publishMaxByteRate.get(publishRateLimiter), 100L);

        //5 remove namespace-level policy, broker-level policy will take effect
        admin.namespaces().removePublishRate(myNamespace);

        Awaitility.await().atMost(3, TimeUnit.SECONDS)
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
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(persistenceTopic)));
        assertNull(admin.topics().getMaxMessageSize(persistenceTopic));

        admin.topics().setMaxMessageSize(persistenceTopic,10);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()
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
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> pulsar.getTopicPoliciesService().cacheIsInitialized(TopicName.get(topic)));
        assertNull(admin.topics().getMaxMessageSize(topic));
        // set msg size
        admin.topics().setMaxMessageSize(topic, 10);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()
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

        MessageId messageId = producer.send(new byte[1024]);
        assertNotNull(messageId);
        producer.close();
    }
}
