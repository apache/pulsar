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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.MetadataChangeEvent;
import org.apache.pulsar.broker.service.MetadataChangeEvent.EventType;
import org.apache.pulsar.broker.service.MetadataChangeEvent.ResourceType;
import org.apache.pulsar.broker.service.MetadataStoreSynchronizer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Cleanup;

@Test(groups = "broker-impl")
public class MetadataStoreSynchronizerTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testNamespaceMetadataEventSyncerPublish() throws Exception {
        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        Policies policies = new Policies();
        policies.replication_clusters = Sets.newHashSet("test1", "test2");
        byte[] data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        // (1) create a new namespace
        String ns2 = "my-property/brok-ns2";
        MetadataChangeEvent event = new MetadataChangeEvent();
        event.setResource(ResourceType.Namespaces);
        event.setType(EventType.Created);
        event.setSourceCluster("test2");
        event.setResourceName(ns2);
        event.setData(data);

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        // (1) create namespace
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.namespaces().getPolicies(ns2);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        policies = admin.namespaces().getPolicies(ns2);
        assertNotNull(policies);
        assertEquals(policies.replication_clusters, Sets.newHashSet("test1", "test2"));

        // (2) try to publish : create namespace message again. even should be acked successfully
        policies.lastUpdatedTimestamp += 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        long time = pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp;

        // (3) update with old event time
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp -= 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);

        // (4) update with new event time
        event.setType(EventType.Modified);
        time = policies.lastUpdatedTimestamp + 1;
        policies.lastUpdatedTimestamp = time;
        policies.replication_clusters = Sets.newHashSet("test1", "test2", "test3");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1", "test2", "test3"));

        // (5) Invalid data
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp += 1;
        policies.replication_clusters = Sets.newHashSet("test1", "test2", "test3");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes("invalid-data");
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies which should have not changed
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);

        // (6) timestamp racecondition but pick cluster-name to win the update: local cluster will win
        event.setType(EventType.Modified);
        time = pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp;
        policies.lastUpdatedTimestamp = time;
        policies.replication_clusters = Sets.newHashSet("test1");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setSourceCluster("abc"); // "test" < "abc". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1", "test2", "test3"));

        // (7) timestamp racecondition but pick cluster-name to win the update: remote cluster will win
        event.setType(EventType.Modified);
        policies.lastUpdatedTimestamp = time + 1;
        policies.replication_clusters = Sets.newHashSet("test1");
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(policies);
        event.setSourceCluster("uvw"); // "test" < "uvw". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().lastUpdatedTimestamp, time + 1);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get(ns2))
                .get().replication_clusters, Sets.newHashSet("test1"));

        producer1.close();
    }

    @Test
    public void testTenantMetadataEventSyncerPublish() throws Exception {
        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        TenantInfoImpl t1 = (TenantInfoImpl) TenantInfo.builder().allowedClusters(Sets.newHashSet("test")).build();
        byte[] data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        // (1) create a new namespace
        String tenantName = "tenant1";
        MetadataChangeEvent event = new MetadataChangeEvent();
        event.setResource(ResourceType.Tenants);
        event.setType(EventType.Created);
        event.setSourceCluster("test2");
        event.setResourceName(tenantName);
        event.setData(data);

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        // (1) create namespace
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.tenants().getTenantInfo(tenantName);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        t1 = (TenantInfoImpl) admin.tenants().getTenantInfo(tenantName);
        assertNotNull(t1);
        assertEquals(t1.getAllowedClusters(), Sets.newHashSet("test"));

        // (2) try to publish : create namespace message again. even should be acked successfully
        t1.setLastUpdatedTimestamp(t1.getLastUpdatedTimestamp() + 1);
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of tenant
        long time = ((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp();

        // (3) update with old event time
        event.setType(EventType.Modified);
        t1.setLastUpdatedTimestamp(t1.getLastUpdatedTimestamp() - 1);
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp(), time);

        // (4) update with new event time
        event.setType(EventType.Modified);
        time = t1.getLastUpdatedTimestamp() + 1;
        t1.setLastUpdatedTimestamp(time);
        t1.setAdminRoles(Sets.newHashSet("test1", "test2", "test3"));
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp(), time);
        assertEquals(pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get().getAdminRoles(),
                Sets.newHashSet("test1", "test2", "test3"));

        // (5) Invalid data
        event.setType(EventType.Modified);
        t1.setLastUpdatedTimestamp(t1.getLastUpdatedTimestamp() + 1);
        t1.setAdminRoles(Sets.newHashSet("test1", "test2", "test3"));
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes("invalid-data");
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies which should have not changed
        assertEquals(((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp(), time);

        // (6) timestamp racecondition but pick cluster-name to win the update: local cluster will win
        event.setType(EventType.Modified);
        time = ((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp();
        t1.setLastUpdatedTimestamp(time);
        t1.setAdminRoles(Sets.newHashSet("test1"));
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        event.setSourceCluster("abc"); // "test" < "abc". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp(), time);
        assertEquals(pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get().getAdminRoles(),
                Sets.newHashSet("test1", "test2", "test3"));

        // (7) timestamp racecondition but pick cluster-name to win the update: remote cluster will win
        event.setType(EventType.Modified);
        t1.setLastUpdatedTimestamp(time + 1);
        t1.setAdminRoles(Sets.newHashSet("test1"));
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(t1);
        event.setSourceCluster("uvw"); // "test" < "uvw". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(((TenantInfoImpl) pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get())
                .getLastUpdatedTimestamp(), time + 1);
        assertEquals(pulsar.getPulsarResources().getTenantResources().getTenant(tenantName).get().getAdminRoles(),
                Sets.newHashSet("test1"));

        producer1.close();
    }

    @Test
    public void testPartitionedMetadataEventSyncerPublish() throws Exception {
        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        admin.namespaces().createNamespace("my-property/brok-ns2");
        PartitionedTopicMetadata partitionedMetadata = new PartitionedTopicMetadata(3);
        byte[] data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        // (1) create a new namespace
        String topicName = "persistent://my-property/brok-ns2/t1";
        MetadataChangeEvent event = new MetadataChangeEvent();
        event.setResource(ResourceType.TopicPartition);
        event.setType(EventType.Created);
        event.setSourceCluster("test2");
        event.setResourceName(topicName);
        event.setData(data);

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        // (1) create namespace
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.topics().getPartitionedTopicMetadata(topicName);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        partitionedMetadata = admin.topics().getPartitionedTopicMetadata(topicName);
        assertNotNull(partitionedMetadata);
        assertEquals(partitionedMetadata.partitions, 3);

        // (2) try to publish : create namespace message again. even should be acked successfully
        partitionedMetadata.lastUpdatedTimestamp += 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        long time = pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().lastUpdatedTimestamp;

        // (3) update with old event time
        event.setType(EventType.Modified);
        partitionedMetadata.lastUpdatedTimestamp -= 1;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(
                pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                        .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().lastUpdatedTimestamp,
                time);

        // (4) update with new event time
        event.setType(EventType.Modified);
        time = partitionedMetadata.lastUpdatedTimestamp + 1;
        partitionedMetadata.lastUpdatedTimestamp = time;
        partitionedMetadata.partitions = 4;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of partitioned-topic metadata
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().partitions, 4);

        // (5) Invalid data
        event.setType(EventType.Modified);
        partitionedMetadata.lastUpdatedTimestamp += 1;
        partitionedMetadata.partitions = 10;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes("invalid-data");
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
       
        // (6) timestamp racecondition but pick cluster-name to win the update: local cluster will win
        event.setType(EventType.Modified);
        time = pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().lastUpdatedTimestamp;
        partitionedMetadata.lastUpdatedTimestamp = time;
        partitionedMetadata.partitions = 10;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        event.setSourceCluster("abc"); // "test" < "abc". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        // check lastUpdated time of policies
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().partitions, 4);

        // (7) timestamp racecondition but pick cluster-name to win the update: remote cluster will win
        event.setType(EventType.Modified);
        partitionedMetadata.lastUpdatedTimestamp = time + 1;
        partitionedMetadata.partitions = 10;
        data = ObjectMapperFactory.getThreadLocal().writer().writeValueAsBytes(partitionedMetadata);
        event.setSourceCluster("uvw"); // "test" < "uvw". event should be skipped
        event.setData(data);
        producer1.newMessage().value(event).send();
        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                return admin.topics().getStats(metadataEventTopic).getSubscriptions()
                        .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize() == 0;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertEquals(admin.topics().getStats(metadataEventTopic).getSubscriptions()
                .get(MetadataStoreSynchronizer.SUBSCRIPTION_NAME).getBacklogSize(), 0);
        assertEquals(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(TopicName.get(topicName)).get().get().partitions, 10);

        producer1.close();
    }

    @Test
    public void testPublishConsumeNamespaceMetadataEventSyncer() throws Exception {

        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        @Cleanup
        Consumer<MetadataChangeEvent> consumer = pulsarClient.newConsumer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).subscriptionName("my-subscriber-name").subscribe();

        // (1) create a new namespace
        final String ns1 = "my-property/brok-ns2";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        // check event generation
        Message<MetadataChangeEvent> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        MetadataChangeEvent event = msg.getValue();
        assertEquals(event.getResource(), ResourceType.Namespaces);
        assertEquals(event.getType(), EventType.Created);
        assertEquals(event.getResourceName(), ns1);
        assertEquals(event.getSourceCluster(), conf.getClusterName());
        assertNotNull(event.getData());

        @Cleanup
        Producer<MetadataChangeEvent> producer1 = pulsarClient.newProducer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).create();

        String ns2 = event.getResourceName() + "-2";
        event.setResourceName(ns2);
        event.setSourceCluster("test2");
        producer1.newMessage().value(event).send();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> {
            try {
                admin.namespaces().getPolicies(ns2);
                return true;
            } catch (Exception e) {
                // ok
                return false;
            }
        });
        assertNotNull(admin.namespaces().getPolicies(ns2));
        producer1.close();
        consumer.close();
    }

    @Test
    public void testTriggerSnapshotMetadataEventSyncer() throws Exception {

        String metadataEventNs = "my-property/metadataTopic";
        String metadataEventTopic = "persistent://" + metadataEventNs + "/event";
        admin.namespaces().createNamespace(metadataEventNs, Sets.newHashSet("test"));
        conf.setMetadataSyncEventTopic(metadataEventTopic);
        restartBroker();

        Awaitility.waitAtMost(5, TimeUnit.SECONDS).until(() -> pulsar.getMetadataSyncer().isActive());

        // (1) create a new namespace
        String tenant1 = "my-tenant1";
        String tenant2 = "my-tenant2";
        TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(Sets.newHashSet("test"))
                .adminRoles(Sets.newHashSet("role1")).build();
        admin.tenants().createTenant(tenant1, tenantInfo);
        admin.tenants().createTenant(tenant2, tenantInfo);
        final String ns1 = tenant1 + "/brok-ns1";
        final String ns2 = tenant2 + "/brok-ns2";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns2, Sets.newHashSet("test"));

        Map<String, MetadataChangeEvent> eventResource = Maps.newHashMap();
        @Cleanup
        Consumer<MetadataChangeEvent> consumer = pulsarClient.newConsumer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).subscriptionName("my-subscriber-name").messageListener((c, m) -> {
                    MetadataChangeEvent event = m.getValue();
                    eventResource.put(event.getResourceName(), event);
                    c.acknowledgeAsync(m);
                }).subscribe();

        MetadataStoreSynchronizer syncer = pulsar.getMetadataSyncer();
        syncer.triggerSyncSnapshot();

        Awaitility.waitAtMost(135, TimeUnit.SECONDS).until(() -> {
            return eventResource.size() > 6;
        });

        // verify tenant and namespace updates
        // verify tenant-1
        verifyTenant(eventResource, tenant1, tenant2);
        verifyNamespace(eventResource, ns1, ns2);

        consumer.unsubscribe();

        // update tenant and namespaces
        tenantInfo = TenantInfo.builder().allowedClusters(Sets.newHashSet("test"))
                .adminRoles(Sets.newHashSet("role1", "role2")).build();
        admin.tenants().updateTenant(tenant1, tenantInfo);
        admin.tenants().updateTenant(tenant2, tenantInfo);

        eventResource.clear();
        @Cleanup
        Consumer<MetadataChangeEvent> consumer2 = pulsarClient.newConsumer(AvroSchema.of(MetadataChangeEvent.class))
                .topic(metadataEventTopic).subscriptionName("my-subscriber-name").messageListener((c, m) -> {
                    MetadataChangeEvent event = m.getValue();
                    eventResource.put(event.getResourceName(), event);
                    c.acknowledgeAsync(m);
                }).subscribe();

        syncer.triggerSyncSnapshot();

        Awaitility.waitAtMost(135, TimeUnit.SECONDS).until(() -> {
            return eventResource.size() > 6;
        });

        verifyTenant(eventResource, tenant1, tenant2);
        consumer2.close();
    }

    private void verifyNamespace(Map<String, MetadataChangeEvent> eventResource, String ns1, String ns2)
            throws Exception {
        // verify ns-1
        Policies eventPolicies1 = ObjectMapperFactory.getThreadLocal().readValue(eventResource.get(ns1).getData(),
                Policies.class);
        Policies n1 = admin.namespaces().getPolicies(ns1);
        assertEquals(eventPolicies1.replication_clusters, n1.replication_clusters);
        assertEquals(eventPolicies1.lastUpdatedTimestamp, n1.lastUpdatedTimestamp);
        Policies eventPolicies2 = ObjectMapperFactory.getThreadLocal().readValue(eventResource.get(ns2).getData(),
                Policies.class);
        Policies n2 = admin.namespaces().getPolicies(ns2);
        assertEquals(eventPolicies2.replication_clusters, n2.replication_clusters);
        assertEquals(eventPolicies2.lastUpdatedTimestamp, n2.lastUpdatedTimestamp);
    }

    private void verifyTenant(Map<String, MetadataChangeEvent> eventResource, String tenant1, String tenant2)
            throws Exception {
        TenantInfoImpl eventTenantInfo1 = ObjectMapperFactory.getThreadLocal()
                .readValue(eventResource.get(tenant1).getData(), TenantInfoImpl.class);
        TenantInfoImpl t1 = (TenantInfoImpl) admin.tenants().getTenantInfo(tenant1);
        assertEquals(eventTenantInfo1.getLastUpdatedTimestamp(), t1.getLastUpdatedTimestamp());
        assertEquals(eventTenantInfo1.getAdminRoles(), t1.getAdminRoles());
        assertEquals(eventTenantInfo1.getAllowedClusters(), t1.getAllowedClusters());
        // verify tenant-2
        TenantInfoImpl eventTenantInfo2 = ObjectMapperFactory.getThreadLocal()
                .readValue(eventResource.get(tenant2).getData(), TenantInfoImpl.class);
        TenantInfoImpl t2 = (TenantInfoImpl) admin.tenants().getTenantInfo(tenant2);
        assertEquals(eventTenantInfo2.getLastUpdatedTimestamp(), t2.getLastUpdatedTimestamp());
        assertEquals(eventTenantInfo2.getAdminRoles(), t2.getAdminRoles());
        assertEquals(eventTenantInfo2.getAllowedClusters(), t2.getAllowedClusters());
    }
}
