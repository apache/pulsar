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

import com.google.common.collect.Lists;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ShadowTopicRealBkTest {

    private static final String cluster = "test";
    private final int zkPort = PortManager.nextLockedFreePort();
    private final LocalBookkeeperEnsemble bk = new LocalBookkeeperEnsemble(2, zkPort, PortManager::nextLockedFreePort);
    private PulsarService pulsar;
    private PulsarAdmin admin;

    @BeforeClass
    public void setup() throws Exception {
        bk.start();
        final var config = new ServiceConfiguration();
        config.setClusterName(cluster);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:localhost:" + zkPort);
        pulsar = new PulsarService(config);
        pulsar.start();
        admin = pulsar.getAdminClient();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl()).build());
        admin.tenants().createTenant("public", TenantInfo.builder().allowedClusters(Set.of(cluster)).build());
        admin.namespaces().createNamespace("public/default");
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (pulsar != null) {
            pulsar.close();
        }
        bk.stop();
    }

    @Test
    public void testReadFromStorage() throws Exception {
        final var sourceTopic = TopicName.get("test-read-from-source" + UUID.randomUUID()).toString();
        final var shadowTopic = sourceTopic + "-shadow";

        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(()->{
            final var sourcePersistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicIfExists(sourceTopic).get().orElseThrow();
            final var replicator = (ShadowReplicator) sourcePersistentTopic.getShadowReplicators().get(shadowTopic);
            Assert.assertNotNull(replicator);
            Assert.assertEquals(String.valueOf(replicator.getState()), "Started");
        });

        final var client = pulsar.getClient();
        // When the message was sent, there is no cursor, so it will read from the cache
        final var producer = client.newProducer().topic(sourceTopic).create();
        producer.send("message".getBytes());
        // 1. Verify RangeEntryCacheImpl#readFromStorage
        final var consumer = client.newConsumer().topic(shadowTopic).subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        final var msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(msg);
        Assert.assertEquals(msg.getValue(), "message".getBytes());

        // 2. Verify EntryCache#asyncReadEntry
        final var shadowManagedLedger = ((PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get()
                .orElseThrow()).getManagedLedger();
        Assert.assertTrue(shadowManagedLedger instanceof ShadowManagedLedgerImpl);
        shadowManagedLedger.getEarliestMessagePublishTimeInBacklog().get(3, TimeUnit.SECONDS);
    }
}
