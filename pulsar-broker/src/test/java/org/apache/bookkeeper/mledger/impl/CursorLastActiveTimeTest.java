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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class CursorLastActiveTimeTest extends TestRetrySupport {

    protected static final String DEFAULT_NS = "public/default";

    protected String clusterName = "c1";

    protected LocalBookkeeperEnsemble bkEnsemble;

    protected ServiceConfiguration conf = new ServiceConfiguration();

    protected PulsarService pulsar1;
    protected URL url1;
    protected PulsarAdmin admin1;
    protected PulsarClientImpl clientWithHttpLookup1;
    protected PulsarClientImpl clientWitBinaryLookup1;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        incrementSetupNumber();
        bkEnsemble = new LocalBookkeeperEnsemble(2, 0, () -> 0);
        bkEnsemble.start();
        // Start broker.
        setupBrokers();
        // Create default NS.
        admin1.clusters().createCluster(clusterName, new ClusterDataImpl());
        admin1.tenants().createTenant(NamespaceName.get(DEFAULT_NS).getTenant(),
                new TenantInfoImpl(Collections.emptySet(), Sets.newHashSet(clusterName)));
        admin1.namespaces().createNamespace(DEFAULT_NS);
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        cleanupBrokers();
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }

    protected void cleanupBrokers() throws Exception {
        // Cleanup broker2.
        if (clientWithHttpLookup1 != null) {
            clientWithHttpLookup1.close();
            clientWithHttpLookup1 = null;
        }
        if (clientWitBinaryLookup1 != null) {
            clientWitBinaryLookup1.close();
            clientWitBinaryLookup1 = null;
        }
        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }
        // Reset configs.
        conf = new ServiceConfiguration();
    }

    protected void setupBrokers() throws Exception {
        doInitConf();
        // Start broker.
        pulsar1 = new PulsarService(conf);
        pulsar1.start();
        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        clientWithHttpLookup1 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar1.getWebServiceAddress()).build();
        clientWitBinaryLookup1 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl()).build();
    }

    protected void doInitConf() {
        conf.setClusterName(clusterName);
        conf.setAdvertisedAddress("localhost");
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        conf.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        conf.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/foo");
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerShutdownTimeoutMs(0L);
        conf.setLoadBalancerSheddingEnabled(false);
    }

    @Test
    public void testDeleteInactiveSubscriptionEvenUnloadedTopics() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("public/default/tp");
        String cursorName = "c1";
        admin1.topics().createNonPartitionedTopic(topic);
        Producer<String> producer = clientWitBinaryLookup1.newProducer(Schema.STRING).topic(topic).create();
        admin1.topics().createSubscription(topic, cursorName, MessageId.earliest);
        ConsumerImpl<String> consumer1 = (ConsumerImpl<String>) clientWitBinaryLookup1.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(cursorName)
                .subscribe();
        // Close all consumers.
        consumer1.close();

        // Unload topic, and no more consumers connected.
        Thread.sleep(2000);
        admin1.topics().unload(topic);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopicIfExists(topic).get().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // Verify the subscription will be deleted if the expiration time is reached.
        // We call an internal method to make the expiration time to be 1 second.
        persistentTopic.checkInactiveSubscriptions(1000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml.getCursors().size(), 0);
        });

        // cleanup.
        producer.close();
        admin1.topics().delete(topic, false);
    }

    @Test
    public void testLastActiveTimestamp() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("public/default/tp");
        String cursorName = "c1";
        admin1.topics().createNonPartitionedTopic(topic);
        Producer<String> producer = clientWitBinaryLookup1.newProducer(Schema.STRING).topic(topic).create();
        admin1.topics().createSubscription(topic, cursorName, MessageId.earliest);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar1.getBrokerService().getTopicIfExists(topic).get().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // Cursor initialized.
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.openCursor(cursorName);
        long latestActiveTime1 = cursor.getLastActiveOrInActive();
        assertTrue(latestActiveTime1 < 0);
        assertTrue(System.currentTimeMillis() - Math.abs(latestActiveTime1) < 2000);
        // The lastActive will be changed before persists to metadata store, so it should be persisted.
        assertTrue(ManagedCursorImpl.isLastActivePersisted(latestActiveTime1));

        // Added the first consumer.
        Thread.sleep(2000);
        ConsumerImpl<String> consumer1 = (ConsumerImpl<String>) clientWitBinaryLookup1.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(cursorName)
                .subscribe();
        long latestActiveTime2 = cursor.getLastActiveOrInActive();
        assertTrue(latestActiveTime2 > 0);
        assertTrue(System.currentTimeMillis() - Math.abs(latestActiveTime2) < 2000);
        // Since the inactive state has been persisted, the first active consumer will trigger a new
        // persistence, to avoid the issue that https://github.com/apache/pulsar/pull/22794 mentioned.
        assertTrue(ManagedCursorImpl.isLastActivePersisted(latestActiveTime2));

        // Added the second consumer.
        Thread.sleep(2000);
        Consumer<String> consumer2 = clientWitBinaryLookup1.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(cursorName)
                .subscribe();
        long latestActiveTime3 = cursor.getLastActiveOrInActive();
        assertTrue(latestActiveTime3 > 0);
        assertTrue(System.currentTimeMillis() - Math.abs(latestActiveTime3) < 2000);
        // Since the current state is active, the second active consumer will not trigger a new
        // persistence, to improve performance.
        assertFalse(ManagedCursorImpl.isLastActivePersisted(latestActiveTime3));

        // Acknowledge messages.
        Thread.sleep(2000);
        producer.send("msg-1");
        boolean consumer2Received = true;
        Message<String> msg = consumer2.receive(2, TimeUnit.SECONDS);
        if (msg == null) {
            consumer2Received = false;
            msg = consumer1.receive(2, TimeUnit.SECONDS);
        }
        assertNotNull(msg);
        if (consumer2Received) {
            consumer2.acknowledge(msg);
        } else {
            consumer1.acknowledge(msg);
        }
        long currentTime = System.currentTimeMillis();
        Awaitility.await().untilAsserted(() -> {
            long latestActiveTime4 = cursor.getLastActiveOrInActive();
            assertTrue(latestActiveTime4 > 0);
            assertTrue(currentTime - Math.abs(latestActiveTime4) < 2000);
            // The newest state will be persisted by mark deleting.
            assertTrue(ManagedCursorImpl.isLastActivePersisted(latestActiveTime4));
        });


        // Unload topic.
        Thread.sleep(2000);
        admin1.topics().unload(topic);
        PersistentTopic persistentTopic2 =
                (PersistentTopic) pulsar1.getBrokerService().getTopicIfExists(topic).get().get();
        ManagedLedgerImpl ml2 = (ManagedLedgerImpl) persistentTopic2.getManagedLedger();
        ManagedCursorImpl cursor2 = (ManagedCursorImpl) ml2.openCursor(cursorName);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(consumer1.isConnected());
            assertTrue(consumer2.isConnected());
        });
        long latestActiveTime5 = cursor2.getLastActiveOrInActive();
        assertTrue(latestActiveTime5 > 0);
        assertTrue(System.currentTimeMillis() - Math.abs(latestActiveTime5) < 2000);
        // 1. Topic closing will trigger a new persistence.
        // 2. The registering of the first consumer will trigger a new persistence.
        // 3. The registering of the second consumer will not trigger a new persistence.
        // So the value should not be persisted.
        assertFalse(ManagedCursorImpl.isLastActivePersisted(latestActiveTime5));

        // Close all consumers.
        Thread.sleep(2000);
        consumer1.close();
        consumer2.close();
        long latestActiveTime6 = cursor2.getLastActiveOrInActive();
        assertTrue(latestActiveTime6 < 0);
        assertTrue(System.currentTimeMillis() - Math.abs(latestActiveTime6) < 2000);
        // Closing consumers will not trigger persistence.
        assertFalse(ManagedCursorImpl.isLastActivePersisted(latestActiveTime6));

        // Unload topic, and no more consumers connected.
        Thread.sleep(2000);
        admin1.topics().unload(topic);
        PersistentTopic persistentTopic3 =
                (PersistentTopic) pulsar1.getBrokerService().getTopicIfExists(topic).get().get();
        ManagedLedgerImpl ml3 = (ManagedLedgerImpl) persistentTopic3.getManagedLedger();
        ManagedCursorImpl cursor3 = (ManagedCursorImpl) ml3.openCursor(cursorName);
        long latestActiveTime7 = cursor3.getLastActiveOrInActive();
        assertTrue(latestActiveTime7 < 0);
        assertTrue(Math.abs(latestActiveTime7 - latestActiveTime6) < 2);
        // Closing topics will trigger persistence.
        assertTrue(ManagedCursorImpl.isLastActivePersisted(latestActiveTime7));

        // Verify the subscription will be deleted if the expiration time is reached.
        // We call an internal method to make the expiration time to be 1 second.
        persistentTopic3.checkInactiveSubscriptions(1000);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(ml3.getCursors().size(), 0);
        });

        // cleanup.
        producer.close();
        admin1.topics().delete(topic, false);
    }
}
