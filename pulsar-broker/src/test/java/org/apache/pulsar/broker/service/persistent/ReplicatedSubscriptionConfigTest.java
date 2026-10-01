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

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionConfigTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSnapshotCanStartBeforeControllerConstructorReturns() throws Exception {
        conf.setEnableReplicatedSubscriptions(true);
        String remoteCluster = BrokerTestUtil.newUniqueName("snapshot-init-remote");
        String tenant = BrokerTestUtil.newUniqueName("snapshot-init");
        String namespace = tenant + "/ns";
        String topicName = "persistent://" + namespace + "/topic";
        admin.clusters().createCluster(remoteCluster, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).brokerServiceUrl(pulsar.getBrokerServiceUrl()).build());
        admin.tenants().createTenant(tenant, new TenantInfoImpl(Set.of(), Set.of("test", remoteCluster)));
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Set.of("test", remoteCluster));

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(false).create();
        producer.newMessage().replicationClusters(List.of("test")).value("data-before-activation").send();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName("sub").replicateSubscriptionState(true).subscribe();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().orElseThrow();
        assertThat(topic.getLastMaxReadPositionMovedForwardTimestamp()).isPositive();
        topic.getReplicatedSubscriptionController().orElseThrow().close();
        topic.removeReplicator(remoteCluster).get(10, TimeUnit.SECONDS);

        // Exercise the real topic, marker publish and metrics with the earliest legal first scheduler tick.
        // Running inline makes the constructor interleaving deterministic without mocking its collaborators.
        @Cleanup("shutdownNow")
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1) {
            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                         TimeUnit unit) {
                command.run();
                return super.scheduleAtFixedRate(command, 1, 1, TimeUnit.DAYS);
            }
        };
        long entriesBeforeSnapshot = topic.getManagedLedger().getNumberOfEntries();
        ReplicatedSubscriptionsController controller =
                new ReplicatedSubscriptionsController(topic, "test", executor);
        try {
            assertThat(controller.pendingSnapshots()).hasSize(1);
            Awaitility.await().untilAsserted(() -> assertThat(topic.getManagedLedger().getNumberOfEntries())
                    .isGreaterThan(entriesBeforeSnapshot));
        } finally {
            controller.pendingSnapshots().keySet().forEach(controller::snapshotCompleted);
            controller.close();
        }
    }

    @Test
    public void createReplicatedSubscription() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("createReplicatedSubscription");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .replicateSubscriptionState(true)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub1").isReplicated());

        admin.topics().unload(topic);

        // Check that subscription is still marked replicated after reloading
        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub1").isReplicated());
    }

    @Test
    public void upgradeToReplicatedSubscription() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("upgradeToReplicatedSubscription");

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }

    @Test
    public void upgradeToReplicatedSubscriptionAfterRestart() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("upgradeToReplicatedSubscriptionAfterRestart");

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();

        admin.topics().unload(topic);

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }

    @Test
    public void testDisableReplicatedSubscriptions() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(false);
        String topic = BrokerTestUtil.newUniqueName("disableReplicatedSubscriptions");
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }
}
