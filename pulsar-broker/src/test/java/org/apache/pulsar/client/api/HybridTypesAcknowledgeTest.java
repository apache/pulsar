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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class HybridTypesAcknowledgeTest  extends TestRetrySupport {

    protected static final String DEFAULT_NS = "public/default";
    protected String clusterName = "c1";
    protected LocalBookkeeperEnsemble bkEnsemble;
    protected ServiceConfiguration conf = new ServiceConfiguration();
    protected PulsarService pulsar;
    protected URL url;
    protected PulsarAdmin admin;
    protected PulsarClientImpl client;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        incrementSetupNumber();
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        // Start broker.
        setupBrokers();
        // Create default NS.
        admin.clusters().createCluster(clusterName, new ClusterDataImpl());
        admin.tenants().createTenant(NamespaceName.get(DEFAULT_NS).getTenant(),
                new TenantInfoImpl(Collections.emptySet(), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(DEFAULT_NS);
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
        if (client != null) {
            client.close();
            client = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }
        // Reset configs.
        conf = new ServiceConfiguration();
    }

    protected void setupBrokers() throws Exception {
        doInitConf();
        // Start broker.
        pulsar = new PulsarService(conf);
        pulsar.start();
        url = new URL(pulsar.getWebServiceAddress());
        admin = PulsarAdmin.builder().serviceHttpUrl(url.toString()).build();
        client =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
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
    public void testBacklogWithHybridAcknowledgement() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String mlName = TopicName.get(topic).getPersistenceNamingEncoding();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(500);
        config.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerFactory factory = pulsar.getDefaultManagedLedgerFactory();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(mlName, config);
        Producer<String> producer = client.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic)
                .create();
        admin.topics().createSubscription(topic, "s1", MessageId.earliest);
        admin.topics().createSubscription(topic, "s2", MessageId.earliest);
        admin.topics().createSubscription(topic, "s3", MessageId.earliest);

        Consumer<String> c1 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s1").subscribe();
        Consumer<String> c2 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s2").subscribe();
        Consumer<String> c3 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s3").subscribe();
        Consumer<String> c4 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s4").subscribe();

        Random random = new Random();
        List<Message<String>> cachedMessagesInMem2 = new ArrayList<>();
        List<Message<String>> cachedMessagesInMem3 = new ArrayList<>();
        List<Message<String>> cachedMessagesInMem4 = new ArrayList<>();
        List<String> randomlyAcked = new ArrayList<>();

        for (int i = 0; i < 400; i++) {
            producer.send("message-" + i);
        }

        for (int i = 0; i < 400; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
            if (random.nextBoolean()) {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                c2.acknowledge(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                c3.acknowledge(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                c4.acknowledge(m4);
                randomlyAcked.add(m4.getValue());
            } else {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem2.add(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem3.add(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem4.add(m4);
            }
        }

        for (int i = 400; i < 900; i++) {
            producer.send("message-" + i);
        }

        for (int i = 400; i < 600; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
            if (random.nextBoolean()) {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                c2.acknowledge(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                c3.acknowledge(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                c4.acknowledge(m4);
                randomlyAcked.add(m4.getValue());
            } else {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem2.add(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem3.add(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem4.add(m4);
            }
        }

        log.info("s3 cached unacked messages: " + cachedMessagesInMem3.size() + ", acked list: "
                + randomlyAcked.size() + ", acked set: " + new HashSet<>(randomlyAcked).size());
        assertEquals(randomlyAcked.size(), new HashSet<>(randomlyAcked).size());

        for (int i = 600; i < 900; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
        }

        Awaitility.await().untilAsserted(() -> {
            Map<String, ? extends SubscriptionStats> statsPrecise =
                    admin.topics().getStats(topic, true).getSubscriptions();
            assertEquals(statsPrecise.get("s2").getMsgBacklog(), 100 + cachedMessagesInMem2.size());
            assertEquals(statsPrecise.get("s3").getMsgBacklog(), 100 + cachedMessagesInMem3.size());
            assertEquals(statsPrecise.get("s4").getMsgBacklog(), 100 + cachedMessagesInMem4.size());
        });

        // c2: ack all messages that un-acked
        // c3: seek to skip all messages that un0acked.
        // c4: cumulative to skip all messages that un-acked.
        // c2.
        for (Message<String> m2 : cachedMessagesInMem2) {
            c2.acknowledge(m2);
        }
        // c3.
        MessageIdAdv messageIdAdv =
                (MessageIdAdv) cachedMessagesInMem3.get(cachedMessagesInMem3.size() - 1).getMessageId();
        Position pos = ml.getNextValidPosition(PositionFactory.create(messageIdAdv.getLedgerId(),
                messageIdAdv.getEntryId()));
        c3.seek(new MessageIdImpl(pos.getLedgerId(), pos.getEntryId(), -1));
        c3.acknowledgeCumulative(cachedMessagesInMem3.get(cachedMessagesInMem3.size() - 1));
        // c4.
        c4.acknowledgeCumulative(cachedMessagesInMem3.get(cachedMessagesInMem3.size() - 1));

        // Verify: the backlog is the same as precise one.
        verifyBacklogSameAsPrecise(topic);

        // cleanup.
        c1.close();
        c2.close();
        c3.close();
        c4.close();
        producer.close();
        admin.topics().delete(topic);
    }

    @DataProvider
    public Object[][] resetDirection() {
        return  new Object[][]{
            {"forward"},
            {"stationary"},
            {"backward"}
        };
    }

    @Test(dataProvider = "resetDirection")
    public void testResetCursorAndClearFollowingIndividualAcks(String resetDirection) throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        String mlName = TopicName.get(topic).getPersistenceNamingEncoding();
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(500);
        config.setMinimumRolloverTime(1, TimeUnit.SECONDS);
        ManagedLedgerFactory factory = pulsar.getDefaultManagedLedgerFactory();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open(mlName, config);
        Producer<String> producer = client.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic)
                .create();
        admin.topics().createSubscription(topic, "s1", MessageId.earliest);
        admin.topics().createSubscription(topic, "s2", MessageId.earliest);
        admin.topics().createSubscription(topic, "s3", MessageId.earliest);
        admin.topics().createSubscription(topic, "s4", MessageId.earliest);
        Consumer<String> c1 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s1").subscribe();
        Consumer<String> c2 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s2").subscribe();
        Consumer<String> c3 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s3").subscribe();
        Consumer<String> c4 = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("s4").subscribe();

        Random random = new Random();
        List<Message<String>> cachedMessagesInMem2 = new ArrayList<>();
        List<Message<String>> cachedMessagesInMem3 = new ArrayList<>();
        List<Message<String>> cachedMessagesInMem4 = new ArrayList<>();
        List<String> randomlyAcked = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            producer.send("message-" + i);
        }

        // Continuously acknowledges 600 messages.
        MessageId messageId200 = null;
        for (int i = 0; i < 600; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
            Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
            c2.acknowledge(m2);
            Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
            c3.acknowledge(m3);
            Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
            c4.acknowledge(m4);
            randomlyAcked.add(m4.getValue());
            if (i == 200) {
                messageId200 = m3.getMessageId();
            }
        }

        // Makes acknowledge holes between 601~900.
        for (int i = 600; i < 900; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
            // The condition "Method…"
            if (random.nextBoolean() && i % 100 != 0) {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                c2.acknowledge(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                c3.acknowledge(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                c4.acknowledge(m4);
                randomlyAcked.add(m4.getValue());
            } else {
                Message<String> m2 = c2.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem2.add(m2);
                Message<String> m3 = c3.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem3.add(m3);
                Message<String> m4 = c4.receive(2, TimeUnit.SECONDS);
                cachedMessagesInMem4.add(m4);
            }
        }

        Awaitility.await().untilAsserted(() -> {
            Map<String, ? extends SubscriptionStats> statsPrecise =
                    admin.topics().getStats(topic, true).getSubscriptions();
            assertEquals(statsPrecise.get("s2").getMsgBacklog(), 100 + cachedMessagesInMem2.size());
            assertEquals(statsPrecise.get("s3").getMsgBacklog(), 100 + cachedMessagesInMem3.size());
            assertEquals(statsPrecise.get("s4").getMsgBacklog(), 100 + cachedMessagesInMem4.size());
        });

        log.info("subscription cached unacked messages: " + cachedMessagesInMem3.size() + ", acked list: "
                + randomlyAcked.size() + ", acked set: " + new HashSet<>(randomlyAcked).size());
        assertEquals(randomlyAcked.size(), new HashSet<>(randomlyAcked).size());


        // c1: continuously ack all messages.
        // c2: ack all messages that un-acked
        // c3: seek to skip all messages that un0acked.
        // c4: cumulative to skip all messages that un-acked.

        // c1.
        for (int i = 900; i < 1000; i++) {
            Message<String> m1 = c1.receive(2, TimeUnit.SECONDS);
            c1.acknowledge(m1);
        }

        // c2.
        for (Message<String> m2 : cachedMessagesInMem2) {
            c2.acknowledge(m2);
        }

        // c3.
        ManagedCursorImpl cursor3 = (ManagedCursorImpl) ml.getCursors().get("s3");
        assertTrue(cachedMessagesInMem3.size() >= 2);
        MessageId targetMessageId;
        if ("forward".equals(resetDirection)) {
            targetMessageId = cachedMessagesInMem3.get(cachedMessagesInMem3.size() >> 1).getMessageId();
        } else if ("stationary".equals(resetDirection)) {
            Position mdPosition = ml.getCursors().get("s3").getMarkDeletedPosition();
            targetMessageId = new MessageIdImpl(mdPosition.getLedgerId(), mdPosition.getEntryId(), -1);
        } else {
            targetMessageId = messageId200;
        }
        log.info("cursor3 before seek. md-pos: {}, individualAcks: {}, targetPosition: {}, backlog: {}",
                cursor3.getMarkDeletedPosition(), cursor3.getIndividuallyDeletedMessages(), targetMessageId,
                cursor3.getNumberOfEntriesInBacklog(false));
        c3.seek(targetMessageId);
        log.info("cursor3 after seek. md-pos: {}, individualAcks: {}, targetPosition: {}, backlog: {}",
                cursor3.getMarkDeletedPosition(), cursor3.getIndividuallyDeletedMessages(), targetMessageId,
                cursor3.getNumberOfEntriesInBacklog(false));

        // c4.
        c4.acknowledgeCumulative(targetMessageId);

        // Verify: the backlog is the same as precise one.
        verifyBacklogSameAsPrecise(topic);

        // cleanup.
        c1.close();
        c2.close();
        c3.close();
        c4.close();
        producer.close();
        admin.topics().delete(topic);
    }

    private void verifyBacklogSameAsPrecise(String topic) {
        Awaitility.await().pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ? extends SubscriptionStats> stats = admin.topics().getStats(topic).getSubscriptions();
            Map<String, ? extends SubscriptionStats> statsPrecise =
                    admin.topics().getStats(topic, true).getSubscriptions();
            for (Map.Entry<String, ? extends SubscriptionStats> item : stats.entrySet()) {
                long preciseBacklog = statsPrecise.get(item.getKey()).getMsgBacklog();
                long backlog = item.getValue().getMsgBacklog();
                log.info("subscription: " + item.getKey() + ", preciseBacklog: "
                        + preciseBacklog + ", backlog: " + backlog);
                assertEquals(backlog, preciseBacklog);
            }
        });
    }
}
