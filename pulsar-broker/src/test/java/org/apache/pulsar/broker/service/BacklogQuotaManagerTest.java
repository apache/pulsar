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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Sets;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BacklogQuotaManagerTest {
    PulsarService pulsar;
    ServiceConfiguration config;

    URL adminUrl;
    PulsarAdmin admin;

    LocalBookkeeperEnsemble bkEnsemble;

    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 3;
    private static final int MAX_ENTRIES_PER_LEDGER = 5;

    @DataProvider(name = "backlogQuotaSizeGB")
    public Object[][] backlogQuotaSizeGB() {
        return new Object[][] { { true }, { false } };
    }

    @BeforeMethod
    void setup() throws Exception {
        try {
            // start local bookie and zookeeper
            bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
            bkEnsemble.start();

            // start pulsar service
            config = new ServiceConfiguration();
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setClusterName("usc");
            config.setBrokerShutdownTimeoutMs(0L);
            config.setBrokerServicePort(Optional.of(0));
            config.setAuthorizationEnabled(false);
            config.setAuthenticationEnabled(false);
            config.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            config.setManagedLedgerMaxEntriesPerLedger(MAX_ENTRIES_PER_LEDGER);
            config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
            config.setAllowAutoTopicCreationType("non-partitioned");

            pulsar = new PulsarService(config);
            pulsar.start();

            adminUrl = new URL("http://127.0.0.1" + ":" + pulsar.getListenPortHTTP().get());
            admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl.toString()).build();

            admin.clusters().createCluster("usc", ClusterData.builder().serviceUrl(adminUrl.toString()).build());
            admin.tenants().createTenant("prop",
                    new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("usc")));
            admin.namespaces().createNamespace("prop/ns-quota");
            admin.namespaces().setNamespaceReplicationClusters("prop/ns-quota", Sets.newHashSet("usc"));
            admin.namespaces().createNamespace("prop/quotahold");
            admin.namespaces().setNamespaceReplicationClusters("prop/quotahold", Sets.newHashSet("usc"));
            admin.namespaces().createNamespace("prop/quotaholdasync");
            admin.namespaces().setNamespaceReplicationClusters("prop/quotaholdasync", Sets.newHashSet("usc"));
        } catch (Throwable t) {
            LOG.error("Error setting up broker test", t);
            fail("Broker test setup failed");
        }
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        try {
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (pulsar != null) {
                pulsar.close();
                pulsar = null;
            }
            if (bkEnsemble != null) {
                bkEnsemble.stop();
                bkEnsemble = null;
            }
        } catch (Throwable t) {
            LOG.error("Error cleaning up broker test setup state", t);
            fail("Broker test cleanup failed");
        }
    }

    private void rolloverStats() {
        pulsar.getBrokerService().updateRates();
    }

    /**
     * Readers should not effect backlog quota
     */
    @Test
    public void testBacklogQuotaWithReader() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build());
        try (PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS).build();) {
            final String topic1 = "persistent://prop/ns-quota/topic1";
            final int numMsgs = 20;

            Reader<byte[]> reader = client.newReader().topic(topic1).receiverQueueSize(1).startMessageId(MessageId.latest).create();

            org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);

            byte[] content = new byte[1024];
            for (int i = 0; i < numMsgs; i++) {
                content[0] = (byte) (content[0] + 1);
                MessageId msgId = producer.send(content);
            }

            Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

            rolloverStats();

            TopicStats stats = admin.topics().getStats(topic1);

            // overall backlogSize should be zero because we only have readers
            assertEquals(stats.getBacklogSize(), 0, "backlog size is [" + stats.getBacklogSize() + "]");

            // non-durable mes should still
            assertEquals(stats.getSubscriptions().size(), 1);
            long nonDurableSubscriptionBacklog = stats.getSubscriptions().values().iterator().next().getMsgBacklog();
            assertEquals(nonDurableSubscriptionBacklog, MAX_ENTRIES_PER_LEDGER,
              "non-durable subscription backlog is [" + nonDurableSubscriptionBacklog + "]"); ;

            try {
                // try to send over backlog quota and make sure it fails
                for (int i = 0; i < numMsgs; i++) {
                    content[0] = (byte) (content[0] + 1);
                    MessageId msgId = producer.send(content);
                }
            } catch (PulsarClientException ce) {
                fail("Should not have gotten exception: " + ce.getMessage());
            }

            // TODO in theory there shouldn't be any ledgers left if we are using readers.
            //  However, trimming of ledgers are piggy packed onto ledger operations.
            //  So if there isn't new data coming in, trimming never occurs.
            //  We need to trigger trimming on a schedule to actually delete all remaining ledgers
            Awaitility.await().untilAsserted(() -> {
                // make sure ledgers are trimmed
                PersistentTopicInternalStats internalStats =
                        admin.topics().getInternalStats(topic1, false);

                // check there is only one ledger left
                assertEquals(internalStats.ledgers.size(), 1);

                // check if its the expected ledger id given MAX_ENTRIES_PER_LEDGER
                assertEquals(internalStats.ledgers.get(0).ledgerId, (2 * numMsgs / MAX_ENTRIES_PER_LEDGER) - 1);
            });

            // check reader can still read with out error

            while (true) {
                Message<byte[]> msg = reader.readNext(5, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                LOG.info("msg read: {} - {}", msg.getMessageId(), msg.getData()[0]);
            }
        }
    }

    @Test
    public void testTriggerBacklogQuotaSizeWithReader() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build());
        try (PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS).build();) {
            final String topic1 = "persistent://prop/ns-quota/topic1" + UUID.randomUUID();
            final int numMsgs = 20;
            Reader<byte[]> reader = client.newReader().topic(topic1).receiverQueueSize(1).startMessageId(MessageId.latest).create();
            Producer<byte[]> producer = createProducer(client, topic1);
            byte[] content = new byte[1024];
            for (int i = 0; i < numMsgs; i++) {
                content[0] = (byte) (content[0] + 1);
                producer.send(content);
            }
            Thread.sleep(TIME_TO_CHECK_BACKLOG_QUOTA * 1000);
            admin.brokers().backlogQuotaCheck();
            rolloverStats();
            TopicStats stats = admin.topics().getStats(topic1);
            // overall backlogSize should be zero because we only have readers
            assertEquals(stats.getBacklogSize(), 0, "backlog size is [" + stats.getBacklogSize() + "]");
            // non-durable mes should still
            assertEquals(stats.getSubscriptions().size(), 1);
            long nonDurableSubscriptionBacklog = stats.getSubscriptions().values().iterator().next().getMsgBacklog();
            assertEquals(nonDurableSubscriptionBacklog, MAX_ENTRIES_PER_LEDGER,
              "non-durable subscription backlog is [" + nonDurableSubscriptionBacklog + "]"); ;
            try {
                // try to send over backlog quota and make sure it fails
                for (int i = 0; i < numMsgs; i++) {
                    content[0] = (byte) (content[0] + 1);
                    producer.send(content);
                }
            } catch (PulsarClientException ce) {
                fail("Should not have gotten exception: " + ce.getMessage());
            }

            Awaitility.await().untilAsserted(() -> {
                // make sure ledgers are trimmed
                PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic1, false);

                // check there is only one ledger left
                assertEquals(internalStats.ledgers.size(), 1);

                // check if its the expected ledger id given MAX_ENTRIES_PER_LEDGER
                assertEquals(internalStats.ledgers.get(0).ledgerId, (2 * numMsgs / MAX_ENTRIES_PER_LEDGER) - 1);
            });
            // check reader can still read with out error

            while (true) {
                Message<byte[]> msg = reader.readNext(5, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                LOG.info("msg read: {} - {}", msg.getMessageId(), msg.getData()[0]);
            }
            producer.close();
            reader.close();
        }
    }

    /**
     * Time based backlog quota won't affect reader since broker doesn't keep track of consuming position for reader
     * and can't do message age check against the quota.
     * @throws Exception
     */
    @Test
    public void testTriggerBacklogTimeQuotaWithReader() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build());
        try (PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS).build();) {
            final String topic1 = "persistent://prop/ns-quota/topic2" + UUID.randomUUID();
            final int numMsgs = 9;
            Reader<byte[]> reader = client.newReader().topic(topic1).receiverQueueSize(1).startMessageId(MessageId.latest).create();
            Producer<byte[]> producer = createProducer(client, topic1);
            byte[] content = new byte[1024];
            for (int i = 0; i < numMsgs; i++) {
                content[0] = (byte) (content[0] + 1);
                producer.send(content);
            }
            Thread.sleep(TIME_TO_CHECK_BACKLOG_QUOTA * 1000);
            admin.brokers().backlogQuotaCheck();
            rolloverStats();
            TopicStats stats = admin.topics().getStats(topic1);
            // overall backlogSize should be zero because we only have readers
            assertEquals(stats.getBacklogSize(), 0, "backlog size is [" + stats.getBacklogSize() + "]");
            // non-durable mes should still
            assertEquals(stats.getSubscriptions().size(), 1);
            long nonDurableSubscriptionBacklog = stats.getSubscriptions().values().iterator().next().getMsgBacklog();
            // non-durable subscription won't trigger the check for time based backlog quota
            // and cause back pressure action to be token. Since broker don't keep track consuming position for reader.
            assertEquals(nonDurableSubscriptionBacklog, numMsgs,
                    "non-durable subscription backlog is [" + nonDurableSubscriptionBacklog + "]");

            Awaitility.await()
                    .pollDelay(Duration.ofSeconds(TIME_TO_CHECK_BACKLOG_QUOTA))
                    .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
                // make sure ledgers are trimmed
                PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic1, false);

                // check that there are 2 ledgers
                assertEquals(internalStats.ledgers.size(), 2);
            });

            try {
                // try to send over backlog quota and make sure it fails
                for (int i = 0; i < numMsgs; i++) {
                    content[0] = (byte) (content[0] + 1);
                    producer.send(content);
                }
            } catch (PulsarClientException ce) {
                fail("Should not have gotten exception: " + ce.getMessage());
            }

            // check reader can still read without error
            while (true) {
                Message<byte[]> msg = reader.readNext(5, TimeUnit.SECONDS);
                if (msg == null) {
                    break;
                }
                LOG.info("msg read: {} - {}", msg.getMessageId(), msg.getData()[0]);
            }
            producer.close();
            reader.close();
        }
    }

    @Test
    public void testConsumerBacklogEvictionSizeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topic1 = "persistent://prop/ns-quota/topic2";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 20;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.getBacklogSize() < 10 * 1024, "Storage size is [" + stats.getStorageSize() + "]");
    }

    @Test
    public void testConsumerBacklogEvictionTimeQuotaPrecise() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        config.setPreciseTimeBasedBacklogQuotaCheck(true);
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topic1 = "persistent://prop/ns-quota/topic3";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 9;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 9);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 9);

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);
        rolloverStats();

        stats = admin.topics().getStats(topic1);
        // All messages for both subscription should be cleaned up from backlog by backlog monitor task.
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 0);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 0);
        client.close();
    }

    @Test
    public void testConsumerBacklogEvictionTimeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topic1 = "persistent://prop/ns-quota/topic3";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 14;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 14);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 14);

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);
        rolloverStats();

        stats = admin.topics().getStats(topic1);
        // Messages on first 2 ledgers should be expired, backlog is number of
        // message in current ledger which should be 4.
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 4);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 4);
        client.close();
    }

    @Test
    public void testConsumerBacklogEvictionWithAckSizeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).build();

        final String topic1 = "persistent://prop/ns-quota/topic11";
        final String subName1 = "c11";
        final String subName2 = "c21";
        final int numMsgs = 20;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            // only one consumer acknowledges the message
            consumer1.acknowledge(consumer1.receive());
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.getBacklogSize() <= 10 * 1024, "Storage size is [" + stats.getStorageSize() + "]");
    }

    @Test
    public void testConsumerBacklogEvictionWithAckTimeQuotaPrecise() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        config.setPreciseTimeBasedBacklogQuotaCheck(true);
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).build();

        final String topic1 = "persistent://prop/ns-quota/topic12";
        final String subName1 = "c11";
        final String subName2 = "c21";
        final int numMsgs = 9;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];

        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 9);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 9);

        consumer1.redeliverUnacknowledgedMessages();
        for (int i = 0; i < numMsgs; i++) {
            // only one consumer acknowledges the message
            consumer1.acknowledge(consumer1.receive());
        }

        Thread.sleep(1000);
        rolloverStats();
        stats = admin.topics().getStats(topic1);
        // sub1 has empty backlog as it acked all messages
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 0);
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 9);

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);
        rolloverStats();

        stats = admin.topics().getStats(topic1);
        // sub2 has empty backlog because it's backlog get cleaned up by backlog quota monitor task
        assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 0);
        client.close();
    }

    private Producer<byte[]> createProducer(PulsarClient client, String topic)
            throws PulsarClientException {
        return client.newProducer()
                .enableBatching(false)
                .sendTimeout(2, TimeUnit.SECONDS)
                .topic(topic)
                .create();
    }

    @Test
    public void testConsumerBacklogEvictionWithAckTimeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitTime(2 * TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).build();

        final String topic1 = "persistent://prop/ns-quota/topic12";
        final String subName1 = "c11";
        final String subName2 = "c21";
        final int numMsgs = 14;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];

        List<Message<byte[]>> messagesToAcknowledge = new ArrayList<>();

        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            messagesToAcknowledge.add(consumer1.receive());
            consumer2.receive();
        }

        {
            TopicStats stats = admin.topics().getStats(topic1);
            assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 14);
            assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 14);
        }

        for (int i = 0; i < numMsgs; i++) {
            // pause before acknowledging the 11. message so that 2 first ledgers (5 msgs/ledger) will expire before the
            // last ledger
            if (i == 10) {
                Thread.sleep(TIME_TO_CHECK_BACKLOG_QUOTA * 1000L);
            }
            // only one consumer acknowledges the message
            consumer1.acknowledge(messagesToAcknowledge.get(i));
        }

        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    rolloverStats();
                    TopicStats stats = admin.topics().getStats(topic1);
                    // sub1 has empty backlog as it acked all messages
                    assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 0);
                    assertEquals(stats.getSubscriptions().get(subName2).getMsgBacklog(), 14);
                });

        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(4 * TIME_TO_CHECK_BACKLOG_QUOTA))
                .untilAsserted(() -> {
                    // Messages on first 2 ledgers should be expired, backlog is number of
                    // message in current ledger which should be 4.
                    long msgBacklog = admin.topics().getStats(topic1).getSubscriptions().get(subName2).getMsgBacklog();
                    // TODO: for some reason the backlog size is sometimes off by one
                    // Internally there's a method `long getNumberOfEntriesInBacklog(boolean getPreciseBacklog)`
                    // on org.apache.pulsar.broker.service.Subscription interface
                    // the `boolean getPreciseBacklog` parameter indicates that the backlog size isn't accurate
                    assertEquals(msgBacklog, 4, 1);
                });
    }

    @Test
    public void testConcurrentAckAndEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());

        final String topic1 = "persistent://prop/ns-quota/topic12";
        final String subName1 = "c12";
        final String subName2 = "c22";
        final int numMsgs = 20;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer1 = client2.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
                    byte[] content = new byte[1024];
                    for (int i = 0; i < numMsgs; i++) {
                        producer.send(content);
                    }
                    producer.close();
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread consumerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < numMsgs; i++) {
                        // only one consumer acknowledges the message
                        consumer1.acknowledge(consumer1.receive());
                        consumer2.receive();
                    }
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        producerThread.start();
        consumerThread.start();

        // test hangs without timeout since there is nothing to consume due to eviction
        counter.await(20, TimeUnit.SECONDS);
        assertFalse(gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.getBacklogSize() <= 10 * 1024, "Storage size is [" + stats.getStorageSize() + "]");
    }

    @Test
    public void testNoEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());

        final String topic1 = "persistent://prop/ns-quota/topic13";
        final String subName1 = "c13";
        final String subName2 = "c23";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        final Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        final Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        @Cleanup
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client2, topic1);
                    byte[] content = new byte[1024];
                    for (int i = 0; i < numMsgs; i++) {
                        producer.send(content);
                    }
                    producer.close();
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread consumerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < numMsgs; i++) {
                        consumer1.acknowledge(consumer1.receive());
                        consumer2.acknowledge(consumer2.receive());
                    }
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        producerThread.start();
        consumerThread.start();
        counter.await();
        assertFalse(gotException.get());
    }

    @Test
    public void testEvictionMulti() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                BacklogQuota.builder()
                        .limitSize(15 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                        .build());

        final String topic1 = "persistent://prop/ns-quota/topic14";
        final String subName1 = "c14";
        final String subName2 = "c24";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(4);
        final CountDownLatch counter = new CountDownLatch(4);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        final Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        final Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        @Cleanup
        final PulsarClient client3 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        @Cleanup
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Thread producerThread1 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client2, topic1);
                    byte[] content = new byte[1024];
                    for (int i = 0; i < numMsgs; i++) {
                        producer.send(content);
                    }
                    producer.close();
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread producerThread2 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client3, topic1);
                    byte[] content = new byte[1024];
                    for (int i = 0; i < numMsgs; i++) {
                        producer.send(content);
                    }
                    producer.close();
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread consumerThread1 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < numMsgs * 2; i++) {
                        consumer1.acknowledge(consumer1.receive());
                    }
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread consumerThread2 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < numMsgs * 2; i++) {
                        consumer2.acknowledge(consumer2.receive());
                    }
                } catch (Exception e) {
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        producerThread1.start();
        producerThread2.start();
        consumerThread1.start();
        consumerThread2.start();
        counter.await(20, TimeUnit.SECONDS);
        assertFalse(gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.getBacklogSize() <= 15 * 1024, "Storage size is [" + stats.getStorageSize() + "]");
    }

    @Test
    public void testAheadProducerOnHold() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/hold";
        final String subName1 = "c1hold";
        final int numMsgs = 10;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i <= numMsgs; i++) {
            try {
                producer.send(content);
                LOG.info("sent [{}]", i);
            } catch (PulsarClientException.TimeoutException cte) {
                // producer close may cause a timeout on send
                LOG.info("timeout on [{}]", i);
            }
        }

        for (int i = 0; i < numMsgs; i++) {
            consumer.receive();
            LOG.info("received [{}]", i);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();
        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getPublishers().size(), 0,
                "Number of producers on topic " + topic1 + " are [" + stats.getPublishers().size() + "]");
    }

    @Test
    public void testAheadProducerOnHoldTimeout() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/holdtimeout";
        final String subName1 = "c1holdtimeout";
        boolean gotException = false;

        client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            fail("backlog quota did not exceed");
        } catch (PulsarClientException.TimeoutException te) {
            gotException = true;
        }

        assertTrue(gotException, "timeout did not occur");
    }

    @Test
    public void testProducerException() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build());
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/except";
        final String subName1 = "c1except";
        boolean gotException = false;

        client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        assertTrue(gotException, "backlog exceeded exception did not occur");
    }

    @Test
    public void testProducerExceptionAndThenUnblockSizeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build());
        @Cleanup
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/exceptandunblock";
        final String subName1 = "c1except";
        boolean gotException = false;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        assertTrue(gotException, "backlog exceeded exception did not occur");
        // now remove backlog and ensure that producer is unblocked;

        TopicStats stats = admin.topics().getStats(topic1);
        int backlog = (int) stats.getSubscriptions().get(subName1).getMsgBacklog();

        for (int i = 0; i < backlog; i++) {
            Message<?> msg = consumer.receive();
            consumer.acknowledge(msg);
        }
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        // publish should work now
        Exception sendException = null;
        gotException = false;
        try {
            for (int i = 0; i < 5; i++) {
                producer.send(content);
            }
        } catch (Exception e) {
            gotException = true;
            sendException = e;
        }
        assertFalse(gotException, "unable to publish due to " + sendException);
    }

    @Test
    public void testProducerExceptionAndThenUnblockTimeQuotaPrecise() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        config.setPreciseTimeBasedBacklogQuotaCheck(true);
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/exceptandunblock2";
        final String subName1 = "c1except";
        boolean gotException = false;
        int numMsgs = 9;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        assertTrue(gotException, "backlog exceeded exception did not occur");

        // now remove backlog and ensure that producer is unblocked;
        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            consumer.acknowledge(consumer.receive());
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);
        rolloverStats();
        stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 0);
        // publish should work now
        Exception sendException = null;
        gotException = false;
        try {
            for (int i = 0; i < 5; i++) {
                producer.send(content);
            }
        } catch (Exception e) {
            gotException = true;
            sendException = e;
        }
        assertFalse(gotException, "unable to publish due to " + sendException);
        client.close();
    }

    @Test
    public void testProducerExceptionAndThenUnblockTimeQuota() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                Maps.newHashMap());
        admin.namespaces().setBacklogQuota("prop/quotahold",
                BacklogQuota.builder()
                        .limitTime(TIME_TO_CHECK_BACKLOG_QUOTA)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                        .build(), BacklogQuota.BacklogQuotaType.message_age);
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/exceptandunblock2";
        final String subName1 = "c1except";
        boolean gotException = false;
        int numMsgs = 14;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = createProducer(client, topic1);
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        assertTrue(gotException, "backlog exceeded exception did not occur");

        // now remove backlog and ensure that producer is unblocked;
        TopicStats stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), numMsgs);

        for (int i = 0; i < numMsgs; i++) {
            consumer.acknowledge(consumer.receive());
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA * 2) * 1000);
        rolloverStats();
        stats = admin.topics().getStats(topic1);
        assertEquals(stats.getSubscriptions().get(subName1).getMsgBacklog(), 0);
        // publish should work now
        Exception sendException = null;
        gotException = false;
        try {
            for (int i = 0; i < 5; i++) {
                producer.send(content);
            }
        } catch (Exception e) {
            gotException = true;
            sendException = e;
        }
        assertFalse(gotException, "unable to publish due to " + sendException);
        client.close();
    }

    @Test(dataProvider = "backlogQuotaSizeGB")
    public void testBacklogQuotaInGB(boolean backlogQuotaSizeGB) throws Exception {

        pulsar.close();
        long backlogQuotaByte = 10 * 1024;
        if (backlogQuotaSizeGB) {
            config.setBacklogQuotaDefaultLimitGB(((double) backlogQuotaByte) / BacklogQuotaImpl.BYTES_IN_GIGABYTE);
        } else {
            config.setBacklogQuotaDefaultLimitBytes(backlogQuotaByte);
        }
        config.setBacklogQuotaDefaultRetentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction);
        pulsar = new PulsarService(config);
        pulsar.start();

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topic1 = "persistent://prop/ns-quota/topic2";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 20;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = createProducer(client, topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.getBacklogSize() < 10 * 1024, "Storage size is [" + stats.getStorageSize() + "]");
    }
    private static final Logger LOG = LoggerFactory.getLogger(BacklogQuotaManagerTest.class);
}
