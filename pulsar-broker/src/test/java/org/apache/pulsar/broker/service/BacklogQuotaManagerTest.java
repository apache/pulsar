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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.ConfigHelper;
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
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class BacklogQuotaManagerTest {
    PulsarService pulsar;
    ServiceConfiguration config;

    URL adminUrl;
    PulsarAdmin admin;

    LocalBookkeeperEnsemble bkEnsemble;

    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;
    private static final int MAX_ENTRIES_PER_LEDGER = 5;

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
            config.setWebServicePort(Optional.ofNullable(0));
            config.setClusterName("usc");
            config.setBrokerServicePort(Optional.ofNullable(0));
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

            admin.clusters().createCluster("usc", new ClusterData(adminUrl.toString()));
            admin.tenants().createTenant("prop",
                    new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet("usc")));
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

    @AfterMethod
    void shutdown() throws Exception {
        try {
            admin.close();
            pulsar.close();
            bkEnsemble.stop();
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
          ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
          new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_exception));
        try (PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS).build();) {
            final String topic1 = "persistent://prop/ns-quota/topic1";
            final int numMsgs = 20;

            Reader<byte[]> reader = client.newReader().topic(topic1).receiverQueueSize(1).startMessageId(MessageId.latest).create();

            org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1).sendTimeout(2, TimeUnit.SECONDS).create();

            byte[] content = new byte[1024];
            for (int i = 0; i < numMsgs; i++) {
                content[0] = (byte) (content[0] + 1);
                MessageId msgId = producer.send(content);
            }

            Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

            rolloverStats();

            TopicStats stats = admin.topics().getStats(topic1);

            // overall backlogSize should be zero because we only have readers
            assertEquals(stats.backlogSize, 0, "backlog size is [" + stats.backlogSize + "]");

            // non-durable mes should still
            assertEquals(stats.subscriptions.size(), 1);
            long nonDurableSubscriptionBacklog = stats.subscriptions.values().iterator().next().msgBacklog;
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

            // make sure ledgers are trimmed
            PersistentTopicInternalStats internalStats =
              admin.topics().getInternalStats(topic1);

            // check there is only one ledger left
            // TODO in theory there shouldn't be any ledgers left if we are using readers.
            //  However, trimming of ledgers are piggy packed onto ledger operations.
            //  So if there isn't new data coming in, trimming never occurs.
            //  We need to trigger trimming on a schedule to actually delete all remaining ledgers
            assertEquals(internalStats.ledgers.size(), 1);

            // check if its the expected ledger id given MAX_ENTRIES_PER_LEDGER
            assertEquals(internalStats.ledgers.get(0).ledgerId, (2 * numMsgs / MAX_ENTRIES_PER_LEDGER) - 1);

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
    public void testConsumerBacklogEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String topic1 = "persistent://prop/ns-quota/topic1";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 20;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.backlogSize < 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
    }

    @Test
    public void testConsumerBacklogEvictionWithAck() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).build();

        final String topic1 = "persistent://prop/ns-quota/topic11";
        final String subName1 = "c11";
        final String subName2 = "c21";
        final int numMsgs = 20;

        Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1).create();
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
        assertTrue(stats.backlogSize <= 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
    }

    @Test
    public void testConcurrentAckAndEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/ns-quota/topic12";
        final String subName1 = "c12";
        final String subName2 = "c22";
        final int numMsgs = 20;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer1 = client2.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1)
                            .create();
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

        Thread ConsumerThread = new Thread() {
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
        ConsumerThread.start();

        // test hangs without timeout since there is nothing to consume due to eviction
        counter.await(20, TimeUnit.SECONDS);
        assertFalse(gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.backlogSize <= 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
        client2.close();
    }

    @Test
    public void testNoEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/ns-quota/topic13";
        final String subName1 = "c13";
        final String subName2 = "c23";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        final Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        final Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = client2.newProducer().topic(topic1)
                            .create();
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

        Thread ConsumerThread = new Thread() {
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
        ConsumerThread.start();
        counter.await();
        assertFalse(gotException.get());
        client.close();
        client2.close();
    }

    @Test
    public void testEvictionMulti() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/ns-quota"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/ns-quota",
                new BacklogQuota(15 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/ns-quota/topic14";
        final String subName1 = "c14";
        final String subName2 = "c24";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(4);
        final CountDownLatch counter = new CountDownLatch(4);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        final Consumer<byte[]> consumer1 = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        final Consumer<byte[]> consumer2 = client.newConsumer().topic(topic1).subscriptionName(subName2).subscribe();
        final PulsarClient client3 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();

        Thread producerThread1 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer<byte[]> producer = client2.newProducer().topic(topic1)
                            .create();
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
                    org.apache.pulsar.client.api.Producer<byte[]> producer = client3.newProducer().topic(topic1)
                            .create();
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

        Thread ConsumerThread1 = new Thread() {
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

        Thread ConsumerThread2 = new Thread() {
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
        ConsumerThread1.start();
        ConsumerThread2.start();
        counter.await(20, TimeUnit.SECONDS);
        assertFalse(gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        assertTrue(stats.backlogSize <= 15 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
        client2.close();
        client3.close();
    }

    @Test
    public void testAheadProducerOnHold() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_request_hold));
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/hold";
        final String subName1 = "c1hold";
        final int numMsgs = 10;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = client.newProducer().topic(topic1).sendTimeout(2, TimeUnit.SECONDS).create();
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
        assertEquals(stats.publishers.size(), 0,
                "Number of producers on topic " + topic1 + " are [" + stats.publishers.size() + "]");
        client.close();
    }

    @Test
    public void testAheadProducerOnHoldTimeout() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_request_hold));
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/holdtimeout";
        final String subName1 = "c1holdtimeout";
        boolean gotException = false;

        client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = client.newProducer().topic(topic1).sendTimeout(2, TimeUnit.SECONDS).create();
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
        client.close();
    }

    @Test
    public void testProducerException() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_exception));
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/except";
        final String subName1 = "c1except";
        boolean gotException = false;

        client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = client.newProducer().topic(topic1).sendTimeout(2, TimeUnit.SECONDS).create();
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
        client.close();
    }

    @Test
    public void testProducerExceptionAndThenUnblock() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/quotahold"),
                ConfigHelper.backlogQuotaMap(config));
        admin.namespaces().setBacklogQuota("prop/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_exception));
        final PulsarClient client = PulsarClient.builder().serviceUrl(adminUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).build();
        final String topic1 = "persistent://prop/quotahold/exceptandunblock";
        final String subName1 = "c1except";
        boolean gotException = false;

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();

        byte[] content = new byte[1024];
        Producer<byte[]> producer = client.newProducer().topic(topic1).sendTimeout(2, TimeUnit.SECONDS).create();
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
        // now remove backlog and ensure that producer is unblockedrolloverStats();

        TopicStats stats = admin.topics().getStats(topic1);
        int backlog = (int) stats.subscriptions.get(subName1).msgBacklog;

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
        client.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BacklogQuotaManagerTest.class);
}
