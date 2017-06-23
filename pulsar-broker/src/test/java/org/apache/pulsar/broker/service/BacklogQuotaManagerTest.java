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
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 */
public class BacklogQuotaManagerTest {
    protected static int BROKER_SERVICE_PORT = 16650;
    PulsarService pulsar;
    ServiceConfiguration config;

    URL adminUrl;
    PulsarAdmin admin;

    LocalBookkeeperEnsemble bkEnsemble;

    private final int ZOOKEEPER_PORT = 12759;
    protected final int BROKER_WEBSERVICE_PORT = 15782;
    private static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    @BeforeMethod
    void setup() throws Exception {
        try {
            // start local bookie and zookeeper
            bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, 5001);
            bkEnsemble.start();

            // start pulsar service
            config = new ServiceConfiguration();
            config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
            config.setWebServicePort(BROKER_WEBSERVICE_PORT);
            config.setClusterName("usc");
            config.setBrokerServicePort(BROKER_SERVICE_PORT);
            config.setAuthorizationEnabled(false);
            config.setAuthenticationEnabled(false);
            config.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
            config.setManagedLedgerMaxEntriesPerLedger(5);
            config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);

            pulsar = new PulsarService(config);
            pulsar.start();

            adminUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
            admin = new PulsarAdmin(adminUrl, (Authentication) null);

            admin.clusters().createCluster("usc", new ClusterData(adminUrl.toString()));
            admin.properties().createProperty("prop",
                    new PropertyAdmin(Lists.newArrayList("appid1"), Sets.newHashSet("usc")));
            admin.namespaces().createNamespace("prop/usc/ns-quota");
            admin.namespaces().createNamespace("prop/usc/quotahold");
            admin.namespaces().createNamespace("prop/usc/quotaholdasync");
        } catch (Throwable t) {
            LOG.error("Error setting up broker test", t);
            Assert.fail("Broker test setup failed");
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
            Assert.fail("Broker test cleanup failed");
        }
    }

    private void rolloverStats() {
        pulsar.getBrokerService().updateRates();
    }

    @Test
    public void testConsumerBacklogEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/ns-quota"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);

        final String topic1 = "persistent://prop/usc/ns-quota/topic1";
        final String subName1 = "c1";
        final String subName2 = "c2";
        final int numMsgs = 20;

        Consumer consumer1 = client.subscribe(topic1, subName1);
        Consumer consumer2 = client.subscribe(topic1, subName2);
        org.apache.pulsar.client.api.Producer producer = client.createProducer(topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            consumer1.receive();
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        Assert.assertTrue(stats.storageSize < 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
    }

    @Test
    public void testConsumerBacklogEvictionWithAck() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/ns-quota"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));
        PulsarClient client = PulsarClient.create(adminUrl.toString());

        final String topic1 = "persistent://prop/usc/ns-quota/topic11";
        final String subName1 = "c11";
        final String subName2 = "c21";
        final int numMsgs = 20;

        Consumer consumer1 = client.subscribe(topic1, subName1);
        Consumer consumer2 = client.subscribe(topic1, subName2);
        org.apache.pulsar.client.api.Producer producer = client.createProducer(topic1);
        byte[] content = new byte[1024];
        for (int i = 0; i < numMsgs; i++) {
            producer.send(content);
            // only one consumer acknowledges the message
            consumer1.acknowledge(consumer1.receive());
            consumer2.receive();
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        Assert.assertTrue(stats.storageSize <= 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
    }

    @Test
    public void testConcurrentAckAndEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/ns-quota"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/usc/ns-quota/topic12";
        final String subName1 = "c12";
        final String subName2 = "c22";
        final int numMsgs = 20;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);
        PulsarClient client2 = PulsarClient.create(adminUrl.toString(), clientConf);
        Consumer consumer1 = client2.subscribe(topic1, subName1);
        Consumer consumer2 = client2.subscribe(topic1, subName2);

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    final org.apache.pulsar.client.api.Producer producer = client.createProducer(topic1);
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
        assertTrue(!gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        Assert.assertTrue(stats.storageSize <= 10 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
        client2.close();
    }

    @Test
    public void testNoEviction() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/ns-quota"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/ns-quota",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/usc/ns-quota/topic13";
        final String subName1 = "c13";
        final String subName2 = "c23";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);

        final Consumer consumer1 = client.subscribe(topic1, subName1);
        final Consumer consumer2 = client.subscribe(topic1, subName2);
        final PulsarClient client2 = PulsarClient.create(adminUrl.toString(), clientConf);

        Thread producerThread = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer producer = client2.createProducer(topic1);
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
        assertTrue(!gotException.get());
        client.close();
        client2.close();
    }

    @Test
    public void testEvictionMulti() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/ns-quota"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/ns-quota",
                new BacklogQuota(15 * 1024, BacklogQuota.RetentionPolicy.consumer_backlog_eviction));

        final String topic1 = "persistent://prop/usc/ns-quota/topic14";
        final String subName1 = "c14";
        final String subName2 = "c24";
        final int numMsgs = 10;

        final CyclicBarrier barrier = new CyclicBarrier(4);
        final CountDownLatch counter = new CountDownLatch(4);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);

        final Consumer consumer1 = client.subscribe(topic1, subName1);
        final Consumer consumer2 = client.subscribe(topic1, subName2);
        final PulsarClient client3 = PulsarClient.create(adminUrl.toString(), clientConf);
        final PulsarClient client2 = PulsarClient.create(adminUrl.toString(), clientConf);

        Thread producerThread1 = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    org.apache.pulsar.client.api.Producer producer = client2.createProducer(topic1);
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
                    org.apache.pulsar.client.api.Producer producer = client3.createProducer(topic1);
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
        assertTrue(!gotException.get());
        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);
        rolloverStats();

        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        Assert.assertTrue(stats.storageSize <= 15 * 1024, "Storage size is [" + stats.storageSize + "]");
        client.close();
        client2.close();
        client3.close();
    }

    @Test
    public void testAheadProducerOnHold() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/quotahold"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_request_hold));
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);
        final String topic1 = "persistent://prop/usc/quotahold/hold";
        final String subName1 = "c1hold";
        final int numMsgs = 10;

        Consumer consumer = client.subscribe(topic1, subName1);
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setSendTimeout(2, TimeUnit.SECONDS);

        byte[] content = new byte[1024];
        Producer producer = client.createProducer(topic1, producerConfiguration);
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
        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        Assert.assertEquals(stats.publishers.size(), 0,
                "Number of producers on topic " + topic1 + " are [" + stats.publishers.size() + "]");
        client.close();
    }

    @Test
    public void testAheadProducerOnHoldTimeout() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/quotahold"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_request_hold));
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);
        final String topic1 = "persistent://prop/usc/quotahold/holdtimeout";
        final String subName1 = "c1holdtimeout";
        boolean gotException = false;

        client.subscribe(topic1, subName1);
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setSendTimeout(2, TimeUnit.SECONDS);

        byte[] content = new byte[1024];
        Producer producer = client.createProducer(topic1, producerConfiguration);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            Assert.fail("backlog quota did not exceed");
        } catch (PulsarClientException.TimeoutException te) {
            gotException = true;
        }

        Assert.assertTrue(gotException, "timeout did not occur");
        client.close();
    }

    @Test
    public void testProducerException() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/quotahold"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_exception));
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);
        final String topic1 = "persistent://prop/usc/quotahold/except";
        final String subName1 = "c1except";
        boolean gotException = false;

        client.subscribe(topic1, subName1);
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setSendTimeout(2, TimeUnit.SECONDS);

        byte[] content = new byte[1024];
        Producer producer = client.createProducer(topic1, producerConfiguration);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            Assert.fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            Assert.assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        Assert.assertTrue(gotException, "backlog exceeded exception did not occur");
        client.close();
    }

    @Test
    public void testProducerExceptionAndThenUnblock() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop/usc/quotahold"), Maps.newTreeMap());
        admin.namespaces().setBacklogQuota("prop/usc/quotahold",
                new BacklogQuota(10 * 1024, BacklogQuota.RetentionPolicy.producer_exception));
        final ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        final PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);
        final String topic1 = "persistent://prop/usc/quotahold/exceptandunblock";
        final String subName1 = "c1except";
        boolean gotException = false;

        Consumer consumer = client.subscribe(topic1, subName1);
        ProducerConfiguration producerConfiguration = new ProducerConfiguration();
        producerConfiguration.setSendTimeout(2, TimeUnit.SECONDS);

        byte[] content = new byte[1024];
        Producer producer = client.createProducer(topic1, producerConfiguration);
        for (int i = 0; i < 10; i++) {
            producer.send(content);
        }

        Thread.sleep((TIME_TO_CHECK_BACKLOG_QUOTA + 1) * 1000);

        try {
            // try to send over backlog quota and make sure it fails
            producer.send(content);
            producer.send(content);
            Assert.fail("backlog quota did not exceed");
        } catch (PulsarClientException ce) {
            Assert.assertTrue(ce instanceof PulsarClientException.ProducerBlockedQuotaExceededException
                    || ce instanceof PulsarClientException.TimeoutException, ce.getMessage());
            gotException = true;
        }

        Assert.assertTrue(gotException, "backlog exceeded exception did not occur");
        // now remove backlog and ensure that producer is unblockedrolloverStats();

        PersistentTopicStats stats = admin.persistentTopics().getStats(topic1);
        int backlog = (int) stats.subscriptions.get(subName1).msgBacklog;

        for (int i = 0; i < backlog; i++) {
            Message msg = consumer.receive();
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
        Assert.assertFalse(gotException, "unable to publish due to " + sendException);
        client.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(BacklogQuotaManagerTest.class);
}
