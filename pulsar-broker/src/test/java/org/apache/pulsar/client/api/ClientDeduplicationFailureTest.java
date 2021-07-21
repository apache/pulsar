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
package org.apache.pulsar.client.api;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "quarantine")
public class ClientDeduplicationFailureTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    URL url;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    final String tenant = "external-repl-prop";
    String primaryHost;

    @BeforeMethod(timeOut = 300000, alwaysRun = true)
    void setup(Method method) throws Exception {
        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setTlsAllowInsecureConnection(true);
        config.setAdvertisedAddress("localhost");
        config.setLoadBalancerSheddingEnabled(false);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        config.setLoadBalancerEnabled(false);
        config.setLoadBalancerAutoUnloadSplitBundlesEnabled(false);

        config.setAllowAutoTopicCreationType("non-partitioned");


        pulsar = new PulsarService(config);
        pulsar.start();

        String brokerServiceUrl = pulsar.getWebServiceAddress();
        url = new URL(brokerServiceUrl);

        admin = PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).build();

        brokerStatsClient = admin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        ClusterData clusterData = ClusterData.builder().serviceUrl(url.toString()).build();
        admin.clusters().createCluster(config.getClusterName(), clusterData);

        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).maxBackoffInterval(1, TimeUnit.SECONDS);
        pulsarClient = clientBuilder.build();

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton("use"))
                .build();
        admin.tenants().createTenant(tenant, tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        pulsarClient.close();
        admin.close();
        pulsar.close();
        bkEnsemble.stop();
    }

    private static class ProducerThread implements Runnable {

        private volatile boolean isRunning = false;
        private Thread thread;
        private Producer<String> producer;
        private long i = 1;
        private final AtomicLong atomicLong = new AtomicLong(0);
        private CompletableFuture<MessageId> lastMessageFuture;

        public ProducerThread(Producer<String> producer) {
            this.thread = new Thread(this);
            this.producer = producer;
        }

        @Override
        public void run() {
            while(isRunning) {
                lastMessageFuture = producer.newMessage().sequenceId(i).value("foo-" + i).sendAsync();
                lastMessageFuture.thenAccept(messageId -> {
                    atomicLong.incrementAndGet();

                }).exceptionally(ex -> {
                    log.info("publish exception:", ex);
                    return null;
                });
                i++;
            }
            log.info("done Producing! Last send: {}", i);
        }

        public void start() {
            this.isRunning = true;
            this.thread.start();
        }

        public void stop() {
            this.isRunning = false;
            try {
                log.info("Waiting for last message to complete");
                try {
                    this.lastMessageFuture.get(60, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    throw new RuntimeException("Last message hasn't completed within timeout!");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.info("Producer Thread stopped!");
        }

        public long getLastSeqId() {
            return this.atomicLong.get();
        }
    }

    @Test(timeOut = 300000, groups = "quarantine")
    public void testClientDeduplicationCorrectnessWithFailure() throws Exception {
        final String namespacePortion = "dedup";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        admin.namespaces().setDeduplicationStatus(replNamespace, true);
        admin.namespaces().setRetention(replNamespace, new RetentionPolicies(-1, -1));
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .blockIfQueueFull(true).sendTimeout(0, TimeUnit.SECONDS)
                .topic(sourceTopic)
                .producerName("test-producer-1")
                .create();


        ProducerThread producerThread = new ProducerThread(producer);
        producerThread.start();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                return topicStats.getPublishers().size() == 1 && topicStats.getPublishers().get(0).getProducerName().equals("test-producer-1") && topicStats.getStorageSize() > 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.getPublishers().get(0).getProducerName(), "test-producer-1");
        assertTrue(topicStats.getStorageSize() > 0);

        for (int i = 0; i < 5; i++) {
            log.info("Stopping BK...");
            bkEnsemble.stopBK();

            Thread.sleep(1000 + new Random().nextInt(500));

            log.info("Starting BK...");
            bkEnsemble.startBK();
        }

        producerThread.stop();

        // send last message
        producer.newMessage().sequenceId(producerThread.getLastSeqId() + 1).value("end").send();
        producer.close();

        Reader<String> reader = pulsarClient.newReader(Schema.STRING).startMessageId(MessageId.earliest)
                .topic(sourceTopic).create();
        Message<String> prevMessage = null;
        Message<String> message = null;
        int count = 0;
        while(true) {
            message = reader.readNext(5, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }

            if (message.getValue().equals("end")) {
                log.info("Last seq Id received: {}", prevMessage.getSequenceId());
                break;
            }
            if (prevMessage == null) {
                assertEquals(message.getSequenceId(), 1);
            } else {
                assertEquals(message.getSequenceId(), prevMessage.getSequenceId() + 1);
            }
            prevMessage = message;
            count++;
        }

        log.info("# of messages read: {}", count);

        assertNotNull(prevMessage);
        assertEquals(prevMessage.getSequenceId(), producerThread.getLastSeqId());
    }

    @Test(timeOut = 300000)
    public void testClientDeduplicationWithBkFailure() throws  Exception {
        final String namespacePortion = "dedup";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String subscriptionName1 = "sub1";
        final String subscriptionName2 = "sub2";
        final String consumerName1 = "test-consumer-1";
        final String consumerName2 = "test-consumer-2";
        final List<Message<String>> msgRecvd = new LinkedList<>();
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        admin.namespaces().setDeduplicationStatus(replNamespace, true);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic)
                .producerName("test-producer-1").create();
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(sourceTopic)
                .consumerName(consumerName1).subscriptionName(subscriptionName1).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(sourceTopic)
                .consumerName(consumerName2).subscriptionName(subscriptionName2).subscribe();

        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    Message<String> msg = consumer2.receive();
                    msgRecvd.add(msg);
                    consumer2.acknowledge(msg);
                } catch (PulsarClientException e) {
                    log.error("Failed to consume message: {}", e, e);
                    break;
                }
            }
        });
        thread.start();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                boolean c1 =  topicStats!= null
                        && topicStats.getSubscriptions().get(subscriptionName1) != null
                        && topicStats.getSubscriptions().get(subscriptionName1).getConsumers().size() == 1
                        && topicStats.getSubscriptions().get(subscriptionName1).getConsumers().get(0).getConsumerName().equals(consumerName1);

                boolean c2 =  topicStats!= null
                        && topicStats.getSubscriptions().get(subscriptionName2) != null
                        && topicStats.getSubscriptions().get(subscriptionName2).getConsumers().size() == 1
                        && topicStats.getSubscriptions().get(subscriptionName2).getConsumers().get(0).getConsumerName().equals(consumerName2);
                return c1 && c2;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        TopicStats topicStats1 = admin.topics().getStats(sourceTopic);
        assertNotNull(topicStats1);
        assertNotNull(topicStats1.getSubscriptions().get(subscriptionName1));
        assertEquals(topicStats1.getSubscriptions().get(subscriptionName1).getConsumers().size(), 1);
        assertEquals(topicStats1.getSubscriptions().get(subscriptionName1).getConsumers().get(0).getConsumerName(), consumerName1);
        TopicStats topicStats2 = admin.topics().getStats(sourceTopic);
        assertNotNull(topicStats2);
        assertNotNull(topicStats2.getSubscriptions().get(subscriptionName2));
        assertEquals(topicStats2.getSubscriptions().get(subscriptionName2).getConsumers().size(), 1);
        assertEquals(topicStats2.getSubscriptions().get(subscriptionName2).getConsumers().get(0).getConsumerName(), consumerName2);

        for (int i=0; i<10; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        for (int i=0; i<10; i++) {
            Message<String> msg = consumer1.receive();
            consumer1.acknowledge(msg);
            assertEquals(msg.getValue(), "foo-" + i);
            assertEquals(msg.getSequenceId(), i);
        }

        log.info("Stopping BK...");
        bkEnsemble.stopBK();

        List<CompletableFuture<MessageId>> futures = new LinkedList<>();
        for (int i=10; i<20; i++) {
            CompletableFuture<MessageId> future = producer.newMessage().sequenceId(i).value("foo-" + i).sendAsync();
            int finalI = i;
            future.thenRun(() -> log.error("message: {} successful", finalI)).exceptionally((Function<Throwable, Void>) throwable -> {
                log.info("message: {} failed: {}", finalI, throwable, throwable);
                return null;
            });
            futures.add(future);
        }

        for (int i = 0; i < futures.size(); i++) {
            try {
                // message should not be produced successfully
                futures.get(i).join();
                fail();
            } catch (CompletionException ex) {

            } catch (Exception e) {
                fail();
            }
        }

        try {
            producer.newMessage().sequenceId(10).value("foo-10").send();
            fail();
        } catch (PulsarClientException ex) {

        }

        try {
            producer.newMessage().sequenceId(10).value("foo-10").send();
            fail();
        } catch (PulsarClientException ex) {

        }

        log.info("Starting BK...");
        bkEnsemble.startBK();

        for (int i=20; i<30; i++) {
            producer.newMessage().sequenceId(i).value("foo-" + i).send();
        }

        MessageId lastMessageId = null;
        for (int i=20; i<30; i++) {
            Message<String> msg = consumer1.receive();
            lastMessageId = msg.getMessageId();
            consumer1.acknowledge(msg);
            assertEquals(msg.getValue(), "foo-" + i);
            assertEquals(msg.getSequenceId(), i);
        }

        // check all messages
        retryStrategically((test) -> msgRecvd.size() >= 20, 5, 200);

        assertEquals(msgRecvd.size(), 20);
        for (int i = 0; i < 10; i++) {
            assertEquals(msgRecvd.get(i).getValue(), "foo-" + i);
            assertEquals(msgRecvd.get(i).getSequenceId(), i);
        }
        for (int i = 10; i <20; i++) {
            assertEquals(msgRecvd.get(i).getValue(), "foo-" + (i + 10));
            assertEquals(msgRecvd.get(i).getSequenceId(), i + 10);
        }

        BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) lastMessageId;
        MessageIdImpl messageId = (MessageIdImpl) consumer1.getLastMessageId();

        assertEquals(messageId.getLedgerId(), batchMessageId.getLedgerId());
        assertEquals(messageId.getEntryId(), batchMessageId.getEntryId());
        thread.interrupt();
    }
}
