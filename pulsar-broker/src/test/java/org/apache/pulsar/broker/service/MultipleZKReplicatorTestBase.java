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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MultipleZKReplicatorTestBase extends TestRetrySupport {
    URL url1;
    URL urlTls1;
    ServiceConfiguration config1 = new ServiceConfiguration();
    PulsarService pulsar1;
    BrokerService ns1;

    PulsarAdmin admin1;
    LocalBookkeeperEnsemble bkEnsemble1;

    URL url2;
    URL urlTls2;
    ServiceConfiguration config2 = new ServiceConfiguration();
    PulsarService pulsar2;
    BrokerService ns2;
    PulsarAdmin admin2;
    LocalBookkeeperEnsemble bkEnsemble2;

    ZookeeperServerTest zk1;
    ZookeeperServerTest zk2;

    ExecutorService executor;

    static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;


    protected final String cluster1 = "r1";
    protected final String cluster2 = "r2";

    // Default frequency
    public int getBrokerServicePurgeInactiveFrequency() {
        return 60;
    }

    public boolean isBrokerServicePurgeInactiveTopic() {
        return false;
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting MultipleZKReplicatorTestBase::setup ---");
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new DefaultThreadFactory("MultipleZKReplicatorTestBase"));

        zk1 = new ZookeeperServerTest(0);
        zk1.start();

        zk2 = new ZookeeperServerTest(10);
        zk2.start();

        // Start region 1
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();

        // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
        // completely
        // independent config objects instead of referring to the same properties object
        setConfig1DefaultValue();
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        ns1 = pulsar1.getBrokerService();

        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start region 2

        // Start zk & bks
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble2.start();

        setConfig2DefaultValue();
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        ns2 = pulsar2.getBrokerService();

        log.info("url2 : {}", pulsar2.getBrokerServiceUrl());
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        // Provision the global namespace
        admin1.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin1.clusters().createCluster(cluster2, ClusterData.builder()
                .serviceUrl(url2.toString())
                .brokerClientTlsEnabled(false)
                // TODO: Why the topic can not be auto-created when special brokerServiceUrl
//                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
//                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .build());

        admin2.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin2.clusters().createCluster(cluster2, ClusterData.builder()
                .serviceUrl(url2.toString())
                .serviceUrlTls(urlTls2.toString())
                .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());

        admin1.tenants().createTenant("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2")));
        admin2.tenants().createTenant("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2")));


        assertEquals(admin2.clusters().getCluster(cluster1).getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster(cluster2).getServiceUrl(), url2.toString());

        Thread.sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    public void setConfig1DefaultValue(){
        setConfigDefaults(config1, cluster1, bkEnsemble1, zk1);
    }

    public void setConfig2DefaultValue() {
        setConfigDefaults(config2, cluster2, bkEnsemble2, zk2);
    }

    private void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                   LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest globalZkS) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bookkeeperEnsemble.getZookeeperPort());
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + globalZkS.getZookeeperPort() + "/foo");
        config.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveTopic());
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setBrokerClientTlsEnabled(false);
        config.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
    }

    public void resetConfig1() {
        config1 = new ServiceConfiguration();
        setConfig1DefaultValue();
    }

    public void resetConfig2() {
        config2 = new ServiceConfiguration();
        setConfig2DefaultValue();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @Override
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        admin1.close();
        admin2.close();

        if (pulsar2 != null) {
            pulsar2.close();
        }
        if (pulsar1 != null) {
            pulsar1.close();
        }

        bkEnsemble1.stop();
        bkEnsemble2.stop();

        zk1.stop();
        zk2.start();

        resetConfig1();
        resetConfig2();
    }

    static class MessageProducer implements AutoCloseable {
        URL url;
        String namespace;
        String topicName;
        PulsarClient client;
        Producer<byte[]> producer;

        MessageProducer(URL url, final TopicName dest) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            try {
                producer = client.newProducer()
                        .topic(topicName)
                        .enableBatching(false)
                        .messageRoutingMode(MessageRoutingMode.SinglePartition)
                        .create();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        MessageProducer(URL url, final TopicName dest, boolean batch) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topicName)
                .enableBatching(batch)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .batchingMaxMessages(5);
            try {
                producer = producerBuilder.create();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void produceBatch(int messages) throws Exception {
            log.info("Start sending batch messages");

            for (int i = 0; i < messages; i++) {
                producer.sendAsync(("test-" + i).getBytes());
                log.info("queued message {}", ("test-" + i));
            }
            producer.flush();
        }

        void produce(int messages) throws Exception {

            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                producer.send(("test-" + i).getBytes());
                log.info("Sent message {}", ("test-" + i));
            }

        }

        TypedMessageBuilder<byte[]> newMessage() {
            return producer.newMessage();
        }

        void produce(int messages, TypedMessageBuilder<byte[]> messageBuilder) throws Exception {
            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                final String m = "test-" + i;
                messageBuilder.value(m.getBytes()).replicationClusters(List.of("cluster-a", "cluster-b")).send();
                log.info("Sent message {}", m);
            }
        }

        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
        }

    }

    static class MessageConsumer implements AutoCloseable {
        final URL url;
        final String namespace;
        final String topicName;
        final PulsarClient client;
        final Consumer<byte[]> consumer;

        MessageConsumer(URL url, final TopicName dest) throws Exception {
            this(url, dest, "sub-id");
        }

        MessageConsumer(URL url, final TopicName dest, String subId) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();

            client = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();

            try {
                consumer = client.newConsumer().topic(topicName).subscriptionName(subId).subscribe();
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void receive(int messages) throws Exception {
            log.info("Start receiving messages");
            Message<byte[]> msg;

            Set<String> receivedMessages = new TreeSet<>();

            int i = 0;
            while (i < messages) {
                msg = consumer.receive(10, TimeUnit.SECONDS);
                assertNotNull(msg);
                consumer.acknowledge(msg);

                String msgData = new String(msg.getData());
                log.info("Received message {}", msgData);

                boolean added = receivedMessages.add(msgData);
                if (added) {
                    assertEquals(msgData, "test-" + i);
                    i++;
                } else {
                    log.info("Ignoring duplicate {}", msgData);
                }
            }
        }

        boolean drained() throws Exception {
            return consumer.receive(0, TimeUnit.MICROSECONDS) == null;
        }

        public void close() {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close client", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MultipleZKReplicatorTestBase.class);
}
