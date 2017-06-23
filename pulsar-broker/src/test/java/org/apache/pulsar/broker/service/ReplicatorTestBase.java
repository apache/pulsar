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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ReplicatorTestBase {
    URL url1;
    PulsarService pulsar1;
    BrokerService ns1;

    PulsarAdmin admin1;
    LocalBookkeeperEnsemble bkEnsemble1;

    URL url2;
    ServiceConfiguration config2;
    PulsarService pulsar2;
    BrokerService ns2;
    PulsarAdmin admin2;
    LocalBookkeeperEnsemble bkEnsemble2;

    URL url3;
    ServiceConfiguration config3;
    PulsarService pulsar3;
    BrokerService ns3;
    PulsarAdmin admin3;
    LocalBookkeeperEnsemble bkEnsemble3;

    ZookeeperServerTest globalZkS;

    ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

    static final int TIME_TO_CHECK_BACKLOG_QUOTA = 5;

    // Default frequency
    public int getBrokerServicePurgeInactiveFrequency() {
        return 60;
    }

    public boolean isBrokerServicePurgeInactiveDestination() {
        return false;
    }

    void setup() throws Exception {
        log.info("--- Starting ReplicatorTestBase::setup ---");
        int globalZKPort = PortManager.nextFreePort();
        globalZkS = new ZookeeperServerTest(globalZKPort);
        globalZkS.start();

        // Start region 1
        int zkPort1 = PortManager.nextFreePort();
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, zkPort1, PortManager.nextFreePort());
        bkEnsemble1.start();

        int webServicePort1 = PortManager.nextFreePort();

        // NOTE: we have to instantiate a new copy of System.getProperties() to make sure pulsar1 and pulsar2 have
        // completely
        // independent config objects instead of referring to the same properties object
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setClusterName("r1");
        config1.setWebServicePort(webServicePort1);
        config1.setZookeeperServers("127.0.0.1:" + zkPort1);
        config1.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
        config1.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
        config1.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config1.setBrokerServicePort(PortManager.nextFreePort());
        config1.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        ns1 = pulsar1.getBrokerService();

        url1 = new URL("http://127.0.0.1:" + webServicePort1);
        admin1 = new PulsarAdmin(url1, (Authentication) null);

        // Start region 2

        // Start zk & bks
        int zkPort2 = PortManager.nextFreePort();
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, zkPort2, PortManager.nextFreePort());
        bkEnsemble2.start();

        int webServicePort2 = PortManager.nextFreePort();
        config2 = new ServiceConfiguration();
        config2.setClusterName("r2");
        config2.setWebServicePort(webServicePort2);
        config2.setZookeeperServers("127.0.0.1:" + zkPort2);
        config2.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
        config2.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
        config2.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config2.setBrokerServicePort(PortManager.nextFreePort());
        config2.setBacklogQuotaCheckIntervalInSeconds(TIME_TO_CHECK_BACKLOG_QUOTA);
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        ns2 = pulsar2.getBrokerService();

        url2 = new URL("http://127.0.0.1:" + webServicePort2);
        admin2 = new PulsarAdmin(url2, (Authentication) null);

        // Start region 3

        // Start zk & bks
        int zkPort3 = PortManager.nextFreePort();
        bkEnsemble3 = new LocalBookkeeperEnsemble(3, zkPort3, PortManager.nextFreePort());
        bkEnsemble3.start();

        int webServicePort3 = PortManager.nextFreePort();
        config3 = new ServiceConfiguration();
        config3.setClusterName("r3");
        config3.setWebServicePort(webServicePort3);
        config3.setZookeeperServers("127.0.0.1:" + zkPort3);
        config3.setGlobalZookeeperServers("127.0.0.1:" + globalZKPort + "/foo");
        config3.setBrokerDeleteInactiveTopicsEnabled(isBrokerServicePurgeInactiveDestination());
        config3.setBrokerServicePurgeInactiveFrequencyInSeconds(
                inSec(getBrokerServicePurgeInactiveFrequency(), TimeUnit.SECONDS));
        config3.setBrokerServicePort(PortManager.nextFreePort());
        pulsar3 = new PulsarService(config3);
        pulsar3.start();
        ns3 = pulsar3.getBrokerService();

        url3 = new URL("http://127.0.0.1:" + webServicePort3);
        admin3 = new PulsarAdmin(url3, (Authentication) null);

        // Provision the global namespace
        admin1.clusters().createCluster("r1", new ClusterData(url1.toString(), null, pulsar1.getBrokerServiceUrl(),
                pulsar1.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r2", new ClusterData(url2.toString(), null, pulsar2.getBrokerServiceUrl(),
                pulsar1.getBrokerServiceUrlTls()));
        admin1.clusters().createCluster("r3", new ClusterData(url3.toString(), null, pulsar3.getBrokerServiceUrl(),
                pulsar1.getBrokerServiceUrlTls()));

        admin1.clusters().createCluster("global", new ClusterData("http://global:8080"));
        admin1.properties().createProperty("pulsar",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2", "appid3"), Sets.newHashSet("r1", "r2", "r3")));
        admin1.namespaces().createNamespace("pulsar/global/ns");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns", Lists.newArrayList("r1", "r2", "r3"));
        admin1.namespaces().createNamespace("pulsar/global/ns1");
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns1", Lists.newArrayList("r1", "r2"));

        assertEquals(admin2.clusters().getCluster("r1").getServiceUrl(), url1.toString());
        assertEquals(admin2.clusters().getCluster("r2").getServiceUrl(), url2.toString());
        assertEquals(admin2.clusters().getCluster("r3").getServiceUrl(), url3.toString());
        assertEquals(admin2.clusters().getCluster("r1").getBrokerServiceUrl(), pulsar1.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r2").getBrokerServiceUrl(), pulsar2.getBrokerServiceUrl());
        assertEquals(admin2.clusters().getCluster("r3").getBrokerServiceUrl(), pulsar3.getBrokerServiceUrl());
        /*
         * assertEquals(admin2.clusters().getCluster("global").getServiceUrl(), "http://global:8080");
         * assertEquals(admin2.properties().getPropertyAdmin("pulsar").getAdminRoles(), Lists.newArrayList("appid1",
         * "appid2")); assertEquals(admin2.namespaces().getPolicies("pulsar/global/ns").replication_clusters,
         * Lists.newArrayList("r1", "r2", "r3"));
         *
         * admin1.namespaces().createNamespace("pulsar/global/ns2");
         * admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns2", Lists.newArrayList("r1", "r2",
         * "r3"));
         */
        Thread.sleep(100);
        log.info("--- ReplicatorTestBase::setup completed ---");

    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        admin1.close();
        admin2.close();
        admin3.close();

        pulsar3.close();
        ns3.close();

        pulsar2.close();
        ns2.close();

        pulsar1.close();
        ns1.close();

        bkEnsemble1.stop();
        bkEnsemble2.stop();
        bkEnsemble3.stop();
        globalZkS.stop();
    }

    static class MessageProducer {
        URL url;
        String namespace;
        String topicName;
        PulsarClient client;
        Producer producer;

        MessageProducer(URL url, final DestinationName dest) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            ClientConfiguration conf = new ClientConfiguration();
            conf.setStatsInterval(0, TimeUnit.SECONDS);
            client = PulsarClient.create(url.toString(), conf);
            producer = client.createProducer(topicName);

        }

        MessageProducer(URL url, final DestinationName dest, boolean batch) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            ClientConfiguration conf = new ClientConfiguration();
            conf.setStatsInterval(0, TimeUnit.SECONDS);
            client = PulsarClient.create(url.toString(), conf);
            ProducerConfiguration producerConfiguration = new ProducerConfiguration();
            if (batch) {
                producerConfiguration.setBatchingEnabled(true);
                producerConfiguration.setBatchingMaxPublishDelay(1, TimeUnit.SECONDS);
                producerConfiguration.setBatchingMaxMessages(5);
            }
            producer = client.createProducer(topicName, producerConfiguration);

        }

        void produceBatch(int messages) throws Exception {
            log.info("Start sending batch messages");
            List<CompletableFuture<MessageId>> futureList = new ArrayList<>();

            for (int i = 0; i < messages; i++) {
                futureList.add(producer.sendAsync(("test-" + i).getBytes()));
                log.info("queued message {}", ("test-" + i));
            }
            FutureUtil.waitForAll(futureList).get();
        }

        void produce(int messages) throws Exception {

            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                producer.send(("test-" + i).getBytes());
                log.info("Sent message {}", ("test-" + i));
            }

        }

        void produce(int messages, MessageBuilder messageBuilder) throws Exception {
            log.info("Start sending messages");
            for (int i = 0; i < messages; i++) {
                final String m = new String("test-builder-" + i);
                messageBuilder.setContent(m.getBytes());
                producer.send(messageBuilder.build());
                log.info("Sent message {}", m);
            }
        }

        void close() throws Exception {
            client.close();
        }

    }

    static class MessageConsumer {
        final URL url;
        final String namespace;
        final String topicName;
        final PulsarClient client;
        final Consumer consumer;

        MessageConsumer(URL url, final DestinationName dest) throws Exception {
            this(url, dest, "sub-id");
        }

        MessageConsumer(URL url, final DestinationName dest, String subId) throws Exception {
            this.url = url;
            this.namespace = dest.getNamespace();
            this.topicName = dest.toString();
            ClientConfiguration conf = new ClientConfiguration();
            conf.setStatsInterval(0, TimeUnit.SECONDS);
            client = PulsarClient.create(url.toString(), conf);

            try {
                consumer = client.subscribe(topicName, subId);
            } catch (Exception e) {
                client.close();
                throw e;
            }
        }

        void receive(int messages) throws Exception {
            log.info("Start receiving messages");
            Message msg = null;

            for (int i = 0; i < messages; i++) {
                msg = consumer.receive();
                consumer.acknowledge(msg);
                String msgData = new String(msg.getData());
                assertEquals(msgData, "test-" + i);
                log.info("Received message {}", msgData);
            }
        }

        boolean drained() throws Exception {
            return consumer.receive(0, TimeUnit.MICROSECONDS) == null;
        }

        void close() throws Exception {
            client.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTestBase.class);
}
