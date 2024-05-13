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

import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import com.google.common.collect.Sets;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.Assert;

@Slf4j
public abstract class OneWayReplicatorTestBase extends TestRetrySupport {

    protected final String defaultTenant = "public";
    protected final String replicatedNamespace = defaultTenant + "/default";
    protected final String nonReplicatedNamespace = defaultTenant + "/ns1";

    protected final String cluster1 = "r1";
    protected URL url1;
    protected URL urlTls1;
    protected ServiceConfiguration config1 = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk1;
    protected LocalBookkeeperEnsemble bkEnsemble1;
    protected PulsarService pulsar1;
    protected BrokerService ns1;
    protected PulsarAdmin admin1;
    protected PulsarClient client1;

    protected URL url2;
    protected URL urlTls2;
    protected final String cluster2 = "r2";
    protected ServiceConfiguration config2 = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk2;
    protected LocalBookkeeperEnsemble bkEnsemble2;
    protected PulsarService pulsar2;
    protected BrokerService ns2;
    protected PulsarAdmin admin2;
    protected PulsarClient client2;

    protected void startZKAndBK() throws Exception {
        // Start ZK.
        brokerConfigZk1 = new ZookeeperServerTest(0);
        brokerConfigZk1.start();
        brokerConfigZk2 = new ZookeeperServerTest(0);
        brokerConfigZk2.start();

        // Start BK.
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble2.start();
    }

    protected void startBrokers() throws Exception {
        // Start brokers.
        setConfigDefaults(config1, cluster1, bkEnsemble1, brokerConfigZk1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        ns1 = pulsar1.getBrokerService();

        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        client1 = PulsarClient.builder().serviceUrl(url1.toString()).build();

        // Start region 2
        setConfigDefaults(config2, cluster2, bkEnsemble2, brokerConfigZk2);
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        ns2 = pulsar2.getBrokerService();

        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();
        client2 = PulsarClient.builder().serviceUrl(url2.toString()).build();
    }

    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        admin1.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin1.clusters().createCluster(cluster2, ClusterData.builder()
                .serviceUrl(url2.toString())
                .serviceUrlTls(urlTls2.toString())
                .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
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

        admin1.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin2.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));

        admin1.namespaces().createNamespace(replicatedNamespace, Sets.newHashSet(cluster1, cluster2));
        admin2.namespaces().createNamespace(replicatedNamespace);
        admin1.namespaces().createNamespace(nonReplicatedNamespace);
        admin2.namespaces().createNamespace(nonReplicatedNamespace);
    }

    protected void cleanupTopics(CleanupTopicAction cleanupTopicAction) throws Exception {
        cleanupTopics(replicatedNamespace, cleanupTopicAction);
    }

    protected void cleanupTopics(String namespace, CleanupTopicAction cleanupTopicAction) throws Exception {
        waitChangeEventsInit(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Collections.singleton(cluster1));
        admin1.namespaces().unload(namespace);
        cleanupTopicAction.run();
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        waitChangeEventsInit(namespace);
    }

    protected void waitChangeEventsInit(String namespace) {
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(namespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME, false)
                .join().get();
        Awaitility.await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            TopicStatsImpl topicStats = topic.getStats(true, false, false);
            topicStats.getSubscriptions().entrySet().forEach(entry -> {
                // No wait for compaction.
                if (COMPACTION_SUBSCRIPTION.equals(entry.getKey())) {
                    return;
                }
                // No wait for durable cursor.
                if (entry.getValue().isDurable()) {
                    return;
                }
                Assert.assertTrue(entry.getValue().getMsgBacklog() == 0, entry.getKey());
            });
        });
    }

    protected interface CleanupTopicAction {
        void run() throws Exception;
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting OneWayReplicatorTestBase::setup ---");

        startZKAndBK();

        startBrokers();

        createDefaultTenantsAndClustersAndNamespace();

        Thread.sleep(100);
        log.info("--- OneWayReplicatorTestBase::setup completed ---");
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                   LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bookkeeperEnsemble.getZookeeperPort());
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + brokerConfigZk.getZookeeperPort() + "/foo");
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(60);
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setBacklogQuotaCheckIntervalInSeconds(5);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
        config.setLoadBalancerSheddingEnabled(false);
    }

    @Override
    protected void cleanup() throws Exception {
        // delete namespaces.
        waitChangeEventsInit(replicatedNamespace);
        admin1.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Sets.newHashSet(cluster1));
        admin1.namespaces().deleteNamespace(replicatedNamespace);
        admin2.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Sets.newHashSet(cluster2));
        admin2.namespaces().deleteNamespace(replicatedNamespace);
        admin1.namespaces().deleteNamespace(nonReplicatedNamespace);
        admin2.namespaces().deleteNamespace(nonReplicatedNamespace);

        // shutdown.
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");

        // Stop brokers.
        if (client1 != null) {
            client1.close();
            client1 = null;
        }
        if (client2 != null) {
            client2.close();
            client2 = null;
        }
        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }
        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }

        // Stop ZK and BK.
        if (bkEnsemble1 != null) {
            bkEnsemble1.stop();
            bkEnsemble1 = null;
        }
        if (bkEnsemble2 != null) {
            bkEnsemble2.stop();
            bkEnsemble2 = null;
        }
        if (brokerConfigZk1 != null) {
            brokerConfigZk1.stop();
            brokerConfigZk1 = null;
        }
        if (brokerConfigZk2 != null) {
            brokerConfigZk2.stop();
            brokerConfigZk2 = null;
        }

        // Reset configs.
        config1 = new ServiceConfiguration();
        config2 = new ServiceConfiguration();
    }
}
