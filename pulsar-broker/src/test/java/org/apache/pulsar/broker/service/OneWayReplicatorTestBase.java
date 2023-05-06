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

import com.google.common.collect.Sets;
import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;

@Slf4j
public abstract class OneWayReplicatorTestBase extends TestRetrySupport {

    protected final String defaultTenant = "public";
    protected final String defaultNamespace = defaultTenant + "/default";

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

        admin1.namespaces().createNamespace(defaultNamespace, Sets.newHashSet(cluster1, cluster2));
        admin2.namespaces().createNamespace(defaultNamespace);
    }

    protected void cleanupTopics(CleanupTopicAction cleanupTopicAction) throws Exception {
        admin1.namespaces().setNamespaceReplicationClusters(defaultNamespace, Collections.singleton(cluster1));
        admin1.namespaces().unload(defaultNamespace);
        cleanupTopicAction.run();
        admin1.namespaces().setNamespaceReplicationClusters(defaultNamespace, Sets.newHashSet(cluster1, cluster2));
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

    private void setConfigDefaults(ServiceConfiguration config, String clusterName,
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
        config.setAllowAutoTopicCreationType("non-partitioned");
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
    }

    @Override
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");

        // Stop brokers.
        client1.close();
        client2.close();
        admin1.close();
        admin2.close();
        if (pulsar2 != null) {
            pulsar2.close();
        }
        if (pulsar1 != null) {
            pulsar1.close();
        }

        // Stop ZK and BK.
        bkEnsemble1.stop();
        bkEnsemble2.stop();
        brokerConfigZk1.stop();
        brokerConfigZk2.stop();

        // Reset configs.
        config1 = new ServiceConfiguration();
        setConfigDefaults(config1, cluster1, bkEnsemble1, brokerConfigZk1);
        config2 = new ServiceConfiguration();
        setConfigDefaults(config2, cluster2, bkEnsemble2, brokerConfigZk2);
    }
}
