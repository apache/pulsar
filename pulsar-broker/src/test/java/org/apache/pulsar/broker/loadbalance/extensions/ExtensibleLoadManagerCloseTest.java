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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ExtensibleLoadManagerCloseTest {

    private static final String clusterName = "test";
    private final List<PulsarService> brokers = new ArrayList<>();
    private LocalBookkeeperEnsemble bk;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        bk = new LocalBookkeeperEnsemble(1, 0, () -> 0);
        bk.start();
    }

    private void setupBrokers(int numBrokers) throws Exception {
        brokers.clear();
        for (int i = 0; i < numBrokers; i++) {
            final var broker = new PulsarService(brokerConfig());
            broker.start();
            brokers.add(broker);
        }
        final var admin = brokers.get(0).getAdminClient();
        if (!admin.clusters().getClusters().contains(clusterName)) {
            admin.clusters().createCluster(clusterName, ClusterData.builder().build());
            admin.tenants().createTenant("public", TenantInfo.builder()
                    .allowedClusters(Collections.singleton(clusterName)).build());
            admin.namespaces().createNamespace("public/default");
        }
    }


    @AfterClass(alwaysRun = true, timeOut = 30000)
    public void cleanup() throws Exception {
        bk.stop();
    }

    private ServiceConfiguration brokerConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bk.getZookeeperPort());
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadBalancerAutoBundleSplitEnabled(false);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(100);

        // Reduce these timeout configs to avoid failed tests being blocked too long
        config.setMetadataStoreOperationTimeoutSeconds(5);
        config.setNamespaceBundleUnloadingTimeoutMs(5000);
        return config;
    }


    @Test(invocationCount = 10)
    public void testCloseAfterLoadingBundles() throws Exception {
        setupBrokers(3);
        final var topic = "test-" + System.currentTimeMillis();
        final var admin = brokers.get(0).getAdminClient();
        admin.topics().createPartitionedTopic(topic, 20);
        admin.lookups().lookupPartitionedTopic(topic);
        final var client = PulsarClient.builder().serviceUrl(brokers.get(0).getBrokerServiceUrl()).build();
        final var producer = client.newProducer().topic(topic).create();
        producer.close();
        client.close();

        final var closeTimeMsList = new ArrayList<Long>();
        for (var broker : brokers) {
            final var startTimeMs = System.currentTimeMillis();
            broker.close();
            closeTimeMsList.add(System.currentTimeMillis() - startTimeMs);
        }
        log.info("Brokers close time: {}", closeTimeMsList);
        for (var closeTimeMs : closeTimeMsList) {
            Assert.assertTrue(closeTimeMs < 5000L);
        }
    }

    @Test
    public void testLookup() throws Exception {
        setupBrokers(1);
        final var topic = "test-lookup";
        final var numPartitions = 16;
        final var admin = brokers.get(0).getAdminClient();
        admin.topics().createPartitionedTopic(topic, numPartitions);

        final var futures = new ArrayList<CompletableFuture<String>>();
        for (int i = 0; i < numPartitions; i++) {
            futures.add(admin.lookups().lookupTopicAsync(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i));
        }
        FutureUtil.waitForAll(futures).get();

        final var start = System.currentTimeMillis();
        brokers.get(0).close();
        final var closeTimeMs = System.currentTimeMillis() - start;
        log.info("Broker close time: {}", closeTimeMs);
        Assert.assertTrue(closeTimeMs < 5000L);
    }
}
