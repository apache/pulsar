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
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

public class TopicOwnerTest {

    private static final Logger log = LoggerFactory.getLogger(TopicOwnerTest.class);

    LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    protected static final int BROKER_COUNT = 5;
    protected ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];
    protected PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];

    @BeforeMethod
    void setup() throws Exception {
        log.info("---- Initializing TopicOwnerTest -----");
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerServicePort(Optional.of(0));
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            configurations[i] = config;

            pulsarServices[i] = new PulsarService(config);
            pulsarServices[i].setShutdownService(new NoOpShutdownService());
            pulsarServices[i].start();

            pulsarAdmins[i] = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarServices[i].getWebServiceAddress())
                    .build();
        }
        Thread.sleep(1000);
    }

    @Test
    public void testConnectToInvalidateBundleCacheBroker() throws Exception {
        pulsarAdmins[0].clusters().createCluster("my-cluster", new ClusterData(pulsarServices[0].getWebServiceAddress()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Sets.newHashSet("my-cluster"));
        pulsarAdmins[0].tenants().createTenant("my-tenant", tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace("my-tenant/my-ns", 16);

        Assert.assertEquals(pulsarAdmins[0].namespaces().getPolicies("my-tenant/my-ns").bundles.getNumBundles(), 16);

        final String topic1 = "persistent://my-tenant/my-ns/topic-1";
        final String topic2 = "persistent://my-tenant/my-ns/topic-2";

        // Do topic lookup here for broker to own namespace bundles
        String serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
        String serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);

        while (serviceUrlForTopic1.equals(serviceUrlForTopic2)) {
            // Retry for bundle distribution, should make sure bundles for topic1 and topic2 are maintained in different brokers.
            pulsarAdmins[0].namespaces().unload("my-tenant/my-ns");
            serviceUrlForTopic1 = pulsarAdmins[0].lookups().lookupTopic(topic1);
            serviceUrlForTopic2 = pulsarAdmins[0].lookups().lookupTopic(topic2);
        }
        // All brokers will invalidate bundles cache after namespace bundle split
        pulsarAdmins[0].namespaces().splitNamespaceBundle("my-tenant/my-ns",
                pulsarServices[0].getNamespaceService().getBundle(TopicName.get(topic1)).getBundleRange(),
                true, null);

        PulsarClient client = PulsarClient.builder().
                serviceUrl(serviceUrlForTopic1)
                .build();

        // Check connect to a topic which owner broker invalidate all namespace bundles cache
        Consumer<byte[]> consumer = client.newConsumer().topic(topic2).subscriptionName("test").subscribe();
        Assert.assertTrue(consumer.isConnected());
    }
}
