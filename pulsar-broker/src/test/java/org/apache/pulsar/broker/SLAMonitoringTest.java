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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SLAMonitoringTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ExecutorService executor;

    private static final int BROKER_COUNT = 5;
    private final int[] brokerWebServicePorts = new int[BROKER_COUNT];
    private final int[] brokerNativeBrokerPorts = new int[BROKER_COUNT];
    private final URL[] brokerUrls = new URL[BROKER_COUNT];
    private final PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];
    private final PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    private final ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];

    @BeforeClass(alwaysRun = true)
    void setup() throws Exception {
        executor =
                new ThreadPoolExecutor(5,
                        20,
                        30,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>());
        log.info("---- Initializing SLAMonitoringTest -----");
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerShutdownTimeoutMs(0L);
            config.setBrokerServicePort(Optional.of(0));
            config.setBrokerShutdownTimeoutMs(0L);
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            configurations[i] = config;

            pulsarServices[i] = new PulsarService(config);
            pulsarServices[i].start();

            brokerWebServicePorts[i] = pulsarServices[i].getListenPortHTTP().get();
            brokerNativeBrokerPorts[i] = pulsarServices[i].getBrokerListenPort().get();
            brokerUrls[i] = new URL(pulsarServices[i].getWebServiceAddress());
            pulsarAdmins[i] = PulsarAdmin.builder().serviceHttpUrl(brokerUrls[i].toString()).build();
        }

        Thread.sleep(100);

        createTenant(pulsarAdmins[BROKER_COUNT - 1]);
        for (int i = 0; i < BROKER_COUNT; i++) {
            String topic = String.format("%s/%s/%s:%s", NamespaceService.SLA_NAMESPACE_PROPERTY, "my-cluster",
                    pulsarServices[i].getAdvertisedAddress(), brokerWebServicePorts[i]);
            pulsarAdmins[0].namespaces().createNamespace(topic);
        }
    }

    private void createTenant(PulsarAdmin pulsarAdmin)
            throws PulsarAdminException {
        ClusterData clusterData = ClusterData.builder()
                .serviceUrl(pulsarAdmin.getServiceUrl())
                .build();
        pulsarAdmins[0].clusters().createCluster("my-cluster", clusterData);
        Set<String> allowedClusters = new HashSet<>();
        allowedClusters.add("my-cluster");
        TenantInfo adminConfig = TenantInfo.builder()
                .adminRoles(Collections.singleton(""))
                .allowedClusters(allowedClusters)
                .build();
        pulsarAdmin.tenants().createTenant("sla-monitor", adminConfig);
    }

    @AfterClass(alwaysRun = true)
    public void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdownNow();
        executor = null;

        for (int i = 0; i < BROKER_COUNT; i++) {
            pulsarAdmins[i].close();
            pulsarServices[i].close();
        }

        bkEnsemble.stop();
    }

    @Test
    public void testOwnershipAfterSetup() {
        for (int i = 0; i < BROKER_COUNT; i++) {
            try {
                assertTrue(pulsarServices[0].getNamespaceService().registerSLANamespace());
            } catch (PulsarServerException e) {
                e.printStackTrace();
                log.error("Exception occurred", e);
                fail("SLA Namespace should have been owned by the broker, Exception.", e);
            }
        }
    }

    @Test
    public void testOwnedNamespaces() {
        testOwnershipViaAdminAfterSetup();
        try {
            for (int i = 0; i < BROKER_COUNT; i++) {
                List<String> list = pulsarAdmins[i].brokers().getActiveBrokers("my-cluster");
                Assert.assertNotNull(list);
                Assert.assertEquals(list.size(), BROKER_COUNT);

                Map<String, NamespaceOwnershipStatus> nsMap = pulsarAdmins[i].brokers().getOwnedNamespaces("my-cluster",
                        list.get(0));
                Assert.assertEquals(nsMap.size(), 3);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Hearbeat namespace and SLA namespace should be owned by the broker");
        }
    }

    @Test
    public void testOwnershipViaAdminAfterSetup() {
        for (int i = 0; i < BROKER_COUNT; i++) {
            try {
                String topic = String.format("persistent://%s/%s/%s:%s/%s",
                        NamespaceService.SLA_NAMESPACE_PROPERTY, "my-cluster", pulsarServices[i].getAdvertisedAddress(),
                        brokerWebServicePorts[i], "my-topic");
                assertEquals(pulsarAdmins[0].lookups().lookupTopic(topic),
                        "pulsar://" + pulsarServices[i].getAdvertisedAddress() + ":" + brokerNativeBrokerPorts[i]);
            } catch (Exception e) {
                e.printStackTrace();
                fail("SLA Namespace should have been owned by the broker(" + "pulsar://"
                        + pulsarServices[i].getAdvertisedAddress()
                        + ":" + brokerNativeBrokerPorts[i] + ")");
            }
        }
    }

    @Test
    public void testUnloadIfBrokerCrashes() {
        int crashIndex = BROKER_COUNT / 2;
        log.info("Trying to close the broker at index = {}", crashIndex);

        try {
            pulsarServices[crashIndex].close();
        } catch (PulsarServerException e) {
            e.printStackTrace();
            fail("Should be a able to close the broker index " + crashIndex + " Exception: " + e);
        }

        String topic = String.format("persistent://%s/%s/%s:%s/%s", NamespaceService.SLA_NAMESPACE_PROPERTY,
                "my-cluster", pulsarServices[crashIndex].getAdvertisedAddress(), brokerWebServicePorts[crashIndex],
                "my-topic");

        log.info("Lookup for namespace {}", topic);

        String broker = null;
        try {
            broker = pulsarAdmins[BROKER_COUNT - 1].lookups().lookupTopic(topic);
            log.info("{} Namespace is owned by {}", topic, broker);
            assertNotEquals(broker,
                    "pulsar://" + pulsarServices[crashIndex].getAdvertisedAddress() + ":"
                            + brokerNativeBrokerPorts[crashIndex]);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
            fail("The SLA Monitor namespace should be owned by some other broker");
        }

        // Check if the namespace is properly unloaded and reowned by the broker
        try {
            pulsarServices[crashIndex] = new PulsarService(configurations[crashIndex]);
            pulsarServices[crashIndex].start();

            // Port for the broker will have changed since it's dynamically allocated
            brokerNativeBrokerPorts[crashIndex] = pulsarServices[crashIndex].getBrokerListenPort().get();
        } catch (PulsarServerException e) {
            e.printStackTrace();
            fail("The broker should be able to start without exception");
        }

        try {
            broker = pulsarAdmins[0].lookups().lookupTopic(topic);
            log.info("{} Namespace is re-owned by {}", topic, broker);
            assertEquals(broker,
                    "pulsar://" + pulsarServices[crashIndex].getAdvertisedAddress() + ":"
                            + brokerNativeBrokerPorts[crashIndex]);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
            fail("The SLA Monitor namespace should be reowned by the broker" + broker);
        }

        try {
            pulsarServices[crashIndex].close();
        } catch (PulsarServerException e) {
            e.printStackTrace();
            fail("The broker should be able to stop without exception");
        }
    }

}
