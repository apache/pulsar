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
package org.apache.pulsar.broker;

import java.net.Inet4Address;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.junit.Assert;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class MetadataStoreStuckTest {

    // prefer inet4.
    private static final String LOCALHOST = Inet4Address.getLoopbackAddress().getHostAddress();
    private static final String CLUSTER = "metadata_stuck_cluster";
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = DEFAULT_TENANT + "/default";

    protected LocalBookkeeperEnsemble bkEnsemble;
    protected ServiceConfiguration pulsarConfig;
    protected PulsarService pulsarService;
    protected int brokerWebServicePort;
    protected int brokerServicePort;
    protected String metadataServiceUri;
    protected BookKeeper bookKeeperClient;
    protected String brokerUrl;
    protected String brokerServiceUrl;
    protected PulsarAdmin pulsarAdmin;
    protected PulsarClient pulsarClient;

    @BeforeClass
    protected void setup() throws Exception {
        log.info("--- Start cluster ---");
        startLocalBookie();
        initPulsarConfig();
        startPulsar();
    }

    @AfterClass
    protected void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        silentStopPulsar();
        stopLocalBookie();
    }

    protected void startLocalBookie() throws Exception{
        log.info("Start bookie ");
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        metadataServiceUri = String.format("zk:%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort());
        initBookieClient();
    }

    protected void initBookieClient() throws Exception {
        bookKeeperClient = new BookKeeper(String.format("%s:%s", LOCALHOST, bkEnsemble.getZookeeperPort()));
    }

    protected void stopLocalBookie() {
        log.info("Close bookie client");
        try {
            bookKeeperClient.close();
            // TODO delete bk files.
            // TODO delete zk files.
        } catch (Exception e){
            log.error("Close bookie client fail", e);
        }
        log.info("Stop bookie ");
        try {
            bkEnsemble.stop();
            // TODO delete bk files.
            // TODO delete zk files.
        } catch (Exception e){
            log.error("Stop bookie fail", e);
        }
    }

    protected void initPulsarConfig() throws Exception{
        pulsarConfig = new ServiceConfiguration();
        pulsarConfig.setAdvertisedAddress(LOCALHOST);
        pulsarConfig.setMetadataStoreUrl(metadataServiceUri);
        pulsarConfig.setClusterName(CLUSTER);
        pulsarConfig.setTransactionCoordinatorEnabled(false);
        pulsarConfig.setAllowAutoTopicCreation(true);
        pulsarConfig.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        pulsarConfig.setAutoSkipNonRecoverableData(true);
    }

    protected void startPulsar() throws Exception {
        log.info("Start pulsar ");
        pulsarService = new PulsarService(pulsarConfig);
        pulsarService.start();
        brokerWebServicePort = pulsarService.getListenPortHTTP().get();
        brokerServicePort = pulsarService.getBrokerListenPort().get();
        brokerUrl = String.format("http://%s:%s", LOCALHOST, brokerWebServicePort);
        brokerServiceUrl = String.format("pulsar://%s:%s", LOCALHOST, brokerServicePort);
        initPulsarAdmin();
        initPulsarClient();
        initDefaultNamespace();
    }

    protected void silentStopPulsar() throws Exception {
        log.info("Close pulsar client ");
        try {
            pulsarClient.close();
        }catch (Exception e){
            log.error("Close pulsar client fail", e);
        }
        log.info("Close pulsar admin ");
        try {
            pulsarAdmin.close();
        }catch (Exception e){
            log.error("Close pulsar admin fail", e);
        }
        log.info("Stop pulsar service ");
        try {
            pulsarService.close();
        }catch (Exception e){
            log.error("Stop pulsar service fail", e);
        }
    }

    protected void initPulsarAdmin() throws Exception {
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl).build();
    }

    protected void initPulsarClient() throws Exception {
        pulsarClient = PulsarClient.builder().serviceUrl(brokerServiceUrl).build();
    }

    protected void initDefaultNamespace() throws Exception {
        if (!pulsarAdmin.clusters().getClusters().contains(CLUSTER)) {
            pulsarAdmin.clusters().createCluster(CLUSTER, ClusterData.builder().serviceUrl(brokerUrl).build());
        }
        if (!pulsarAdmin.tenants().getTenants().contains(DEFAULT_TENANT)){
            pulsarAdmin.tenants().createTenant(DEFAULT_TENANT,
                    TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER)).build());
        }
        if (!pulsarAdmin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
            pulsarAdmin.namespaces().createNamespace(DEFAULT_NAMESPACE, Collections.singleton(CLUSTER));
        }
    }


    @Test
    public void testSchemaLedgerLost() throws Exception {
        OwnershipCache ownershipCache = pulsarService.getNamespaceService().getOwnershipCache();
        NamespaceName namespaceName = NamespaceName.get(DEFAULT_NAMESPACE);
        List<NamespaceBundle> bundles = pulsarService.getNamespaceService().getNamespaceBundleFactory()
                .getBundles(namespaceName).getBundles();
        String topicName = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NAMESPACE + "/tp_");
        final NamespaceBundle bundle0 = bundles.get(0);
        final NamespaceBundle bundle1 = bundles.get(1);

        // task: lookup.
        final AtomicBoolean lookupTaskRunning = new AtomicBoolean();
        final Thread lookupTask = new Thread(() -> {
            pulsarService.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                    .getPartitionedTopicMetadataAsync(TopicName.get(topicName), true).join();
            lookupTaskRunning.set(true);
        });

        // task: create topic.
        Thread createTopicTask = new Thread(() -> {
            acquiringOwnership(ownershipCache, bundle0).thenCompose(res -> {
                log.info("Acquire ownership_0: {}, result: {}", bundle0, res);
                return acquiringOwnership(ownershipCache, bundle1);
            }).thenCompose(res -> {
                log.info("Acquire ownership_1: {}, result: {}", bundle1, res);
                lookupTask.start();
                Awaitility.await().untilAsserted(() -> {
                    Assert.assertTrue(lookupTaskRunning.get());
                });
                try {Thread.sleep(1000);} catch (InterruptedException e) {}
                List<NamespaceBundle> bundleList = pulsarService.getNamespaceService().getNamespaceBundleFactory()
                        .getBundles(namespaceName).getBundles();
                log.info("get bundle list: {}", bundleList);
                return CompletableFuture.completedFuture(res);
            }).join();
        });

        // Verify all tasks will be finished in time.
        createTopicTask.start();
        createTopicTask.join();
        lookupTask.join();
    }

    private CompletableFuture<NamespaceEphemeralData> acquiringOwnership(OwnershipCache ownershipCache,
                                                                         NamespaceBundle bundle) {
        try {
            return ownershipCache.tryAcquiringOwnership(bundle);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}