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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.utils.ResourceUtils;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.reflect.WhiteboxImpl;

@Slf4j
public abstract class NetworkErrorTestBase extends TestRetrySupport {

    protected final static String CA_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    protected final static String BROKER_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    protected final static String BROKER_KEY_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");
    protected final String defaultTenant = "public";
    protected final String defaultNamespace = defaultTenant + "/default";
    protected final String cluster1 = "r1";
    protected URL url1;
    protected URL urlTls1;
    protected URL url2;
    protected URL urlTls2;
    protected ServiceConfiguration config1 = new ServiceConfiguration();
    protected ServiceConfiguration config2 = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk1;
    protected Ipv4Proxy metadataZKProxy;
    protected LocalBookkeeperEnsemble bkEnsemble1;
    protected PulsarService pulsar1;
    protected PulsarService pulsar2;
    protected BrokerService broker1;
    protected BrokerService broker2;
    protected PulsarAdmin admin1;
    protected PulsarAdmin admin2;
    protected PulsarClient client1;
    protected PulsarClient client2;

    private final static AtomicReference<String> preferBroker = new AtomicReference<>();

    protected void startZKAndBK() throws Exception {
        // Start ZK & BK.
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();

        metadataZKProxy = new Ipv4Proxy(getOneFreePort(), "127.0.0.1", bkEnsemble1.getZookeeperPort());
        metadataZKProxy.startup();
    }

    protected void startBrokers() throws Exception {
        // Start brokers.
        setConfigDefaults(config1, cluster1, metadataZKProxy.getLocalPort());
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        broker1 = pulsar1.getBrokerService();
        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());

        setConfigDefaults(config2, cluster1, bkEnsemble1.getZookeeperPort());
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        broker2 = pulsar2.getBrokerService();
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());

        log.info("broker-1: {}, broker-2: {}", broker1.getListenPort(), broker2.getListenPort());
    }

    public static int getOneFreePort() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();
        serverSocket.close();
        return port;
    }

    protected void startAdminClient() throws Exception {
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();
    }

    protected void startPulsarClient() throws Exception{
        ClientBuilder clientBuilder1 = PulsarClient.builder().serviceUrl(url1.toString());
        client1 = initClient(clientBuilder1);
        ClientBuilder clientBuilder2 = PulsarClient.builder().serviceUrl(url2.toString());
        client2 = initClient(clientBuilder2);
    }

    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        admin1.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin1.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1)));
        admin1.namespaces().createNamespace(defaultNamespace, Sets.newHashSet(cluster1));
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting OneWayReplicatorTestBase::setup ---");

        startZKAndBK();

        startBrokers();

        startAdminClient();

        createDefaultTenantsAndClustersAndNamespace();

        startPulsarClient();

        Thread.sleep(100);
        log.info("--- OneWayReplicatorTestBase::setup completed ---");
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName, int zkPort) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + zkPort);
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + zkPort + "/config_meta");
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
        config.setForceDeleteNamespaceAllowed(true);
        config.setLoadManagerClassName(PreferBrokerModularLoadManager.class.getName());
        config.setMetadataStoreSessionTimeoutMillis(5000);
        config.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        config.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
    }

    @Override
    protected void cleanup() throws Exception {
        // shutdown.
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");

        // Stop brokers.
        if (client1 != null) {
            client1.close();
            client1 = null;
        }
        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (client2 != null) {
            client2.close();
            client2 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }
        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }

        // Stop ZK and BK.
        if (bkEnsemble1 != null) {
            bkEnsemble1.stop();
            bkEnsemble1 = null;
        }
        if (metadataZKProxy != null) {
            metadataZKProxy.stop();
        }
        if (brokerConfigZk1 != null) {
            brokerConfigZk1.stop();
            brokerConfigZk1 = null;
        }

        // Reset configs.
        config1 = new ServiceConfiguration();
        preferBroker.set(null);
    }

    protected PulsarClient initClient(ClientBuilder clientBuilder) throws Exception {
        return clientBuilder.build();
    }

    protected static class PreferBrokerModularLoadManager extends ModularLoadManagerImpl {

        @Override
        public String setNamespaceBundleAffinity(String bundle, String broker) {
            if (StringUtils.isNotBlank(broker)) {
                return broker;
            }
            Set<String> availableBrokers = NetworkErrorTestBase.getAvailableBrokers(super.pulsar);
            String prefer = preferBroker.get();
            if (availableBrokers.contains(prefer)) {
                return prefer;
            } else {
                return null;
            }
        }
    }

    protected static class PreferExtensibleLoadManager extends ExtensibleLoadManagerImpl {

        @Override
        public CompletableFuture<Optional<String>> selectAsync(ServiceUnitId bundle,
                                                               Set<String> excludeBrokerSet,
                                                               LookupOptions options) {
            Set<String> availableBrokers = NetworkErrorTestBase.getAvailableBrokers(super.pulsar);
            String prefer = preferBroker.get();
            if (availableBrokers.contains(prefer)) {
                return CompletableFuture.completedFuture(Optional.of(prefer));
            } else {
                return super.selectAsync(bundle, excludeBrokerSet, options);
            }
        }
    }

    public void setPreferBroker(PulsarService target) {
        for (PulsarService pulsar : Arrays.asList(pulsar1, pulsar2)) {
            for (String broker : getAvailableBrokers(pulsar)) {
                if (broker.endsWith(target.getBrokerListenPort().orElse(-1) + "")
                        || broker.endsWith(target.getListenPortHTTPS().orElse(-1) + "")
                        || broker.endsWith(target.getListenPortHTTP().orElse(-1) + "")
                        || broker.endsWith(target.getBrokerListenPortTls().orElse(-1) + "")) {
                    preferBroker.set(broker);
                }
            }
        }
    }

    public static Set<String> getAvailableBrokers(PulsarService pulsar) {
        Object loadManagerWrapper = pulsar.getLoadManager().get();
        Object loadManager = WhiteboxImpl.getInternalState(loadManagerWrapper, "loadManager");
        if (loadManager instanceof ModularLoadManagerImpl) {
            return ((ModularLoadManagerImpl) loadManager).getAvailableBrokers();
        } else if (loadManager instanceof ExtensibleLoadManagerImpl) {
            return new HashSet<>(((ExtensibleLoadManagerImpl) loadManager).getBrokerRegistry()
                    .getAvailableBrokersAsync().join());
        } else {
            throw new RuntimeException("Not support for the load manager: " + loadManager.getClass().getName());
        }
    }

    public void clearPreferBroker() {
        preferBroker.set(null);
    }
}
