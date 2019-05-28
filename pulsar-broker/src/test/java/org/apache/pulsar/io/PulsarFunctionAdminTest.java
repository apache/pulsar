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
package org.apache.pulsar.io;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarFunctionAdminTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    WorkerServer functionsWorkerServer;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String primaryHost;

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int brokerWebServicePort = PortManager.nextFreePort();
    private final int brokerWebServiceTlsPort = PortManager.nextFreePort();
    private final int brokerServicePort = PortManager.nextFreePort();
    private final int brokerServiceTlsPort = PortManager.nextFreePort();
    private final int workerServicePort = PortManager.nextFreePort();

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionAdminTest.class);

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        String brokerServiceUrl = "https://127.0.0.1:" + brokerWebServiceTlsPort;

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.ofNullable(brokerWebServicePort));
        config.setWebServicePortTls(Optional.ofNullable(brokerWebServiceTlsPort));
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(Optional.ofNullable(brokerServicePort));
        config.setBrokerServicePortTls(Optional.ofNullable(brokerServiceTlsPort));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());


        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(true);


        functionsWorkerService = createPulsarFunctionWorker(config);
        urlTls = new URL(brokerServiceUrl);
        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService);
        pulsar.start();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).tlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH)
                        .allowTlsInsecureConnection(true).authentication(authTls).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = String.format("http://%s:%d", "localhost", brokerWebServicePort);

        // update cluster metadata
        ClusterData clusterData = new ClusterData(urlTls.toString());
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        if (isNotBlank(workerConfig.getClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getClientAuthenticationParameters())) {
            clientBuilder.enableTls(workerConfig.isUseTls());
            clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
            clientBuilder.authentication(workerConfig.getClientAuthenticationPlugin(),
                    workerConfig.getClientAuthenticationParameters());
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = new TenantInfo();
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        pulsarClient.close();
        admin.close();
        functionsWorkerService.stop();
        pulsar.close();
        bkEnsemble.stop();
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("use"));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls().get());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePortTls().get());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
        workerConfig.setWorkerPort(workerServicePort);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        workerConfig.setWorkerHostname(hostname);
        workerConfig
                .setWorkerId("c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort());

        workerConfig.setClientAuthenticationPlugin(AuthenticationTls.class.getName());
        workerConfig.setClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH));
        workerConfig.setUseTls(true);
        workerConfig.setTlsAllowInsecureConnection(true);
        workerConfig.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);

        return new WorkerService(workerConfig);
    }
}