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
package org.apache.pulsar.functions.worker;

import static org.testng.Assert.assertEquals;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "functions-worker")
public class PulsarFunctionTlsTest {

    protected static final int BROKER_COUNT = 2;

    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    protected ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];
    protected PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];
    protected PulsarService leaderPulsar;
    protected PulsarAdmin leaderAdmin;
    protected String testCluster = "my-cluster";
    protected String testTenant = "my-tenant";
    protected String testNamespace = testTenant + "/my-ns";
    private PulsarFunctionTestTemporaryDirectory[] tempDirectories = new PulsarFunctionTestTemporaryDirectory[BROKER_COUNT];

    @BeforeMethod
    void setup() throws Exception {
        log.info("---- Initializing TopicOwnerTest -----");
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            int brokerPort = PortManager.nextFreePort();
            int webPort = PortManager.nextFreePort();

            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerShutdownTimeoutMs(0L);
            config.setWebServicePort(Optional.empty());
            config.setWebServicePortTls(Optional.of(webPort));
            config.setBrokerServicePort(Optional.empty());
            config.setBrokerServicePortTls(Optional.of(brokerPort));
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            Set<String> superUsers = Sets.newHashSet("superUser", "admin");
            config.setSuperUserRoles(superUsers);
            Set<String> providers = new HashSet<>();
            providers.add(AuthenticationProviderTls.class.getName());
            config.setAuthenticationEnabled(true);
            config.setAuthorizationEnabled(true);
            config.setAuthenticationProviders(providers);
            config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
            config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
            config.setTlsAllowInsecureConnection(true);
            config.setBrokerClientTlsEnabled(true);
            config.setBrokerClientTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);
            config.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
            config.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + ",tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);
            config.setFunctionsWorkerEnabled(true);
            config.setTlsEnabled(true);

            WorkerConfig workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(config, null);
            tempDirectories[i] = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
            tempDirectories[i].useTemporaryDirectoriesForWorkerConfig(workerConfig);
            workerConfig.setPulsarFunctionsNamespace("public/functions");
            workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
            workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
            workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                    new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
            workerConfig.setFailureCheckFreqMs(100);
            workerConfig.setNumFunctionPackageReplicas(1);
            workerConfig.setClusterCoordinationTopicName("coordinate");
            workerConfig.setFunctionAssignmentTopicName("assignment");
            workerConfig.setFunctionMetadataTopicName("metadata");
            workerConfig.setInstanceLivenessCheckFreqMs(100);
            workerConfig.setBrokerClientAuthenticationEnabled(true);
            workerConfig.setTlsEnabled(true);
            workerConfig.setUseTls(true);
            WorkerService fnWorkerService = WorkerServiceLoader.load(workerConfig);

            configurations[i] = config;

            pulsarServices[i] = new PulsarService(
                config, workerConfig, Optional.of(fnWorkerService), code -> {});
            pulsarServices[i].start();

            // Sleep until pulsarServices[0] becomes leader, this way we can spy namespace bundle assignment easily.
            while (i == 0 && !pulsarServices[0].getLeaderElectionService().isLeader()) {
                Thread.sleep(10);
            }

            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
            authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
            Authentication authTls = new AuthenticationTls();
            authTls.configure(authParams);

            pulsarAdmins[i] = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarServices[i].getWebServiceAddressTls())
                .tlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH)
                .allowTlsInsecureConnection(true)
                .authentication(authTls)
                .build();
        }
        leaderPulsar = pulsarServices[0];
        leaderAdmin = pulsarAdmins[0];
        Thread.sleep(1000);

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(testCluster))
                .build();
        pulsarAdmins[0].tenants().createTenant(testTenant, tenantInfo);
        pulsarAdmins[0].namespaces().createNamespace(testNamespace, 16);
    }

    @AfterMethod(alwaysRun = true)
    void tearDown() throws Exception {
        try {
            for (int i = 0; i < BROKER_COUNT; i++) {
                if (pulsarServices[i] != null) {
                    pulsarServices[i].close();
                }
                if (pulsarAdmins[i] != null) {
                    pulsarAdmins[i].close();
                }
            }
            bkEnsemble.stop();
        } finally {
            for (int i = 0; i < BROKER_COUNT; i++) {
                if (tempDirectories[i] != null) {
                    tempDirectories[i].delete();
                }
            }
        }
    }

    @Test
    public void testFunctionsCreation() throws Exception {

        String jarFilePathUrl = String.format("%s:%s", org.apache.pulsar.common.functions.Utils.FILE,
                PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        for (int i = 0; i < BROKER_COUNT; i++) {
            String functionName = "function-" + i;
            FunctionConfig functionConfig = createFunctionConfig(jarFilePathUrl, testTenant, "my-ns",
                functionName, "my.*", "sink-topic-" + i, "sub-" + i);

            log.info(" -------- Start test function : {}", functionName);

            pulsarAdmins[i].functions().createFunctionWithUrl(
                functionConfig, jarFilePathUrl
            );

            FunctionConfig config = pulsarAdmins[i].functions().getFunction(testTenant, "my-ns", functionName);
            assertEquals(config.getTenant(), testTenant);
            assertEquals(config.getNamespace(), "my-ns");
            assertEquals(config.getName(), functionName);

            pulsarAdmins[i].functions().deleteFunction(config.getTenant(), config.getNamespace(), config.getName());
        }
    }

    protected static FunctionConfig createFunctionConfig(
        String jarFile,
        String tenant,
        String namespace,
        String functionName,
        String sourceTopic,
        String sinkTopic,
        String subscriptionName
    ) throws JsonProcessingException {
        File file = new File(jarFile);
        try {
            ClassLoader classLoader = ClassLoaderUtils.loadJar(file);
            if (classLoader instanceof Closeable) {
                ((Closeable) classLoader).close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load user jar " + file, e);
        }
        String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setParallelism(1);
        functionConfig.setClassName(IdentityFunction.class.getName());
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setTopicsPattern(sourceTopicPattern);
        functionConfig.setAutoAck(true);
        functionConfig.setOutput(sinkTopic);

        log.info("Function Config: {}", new ObjectMapper().writerWithDefaultPrettyPrinter()
            .writeValueAsString(functionConfig));

        return functionConfig;
    }

}
