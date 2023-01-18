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
package org.apache.pulsar.io;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.PulsarFunctionTestTemporaryDirectory;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.PulsarWorkerService.PulsarClientCreator;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test Pulsar function TLS authentication
 */
@Test(groups = "broker-io")
public class PulsarFunctionTlsTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    PulsarWorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String workerId;
    WorkerServer workerServer;
    PulsarAdmin functionAdmin;
    private final List<String> namespaceList = new LinkedList<>();

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionTlsTest.class);
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(ServiceConfiguration.class);
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser", "admin");
        config.setSuperUserRoles(superUsers);
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthorizationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(true);
        config.setAdvertisedAddress("localhost");

        PulsarAdmin admin = mock(PulsarAdmin.class);
        Tenants tenants = mock(Tenants.class);
        when(admin.tenants()).thenReturn(tenants);
        Set<String> admins = Sets.newHashSet("superUser", "admin");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(admins, null);
        when(tenants.getTenantInfo(any())).thenReturn(tenantInfo);
        Namespaces namespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(namespaces);
        when(namespaces.getNamespaces(any())).thenReturn(namespaceList);

        functionsWorkerService = spy(createPulsarFunctionWorker(config, admin));
        doNothing().when(functionsWorkerService).initAsStandalone(any(WorkerConfig.class));
        when(functionsWorkerService.getBrokerAdmin()).thenReturn(admin);
        functionsWorkerService.init(workerConfig, null, false);

        AuthenticationService authenticationService = new AuthenticationService(config);
        AuthorizationService authorizationService = new AuthorizationService(config, mock(PulsarResources.class));
        when(functionsWorkerService.getAuthenticationService()).thenReturn(authenticationService);
        when(functionsWorkerService.getAuthorizationService()).thenReturn(authorizationService);
        when(functionsWorkerService.isInitialized()).thenReturn(true);

        // mock: once authentication passes, function should return response: function already exist
        FunctionMetaDataManager dataManager = mock(FunctionMetaDataManager.class);
        when(dataManager.containsFunction(any(), any(), any())).thenReturn(true);
        when(functionsWorkerService.getFunctionMetaDataManager()).thenReturn(dataManager);

        workerServer = new WorkerServer(functionsWorkerService, authenticationService);
        workerServer.start();
        Thread.sleep(2000);
        String functionTlsUrl = String.format("https://%s:%s",
                functionsWorkerService.getWorkerConfig().getWorkerHostname(), workerServer.getListenPortHTTPS().get());

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        functionAdmin = PulsarAdmin.builder().serviceHttpUrl(functionTlsUrl)
                .tlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH).allowTlsInsecureConnection(true)
                .authentication(authTls).build();

        Thread.sleep(100);
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        try {
            functionAdmin.close();
            functionsWorkerService.stop();
            workerServer.stop();
            bkEnsemble.stop();
        } finally {
            if (tempDirectory != null) {
                tempDirectory.delete();
            }
        }
    }

    private PulsarWorkerService createPulsarFunctionWorker(ServiceConfiguration config,
                                                           PulsarAdmin mockPulsarAdmin) {
        workerConfig = new WorkerConfig();
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper()
                        .convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort().get());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePort().get());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
        workerConfig.setWorkerPort(null);
        workerConfig.setPulsarFunctionsCluster(config.getClusterName());
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        this.workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId(workerId);

        workerConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        workerConfig.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH));
        workerConfig.setUseTls(true);
        workerConfig.setTlsAllowInsecureConnection(true);
        workerConfig.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);

        workerConfig.setWorkerPortTls(0);
        workerConfig.setTlsEnabled(true);
        workerConfig.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        workerConfig.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        PulsarWorkerService workerService = new PulsarWorkerService(new PulsarClientCreator() {
            @Override
            public PulsarAdmin newPulsarAdmin(String pulsarServiceUrl, WorkerConfig workerConfig) {
                return mockPulsarAdmin;
            }

            @Override
            public PulsarClient newPulsarClient(String pulsarServiceUrl, WorkerConfig workerConfig) {
                return null;
            }
        });

        return workerService;
    }

    @Test
    public void testWorkerServerNonTlsNotStarted() {
        assertFalse(workerServer.getListenPortHTTP().isPresent(), "Non-TLS worker service should not be set.");
    }

    @Test
    public void testAuthorization() {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        namespaceList.add(replNamespace);

        String jarFilePathUrl = String.format("%s:%s", org.apache.pulsar.common.functions.Utils.FILE,
                PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        FunctionConfig functionConfig = createFunctionConfig(jarFilePathUrl, tenant, namespacePortion,
                functionName, "my.*", sinkTopic, subscriptionName);

        try {
            functionAdmin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
            fail("Authentication should pass but call should fail with function already exist");
        } catch (PulsarAdminException e) {
            assertTrue(e.getMessage().contains("already exists"));
        }

    }

    protected static FunctionConfig createFunctionConfig(String jarFile,
                                                         String tenant,
                                                         String namespace,
                                                         String functionName,
                                                         String sourceTopic,
                                                         String sinkTopic,
                                                         String subscriptionName) {

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

        return functionConfig;
    }

}
