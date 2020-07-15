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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test Pulsar function TLS authentication
 *
 */
public class PulsarFunctionTlsTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String workerId;
    WorkerServer workerServer;
    PulsarAdmin functionAdmin;
    private List<String> namespaceList = new LinkedList<>();

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionTlsTest.class);

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthorizationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(true);
        config.setAdvertisedAddress("localhost");
        functionsWorkerService = spy(createPulsarFunctionWorker(config));
        AuthenticationService authenticationService = new AuthenticationService(config);
        AuthorizationService authorizationService = new AuthorizationService(config, mock(ConfigurationCacheService.class));
        when(functionsWorkerService.getAuthenticationService()).thenReturn(authenticationService);
        when(functionsWorkerService.getAuthorizationService()).thenReturn(authorizationService);
        when(functionsWorkerService.isInitialized()).thenReturn(true);

        PulsarAdmin admin = mock(PulsarAdmin.class);
        Tenants tenants = mock(Tenants.class);
        when(admin.tenants()).thenReturn(tenants);
        when(functionsWorkerService.getBrokerAdmin()).thenReturn(admin);
        Set<String> admins = Sets.newHashSet("superUser");
        TenantInfo tenantInfo = new TenantInfo(admins, null);
        when(tenants.getTenantInfo(any())).thenReturn(tenantInfo);
        Namespaces namespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(namespaces);
        when(namespaces.getNamespaces(any())).thenReturn(namespaceList);

        // mock: once authentication passes, function should return response: function already exist
        FunctionMetaDataManager dataManager = mock(FunctionMetaDataManager.class);
        when(dataManager.containsFunction(any(), any(), any())).thenReturn(true);
        when(functionsWorkerService.getFunctionMetaDataManager()).thenReturn(dataManager);

        workerServer = new WorkerServer(functionsWorkerService);
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

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        functionAdmin.close();
        bkEnsemble.stop();
        workerServer.stop();
        functionsWorkerService.stop();
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort().get());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePort().get());
        workerConfig.setFailureCheckFreqMs(100);
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setClusterCoordinationTopicName("coordinate");
        workerConfig.setFunctionAssignmentTopicName("assignment");
        workerConfig.setFunctionMetadataTopicName("metadata");
        workerConfig.setInstanceLivenessCheckFreqMs(100);
        workerConfig.setWorkerPort(0);
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

        return new WorkerService(workerConfig);
    }

    @Test
    public void testAuthorization() throws Exception {

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

    protected static FunctionConfig createFunctionConfig(String jarFile, String tenant, String namespace, String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {

        File file = new File(jarFile);
        try {
            ClassLoaderUtils.loadJar(file);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to load user jar " + file, e);
        }
        String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);
        Class<?> typeArg = byte[].class;

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
