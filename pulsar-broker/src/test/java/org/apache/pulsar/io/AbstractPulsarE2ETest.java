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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarIOBatchDataGeneratorNar;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarIODataGeneratorNar;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.FileServer;
import org.apache.pulsar.functions.worker.PulsarFunctionTestTemporaryDirectory;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.ToString;

public abstract class AbstractPulsarE2ETest {

	final String tenant = "external-repl-prop";
	
    LocalBookkeeperEnsemble bkEnsemble;
    ServiceConfiguration config;
    WorkerConfig workerConfig;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    PulsarWorkerService functionsWorkerService;
    String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;
    PulsarFunctionTestTemporaryDirectory tempDirectory;
    
    protected final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    protected final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    protected final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    protected final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";
    protected final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    
    public static final Logger log = LoggerFactory.getLogger(AbstractPulsarE2ETest.class);
    protected FileServer fileServer;
    
    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }
    
    @BeforeMethod
    public void setup(Method method) throws Exception {
        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser", "admin");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setTlsAllowInsecureConnection(true);
        config.setAdvertisedAddress("localhost");

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(providers);

        config.setAuthorizationEnabled(true);
        config.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());

        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

        config.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        config.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + "," + "tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);
        config.setBrokerClientTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        config.setBrokerClientTlsEnabled(true);
        config.setAllowAutoTopicCreationType("non-partitioned");

        System.setProperty(JAVA_INSTANCE_JAR_PROPERTY,
                FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        functionsWorkerService = createPulsarFunctionWorker(config);

        // populate builtin connectors folder
        if (Arrays.asList(method.getAnnotation(Test.class).groups()).contains("builtin")) {
            File connectorsDir = new File(workerConfig.getConnectorsDirectory());

            File file = getPulsarIODataGeneratorNar();
            Files.copy(file.toPath(), new File(connectorsDir, file.getName()).toPath());

            file = getPulsarIOBatchDataGeneratorNar();
            Files.copy(file.toPath(), new File(connectorsDir, file.getName()).toPath());
        }

        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, workerConfig, functionWorkerService, (exitCode) -> {});
        pulsar.start();

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddressTls())
                        .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                        .allowTlsInsecureConnection(true).authentication(authTls).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = String.format("http://%s:%d", "localhost", pulsar.getListenPortHTTP().get());

        // update cluster metadata
        ClusterData clusterData = new ClusterData(pulsar.getBrokerServiceUrlTls());
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        if (isNotBlank(workerConfig.getBrokerClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getBrokerClientAuthenticationParameters())) {
            clientBuilder.enableTls(workerConfig.isUseTls());
            clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
            clientBuilder.authentication(workerConfig.getBrokerClientAuthenticationPlugin(),
                    workerConfig.getBrokerClientAuthenticationParameters());
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = new TenantInfo();
        propAdmin.getAdminRoles().add("superUser");
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        assertTrue(getPulsarIODataGeneratorNar().exists(), "pulsar-io-data-generator.nar file "
                + getPulsarIODataGeneratorNar().getAbsolutePath() + " doesn't exist.");
        assertTrue(getPulsarIOBatchDataGeneratorNar().exists(), "pulsar-io-batch-data-generator.nar file "
                + getPulsarIOBatchDataGeneratorNar().getAbsolutePath() + " doesn't exist.");
        assertTrue(getPulsarApiExamplesJar().exists(), "pulsar-functions-api-examples.jar file "
                + getPulsarApiExamplesJar().getAbsolutePath() + " doesn't exist.");

        // setting up simple web server to test submitting function via URL
        fileServer = new FileServer();
        fileServer.serveFile("/pulsar-io-data-generator.nar", getPulsarIODataGeneratorNar());
        fileServer.serveFile("/pulsar-io-batch-data-generator.nar", getPulsarIOBatchDataGeneratorNar());
        fileServer.serveFile("/pulsar-functions-api-examples.jar", getPulsarApiExamplesJar());
        fileServer.start();

        Awaitility.await().until(() -> functionsWorkerService.getLeaderService().isLeader());
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        try {
        	if (fileServer != null) {
              fileServer.stop();
        	}
        	
        	if (pulsarClient != null) {
              pulsarClient.close();
        	}
        	
        	if (admin != null) {
              admin.close();
        	}
        	
        	if (functionsWorkerService != null) {
              functionsWorkerService.stop();
        	}
        	
        	if (pulsar != null) {
              pulsar.close();
        	}
            
        	if (bkEnsemble != null) {
              bkEnsemble.stop();
        	}
        } finally {
            if (tempDirectory != null) {
                tempDirectory.delete();
            }
        }
    }

    private PulsarWorkerService createPulsarFunctionWorker(ServiceConfiguration config) throws IOException {

        System.setProperty(JAVA_INSTANCE_JAR_PROPERTY,
                FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        workerConfig = new WorkerConfig();
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));        // worker talks to local broker
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
        workerConfig.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        PulsarWorkerService workerService = new PulsarWorkerService();
        workerService.init(workerConfig, null, false);
        return workerService;
    }
    
    protected static String getPrometheusMetrics(int metricsPort) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(String.format("http://%s:%s/metrics", "localhost", metricsPort));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line + System.lineSeparator());
        }
        rd.close();
        return result.toString();
    }

    /**
     * Hacky parsing of Prometheus text format. Sould be good enough for unit tests
     */
    protected static Map<String, Metric> parseMetrics(String metrics) {
        final Map<String, Metric> parsed = new HashMap<>();
        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s(-?[\\d\\w\\.-]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");
        Arrays.asList(metrics.split("\n")).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }
            Matcher matcher = pattern.matcher(line);
            checkArgument(matcher.matches());
            String name = matcher.group(1);
            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.parseDouble(numericValue);
            }
            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }
            parsed.put(name, m);
        });
        return parsed;
    }
    
    @ToString
    static class Metric {
        final Map<String, String> tags = new TreeMap<>();
        double value;
    }
}
