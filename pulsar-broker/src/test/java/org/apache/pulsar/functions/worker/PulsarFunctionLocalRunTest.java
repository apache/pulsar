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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.io.datagenerator.DataGeneratorPrintSink;
import org.apache.pulsar.io.datagenerator.DataGeneratorSource;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarFunctionLocalRunTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/" + CLUSTER + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private static final String CLUSTER = "local";

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
    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionLocalRunTest.class);
    private Thread fileServerThread;
    private static final int fileServerPort = PortManager.nextFreePort();
    private HttpServer fileServer;

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @BeforeMethod
    void setup(Method method) throws Exception {

        // delete all function temp files
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("function");
            }
        });

        for (File file : foundFiles) {
            file.delete();
        }

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        String brokerServiceUrl = "https://127.0.0.1:" + brokerWebServiceTlsPort;

        config = spy(new ServiceConfiguration());
        config.setClusterName(CLUSTER);
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.ofNullable(brokerWebServicePort));
        config.setWebServicePortTls(Optional.ofNullable(brokerWebServiceTlsPort));
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(Optional.ofNullable(brokerServicePort));
        config.setBrokerServicePortTls(Optional.ofNullable(brokerServiceTlsPort));
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
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
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
        propAdmin.getAdminRoles().add("superUser");
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList(CLUSTER)));
        admin.tenants().updateTenant(tenant, propAdmin);

        // setting up simple web sever to test submitting function via URL
        fileServerThread = new Thread(() -> {
            try {
                fileServer = HttpServer.create(new InetSocketAddress(fileServerPort), 0);
                fileServer.createContext("/pulsar-io-data-generator.nar", he -> {
                    try {

                        Headers headers = he.getResponseHeaders();
                        headers.add("Content-Type", "application/octet-stream");

                        File file = new File(getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile());
                        byte[] bytes  = new byte [(int)file.length()];

                        FileInputStream fileInputStream = new FileInputStream(file);
                        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                        bufferedInputStream.read(bytes, 0, bytes.length);

                        he.sendResponseHeaders(200, file.length());
                        OutputStream outputStream = he.getResponseBody();
                        outputStream.write(bytes, 0, bytes.length);
                        outputStream.close();

                    } catch (Exception e) {
                        log.error("Error when downloading: {}", e, e);
                    }
                });
                fileServer.createContext("/pulsar-functions-api-examples.jar", he -> {
                    try {

                        Headers headers = he.getResponseHeaders();
                        headers.add("Content-Type", "application/octet-stream");

                        File file = new File(getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile());
                        byte[] bytes  = new byte [(int)file.length()];

                        FileInputStream fileInputStream = new FileInputStream(file);
                        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                        bufferedInputStream.read(bytes, 0, bytes.length);

                        he.sendResponseHeaders(200, file.length());
                        OutputStream outputStream = he.getResponseBody();
                        outputStream.write(bytes, 0, bytes.length);
                        outputStream.close();

                    } catch (Exception e) {
                        log.error("Error when downloading: {}", e, e);
                    }
                });
                fileServer.setExecutor(null); // creates a default executor
                log.info("Starting file server...");
                fileServer.start();
            } catch (Exception e) {
                log.error("Failed to start file server: ", e);
                fileServer.stop(0);
            }

        });
        fileServerThread.start();
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        fileServer.stop(0);
        fileServerThread.interrupt();
        pulsarClient.close();
        admin.close();
        functionsWorkerService.stop();
        pulsar.close();
        bkEnsemble.stop();
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {

        System.setProperty(JAVA_INSTANCE_JAR_PROPERTY,
                FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName(CLUSTER));
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
        this.workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId(workerId);

        workerConfig.setClientAuthenticationPlugin(AuthenticationTls.class.getName());
        workerConfig.setClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH));
        workerConfig.setUseTls(true);
        workerConfig.setTlsAllowInsecureConnection(true);
        workerConfig.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        return new WorkerService(workerConfig);
    }

    protected static FunctionConfig createFunctionConfig(String tenant, String namespace, String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setInputs(Collections.singleton(sourceTopic));
        functionConfig.setAutoAck(true);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setOutput(sinkTopic);
        functionConfig.setCleanupSubscription(true);
        return functionConfig;
    }

    private static SourceConfig createSourceConfig(String tenant, String namespace, String functionName, String sinkTopic) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(functionName);
        sourceConfig.setParallelism(1);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setTopicName(sinkTopic);
        return sourceConfig;
    }

    private static SinkConfig createSinkConfig(String tenant, String namespace, String functionName, String sourceTopic, String subName) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(functionName);
        sinkConfig.setParallelism(1);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().build()));
        sinkConfig.setSourceSubscriptionName(subName);
        sinkConfig.setCleanupSubscription(true);
        return sinkConfig;
    }
    /**
     * Validates pulsar sink e2e functionality on functions.
     *
     * @throws Exception
     */
    private void testE2EPulsarFunctionLocalRun(String jarFilePathUrl) throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList(CLUSTER));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, sinkTopic, subscriptionName);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        functionConfig.setJar(jarFilePathUrl);
        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls().get()).build();
        localRunner.start(false);

        retryStrategically((test) -> {
            try {
                TopicStats stats = admin.topics().getStats(sourceTopic);
                return stats.subscriptions.get(subscriptionName) != null
                        && !stats.subscriptions.get(subscriptionName).consumers.isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar sink consumer has started on the topic
        TopicStats stats = admin.topics().getStats(sourceTopic);
        assertTrue(stats.subscriptions.get(subscriptionName) != null
                && !stats.subscriptions.get(subscriptionName).consumers.isEmpty());

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats.unackedMessages == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);

        for (int i = 0; i < totalMsgs; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedPropertyValue = msg.getProperty(propertyKey);
            assertEquals(propertyValue, receivedPropertyValue);
            assertEquals(msg.getValue(),  "my-message-" + i + "!");
        }

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).subscriptions.values().iterator().next().unackedMessages,
                totalMsgs);

        // stop functions
        localRunner.stop();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                return topicStats.subscriptions.get(subscriptionName) != null
                        && topicStats.subscriptions.get(subscriptionName).consumers.isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats.subscriptions.get(subscriptionName) != null
                && topicStats.subscriptions.get(subscriptionName).consumers.isEmpty());

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).publishers.size() == 0);
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == 404) {
                    return true;
                }
                return false;
            }
        }, 10, 150);

        try {
            assertTrue(admin.topics().getStats(sinkTopic).publishers.size() == 0);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() != 404) {
                fail();
            }
        }
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionLocalRun() throws Exception {
        testE2EPulsarFunctionLocalRun(null);
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionLocalRunWithJar() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        testE2EPulsarFunctionLocalRun(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testE2EPulsarFunctionLocalRunURL() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-functions-api-examples.jar", fileServerPort);
        testE2EPulsarFunctionLocalRun(jarFilePathUrl);
    }

    private void testPulsarSourceLocalRun(String jarFilePathUrl) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sourceName = "PulsarSource-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList(CLUSTER));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        SourceConfig sourceConfig = createSourceConfig(tenant, namespacePortion, sourceName, sinkTopic);
        if (jarFilePathUrl == null || !jarFilePathUrl.endsWith(".nar")) {
            sourceConfig.setClassName(DataGeneratorSource.class.getName());
        }

        sourceConfig.setArchive(jarFilePathUrl);
        LocalRunner localRunner = LocalRunner.builder()
                .sourceConfig(sourceConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls().get()).build();

        localRunner.start(false);

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).publishers.size() == 1);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 150);

        retryStrategically((test) -> {
            try {
                TopicStats sourceStats = admin.topics().getStats(sinkTopic);
                return sourceStats.publishers.size() == 1
                        && sourceStats.publishers.get(0).metadata != null
                        && sourceStats.publishers.get(0).metadata.containsKey("id")
                        && sourceStats.publishers.get(0).metadata.get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, sourceName));
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats sourceStats = admin.topics().getStats(sinkTopic);
        assertEquals(sourceStats.publishers.size(), 1);
        assertTrue(sourceStats.publishers.get(0).metadata != null);
        assertTrue(sourceStats.publishers.get(0).metadata.containsKey("id"));
        assertEquals(sourceStats.publishers.get(0).metadata.get("id"), String.format("%s/%s/%s", tenant, namespacePortion, sourceName));

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).publishers.size() == 1)
                        && (admin.topics().getInternalStats(sinkTopic).numberOfEntries > 4);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertEquals(admin.topics().getStats(sinkTopic).publishers.size(), 1);

        localRunner.stop();

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).publishers.size() == 0);
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == 404) {
                    return true;
                }
                return false;
            }
        }, 10, 150);

        try {
            assertTrue(admin.topics().getStats(sinkTopic).publishers.size() == 0);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() != 404) {
                fail();
            }
        }
    }


    @Test(timeOut = 20000)
    public void testPulsarSourceLocalRunNoArchive() throws Exception {
        testPulsarSourceLocalRun(null);
    }

    @Test(timeOut = 20000)
    public void testPulsarSourceLocalRunWithFile() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile();
        testPulsarSourceLocalRun(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSourceLocalRunWithUrl() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-io-data-generator.nar", fileServerPort);
        testPulsarSourceLocalRun(jarFilePathUrl);
    }


    private void testPulsarSinkStats(String jarFilePathUrl) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String sinkName = "PulsarSink-test";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("local"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);

        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(1000).build()));
        if (jarFilePathUrl == null || !jarFilePathUrl.endsWith(".nar")) {
            sinkConfig.setClassName(DataGeneratorPrintSink.class.getName());
        }

        sinkConfig.setArchive(jarFilePathUrl);
        LocalRunner localRunner = LocalRunner.builder()
                .sinkConfig(sinkConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls().get()).build();

        localRunner.start(false);

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);

                return topicStats.subscriptions.containsKey(subscriptionName)
                        && topicStats.subscriptions.get(subscriptionName).consumers.size() == 1
                        && topicStats.subscriptions.get(subscriptionName).consumers.get(0).availablePermits == 1000;

            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertEquals(topicStats.subscriptions.size(), 1);
        assertTrue(topicStats.subscriptions.containsKey(subscriptionName));
        assertEquals(topicStats.subscriptions.get(subscriptionName).consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get(subscriptionName).consumers.get(0).availablePermits, 1000);

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats.unackedMessages == 0 && subStats.msgThroughputOut == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        // stop sink
        localRunner.stop();

        retryStrategically((test) -> {
            try {
                TopicStats stats = admin.topics().getStats(sourceTopic);
                return stats.subscriptions.get(subscriptionName) != null
                        && stats.subscriptions.get(subscriptionName).consumers.isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150);

        topicStats = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats.subscriptions.get(subscriptionName) != null
                && topicStats.subscriptions.get(subscriptionName).consumers.isEmpty());

    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsNoArchive() throws Exception {
        testPulsarSinkStats(null);
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsWithFile() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile();
        testPulsarSinkStats(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSinkStatsWithUrl() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-io-data-generator.nar", fileServerPort);
        testPulsarSinkStats(jarFilePathUrl);
    }
}