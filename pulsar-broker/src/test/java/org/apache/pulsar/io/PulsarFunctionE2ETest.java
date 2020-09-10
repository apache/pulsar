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
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.ToString;

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
import org.apache.pulsar.client.api.MessageRoutingMode;
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
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.TwoPhaseCompactor;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarFunctionE2ETest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";
    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionE2ETest.class);
    private Thread fileServerThread;
    private HttpServer fileServer;

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @BeforeMethod
    void setup(Method method) throws Exception {

        // delete all function temp files
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles((dir1, name) -> name.startsWith("function"));

        for (File file : foundFiles) {
            file.delete();
        }

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
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

        functionsWorkerService = createPulsarFunctionWorker(config);
        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService, (exitCode) -> {});
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

        // setting up simple web sever to test submitting function via URL
        CountDownLatch latch = new CountDownLatch(1);
        fileServerThread = new Thread(() -> {
            try {
                fileServer = HttpServer.create(new InetSocketAddress(0), 0);
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
            latch.countDown();
        });
        fileServerThread.start();
        latch.await(1, TimeUnit.SECONDS);

        while (!functionsWorkerService.getLeaderService().isLeader()) {
            Thread.sleep(1000);
        }
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

        return new WorkerService(workerConfig);
    }

    protected static FunctionConfig createFunctionConfig(String tenant, String namespace, String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {
        String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setTopicsPattern(sourceTopicPattern);
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
    private void testE2EPulsarFunction(String jarFilePathUrl) throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sinkTopic2 = "persistent://" + replNamespace + "/output2";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic2).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        functionConfig.setParallelism(2);
        functionConfig.setOutput(sinkTopic2);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sinkTopic2);
                return topicStats.publishers.size() == 2
                        && topicStats.publishers.get(0).metadata != null
                        && topicStats.publishers.get(0).metadata.containsKey("id")
                        && topicStats.publishers.get(0).metadata.get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, functionName));
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats topicStats = admin.topics().getStats(sinkTopic2);
        assertEquals(topicStats.publishers.size(), 2);
        assertNotNull(topicStats.publishers.get(0).metadata);
        assertTrue(topicStats.publishers.get(0).metadata.containsKey("id"));
        assertEquals(topicStats.publishers.get(0).metadata.get("id"), String.format("%s/%s/%s", tenant, namespacePortion, functionName));

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

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
        }, 50, 150);

        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        String receivedPropertyValue = msg.getProperty(propertyKey);
        assertEquals(propertyValue, receivedPropertyValue);

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).subscriptions.values().iterator().next().unackedMessages,
                totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 0);

        // make sure all temp files are deleted
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles((dir1, name) -> name.startsWith("function"));

        Assert.assertEquals(foundFiles.length, 0, "Temporary files left over: " + Arrays.asList(foundFiles));
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionWithFile() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        testE2EPulsarFunction(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testE2EPulsarFunctionWithUrl() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-functions-api-examples.jar",
                fileServer.getAddress().getPort());
        testE2EPulsarFunction(jarFilePathUrl);
    }

    @Test(timeOut = 30000)
    public void testReadCompactedFunction() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final int messageNum = 20;
        final int maxKeys = 10;
        // 1 Setup producer
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(sourceTopic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        pulsarClient.newConsumer().topic(sourceTopic).subscriptionName(subscriptionName).readCompacted(true).subscribe().close();
        // 2 Send messages and record the expected values after compaction
        Map<String, String> expected = new HashMap<>();
        for (int j = 0; j < messageNum; j++) {
            String key = "key" + j % maxKeys;
            String value = "my-message-" + key + j;
            producer.newMessage().key(key).value(value).send();
            //Duplicate keys will exist, the value of the new key will be retained
            expected.put(key, value);
        }
        // 3 Trigger compaction
        ScheduledExecutorService compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
        TwoPhaseCompactor twoPhaseCompactor = new TwoPhaseCompactor(config,
                pulsarClient, pulsar.getBookKeeperClient(), compactionScheduler);
        twoPhaseCompactor.compact(sourceTopic).get();

        // 4 Setup function
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, sinkTopic, subscriptionName);
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        ConsumerConfig consumerConfig = new ConsumerConfig();
        Map<String,String> consumerProperties = new HashMap<>();
        consumerProperties.put("readCompacted","true");
        consumerConfig.setConsumerProperties(consumerProperties);
        inputSpecs.put(sourceTopic, consumerConfig);
        functionConfig.setInputSpecs(inputSpecs);
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // 5 Function should only read compacted value，so we will only receive compacted messages
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sink-sub").subscribe();
        int count = 0;
        while (true) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
            count++;
            Assert.assertEquals(expected.remove(message.getKey()) + "!", message.getValue());
        }
        Assert.assertEquals(count, maxKeys);
        Assert.assertTrue(expected.isEmpty());

        compactionScheduler.shutdownNow();
        consumer.close();
        producer.close();
    }

    @Test(timeOut = 30000)
    public void testReadCompactedSink() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic2";
        final String sinkName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final int messageNum = 20;
        final int maxKeys = 10;
        // 1 Setup producer
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(sourceTopic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        pulsarClient.newConsumer().topic(sourceTopic).subscriptionName(subscriptionName).readCompacted(true).subscribe().close();
        // 2 Send messages and record the expected values after compaction
        Map<String, String> expected = new HashMap<>();
        for (int j = 0; j < messageNum; j++) {
            String key = "key" + j % maxKeys;
            String value = "my-message-" + key + j;
            producer.newMessage().key(key).value(value).send();
            //Duplicate keys will exist, the value of the new key will be retained
            expected.put(key, value);
        }
        // 3 Trigger compaction
        ScheduledExecutorService compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
        TwoPhaseCompactor twoPhaseCompactor = new TwoPhaseCompactor(config,
                pulsarClient, pulsar.getBookKeeperClient(), compactionScheduler);
        twoPhaseCompactor.compact(sourceTopic).get();

        // 4 Setup sink
        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        Map<String,String> consumerProperties = new HashMap<>();
        consumerProperties.put("readCompacted","true");
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().consumerProperties(consumerProperties).build()));
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile();
        admin.sink().createSinkWithUrl(sinkConfig, jarFilePathUrl);

        // 5 Sink should only read compacted value，so we will only receive compacted messages
        retryStrategically((test) -> {
            try {
                String prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
                Map<String, Metric> metrics = parseMetrics(prometheusMetrics);
                Metric m = metrics.get("pulsar_sink_received_total");
                return m.value == (double) maxKeys;
            } catch (Exception e) {
                return false;
            }
        }, 50, 1000);

        compactionScheduler.shutdownNow();
        producer.close();
    }

    @Test(timeOut = 30000)
    private void testPulsarSinkDLQ() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String dlqTopic = sourceTopic+"-DLQ";
        final String sinkName = "PulsarSink-test";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        // 1 create producer、DLQ consumer
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(dlqTopic).subscriptionName(subscriptionName).subscribe();

        // 2 setup sink
        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);
        sinkConfig.setNegativeAckRedeliveryDelayMs(1001L);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setMaxMessageRetries(2);
        sinkConfig.setDeadLetterTopic(dlqTopic);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(1000).build()));
        sinkConfig.setClassName(SinkForTest.class.getName());
        LocalRunner localRunner = LocalRunner.builder()
                .sinkConfig(sinkConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls()).build();

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
        }, 50, 150);

        // 3 send message
        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            producer.newMessage().property(propertyKey, propertyValue).value("fail" + i).sendAsync();
        }

        //4 All messages should enter DLQ
        for (int i = 0; i < totalMsgs; i++) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(message.getValue(), "fail" + i);
        }

        //clean up
        producer.close();
        consumer.close();
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
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);

        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(1000).build()));

        admin.sink().createSinkWithUrl(sinkConfig, jarFilePathUrl);

        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(523).build()));

        admin.sink().updateSinkWithUrl(sinkConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);

                return topicStats.subscriptions.containsKey(subscriptionName)
                        && topicStats.subscriptions.get(subscriptionName).consumers.size() == 1
                        && topicStats.subscriptions.get(subscriptionName).consumers.get(0).availablePermits == 523;

            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertEquals(topicStats.subscriptions.size(), 1);
        assertTrue(topicStats.subscriptions.containsKey(subscriptionName));
        assertEquals(topicStats.subscriptions.get(subscriptionName).consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get(subscriptionName).consumers.get(0).availablePermits, 523);

        // validate prometheus metrics empty
        String prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, Metric> metrics = parseMetrics(prometheusMetrics);
        Metric m = metrics.get("pulsar_sink_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_written_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);

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

        // get stats after producing
        prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheusMetrics: {}", prometheusMetrics);

        metrics = parseMetrics(prometheusMetrics);
        m = metrics.get("pulsar_sink_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_written_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_sink_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertTrue(m.value > 0.0);


        // delete functions
        admin.sink().deleteSink(tenant, namespacePortion, sinkName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 0);

        // make sure all temp files are deleted
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles((dir1, name) -> name.startsWith("function"));

        Assert.assertEquals(foundFiles.length, 0, "Temporary files left over: " + Arrays.asList(foundFiles));
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsWithFile() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile();
        testPulsarSinkStats(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSinkStatsWithUrl() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-io-data-generator.nar",
                fileServer.getAddress().getPort());
        testPulsarSinkStats(jarFilePathUrl);
    }

    private void testPulsarSourceStats(String jarFilePathUrl) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sourceName = "PulsarSource-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        SourceConfig sourceConfig = createSourceConfig(tenant, namespacePortion, sourceName, sinkTopic);

        admin.source().createSourceWithUrl(sourceConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).publishers.size() == 1);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 150);

        final String sinkTopic2 = "persistent://" + replNamespace + "/output2";
        sourceConfig.setTopicName(sinkTopic2);
        admin.source().updateSourceWithUrl(sourceConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                TopicStats sourceStats = admin.topics().getStats(sinkTopic2);
                return sourceStats.publishers.size() == 1
                        && sourceStats.publishers.get(0).metadata != null
                        && sourceStats.publishers.get(0).metadata.containsKey("id")
                        && sourceStats.publishers.get(0).metadata.get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, sourceName));
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats sourceStats = admin.topics().getStats(sinkTopic2);
        assertEquals(sourceStats.publishers.size(), 1);
        assertNotNull(sourceStats.publishers.get(0).metadata);
        assertTrue(sourceStats.publishers.get(0).metadata.containsKey("id"));
        assertEquals(sourceStats.publishers.get(0).metadata.get("id"), String.format("%s/%s/%s", tenant, namespacePortion, sourceName));

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic2).publishers.size() == 1) && (admin.topics().getInternalStats(sinkTopic2).numberOfEntries > 4);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertEquals(admin.topics().getStats(sinkTopic2).publishers.size(), 1);

        String prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheusMetrics: {}", prometheusMetrics);

        Map<String, Metric> metrics = parseMetrics(prometheusMetrics);
        Metric m = metrics.get("pulsar_source_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_written_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_source_source_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_source_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_source_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sourceName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
        assertTrue(m.value > 0.0);

        // make sure all temp files are deleted
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles((dir1, name) -> name.startsWith("function"));

        Assert.assertEquals(foundFiles.length, 0, "Temporary files left over: " + Arrays.asList(foundFiles));
    }

    @Test(timeOut = 20000)
    public void testPulsarSourceStatsWithFile() throws Exception {
        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-io-data-generator.nar").getFile();
        testPulsarSourceStats(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSourceStatsWithUrl() throws Exception {
        String jarFilePathUrl = String.format("http://127.0.0.1:%d/pulsar-io-data-generator.nar",
                fileServer.getAddress().getPort());
        testPulsarSourceStats(jarFilePathUrl);
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionStats() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        // validate stats are empty
        FunctionRuntimeManager functionRuntimeManager = functionsWorkerService.getFunctionRuntimeManager();
        FunctionStats functionStats = functionRuntimeManager.getFunctionStats(tenant, namespacePortion,
                functionName, null);
        FunctionStats functionStatsFromAdmin = admin.functions().getFunctionStats(tenant, namespacePortion,
                functionName);

        assertEquals(functionStats, functionStatsFromAdmin);

        assertEquals(functionStats.getReceivedTotal(), 0);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.avgProcessLatency, null);
        assertEquals(functionStats.oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getAvgProcessLatency(), null);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertEquals(functionStats.getLastInvocation(), null);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().avgProcessLatency, null);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency(), null);

        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(), functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency());
        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(), functionStats.getAvgProcessLatency());

        // validate prometheus metrics empty
        String prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, Metric> metrics = parseMetrics(prometheusMetrics);
        Metric m = metrics.get("pulsar_function_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_process_latency_ms");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, Double.NaN);
        m = metrics.get("pulsar_function_process_latency_ms_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, Double.NaN);
        m = metrics.get("pulsar_function_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);


        // validate function instance stats empty
        FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStats = functionRuntimeManager.getFunctionInstanceStats(tenant, namespacePortion,
                functionName, 0,  null);

        FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData functionInstanceStatsAdmin = admin.functions().getFunctionStats(tenant, namespacePortion,
                functionName, 0);

        assertEquals(functionInstanceStats, functionInstanceStatsAdmin);
        assertEquals(functionInstanceStats, functionStats.instances.get(0).getMetrics());


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


        // get stats after producing
        functionStats = functionRuntimeManager.getFunctionStats(tenant, namespacePortion,
                functionName, null);

        functionStatsFromAdmin = admin.functions().getFunctionStats(tenant, namespacePortion,
                functionName);

        assertEquals(functionStats, functionStatsFromAdmin);

        assertEquals(functionStats.getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertEquals(functionStats.oneMin.getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.oneMin.getAvgProcessLatency() > 0);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().avgProcessLatency > 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency() > 0);

        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(), functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency());
        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(), functionStats.getAvgProcessLatency());

        // validate function instance stats
        functionInstanceStats = functionRuntimeManager.getFunctionInstanceStats(tenant, namespacePortion,
                functionName, 0,  null);

        functionInstanceStatsAdmin = admin.functions().getFunctionStats(tenant, namespacePortion,
                functionName, 0);

        assertEquals(functionInstanceStats, functionInstanceStatsAdmin);
        assertEquals(functionInstanceStats, functionStats.instances.get(0).getMetrics());

        // validate prometheus metrics
        prometheusMetrics = getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        metrics = parseMetrics(prometheusMetrics);
        m = metrics.get("pulsar_function_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_user_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_process_latency_ms");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_process_latency_ms_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_processed_successfully_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 0);

        // make sure all temp files are deleted
        File dir = new File(System.getProperty("java.io.tmpdir"));
        File[] foundFiles = dir.listFiles((dir1, name) -> name.startsWith("function"));

        Assert.assertEquals(foundFiles.length, 0, "Temporary files left over: " + Arrays.asList(foundFiles));
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionStatus() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

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

        FunctionStatus functionStatus = admin.functions().getFunctionStatus(tenant, namespacePortion,
                functionName);

        int numInstances = functionStatus.getNumInstances();
        assertEquals(numInstances, 1);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData status
                = functionStatus.getInstances().get(0).getStatus();

        double count = status.getNumReceived();
        double success = status.getNumSuccessfullyProcessed();
        String ownerWorkerId = status.getWorkerId();
        assertEquals((int)count, totalMsgs);
        assertEquals((int) success, totalMsgs);
        assertEquals(ownerWorkerId, workerId);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 0);
    }

    @Test(dataProvider = "validRoleName")
    public void testAuthorization(boolean validRoleName) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        String roleName = validRoleName ? "superUser" : "invalid";
        TenantInfo propAdmin = new TenantInfo();
        propAdmin.getAdminRoles().add(roleName);
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        try {
            admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
            assertTrue(validRoleName);
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException ne) {
            assertFalse(validRoleName);
        }
    }

    @Test(timeOut = 20000)
    public void testFunctionStopAndRestartApi() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopicName = "restartFunction";
        final String sourceTopic = "persistent://" + replNamespace + "/" + sourceTopicName;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create source topic
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopicName, sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats != null && subStats.consumers.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
        assertEquals(subStats.consumers.size(), 1);

        // it should stop consumer : so, check none of the consumer connected on subscription
        admin.functions().stopFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStat = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStat != null && subStat.consumers.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
        assertEquals(subStats.consumers.size(), 0);

        // it should restart consumer : so, check if consumer came up again after restarting function
        admin.functions().restartFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStat = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStat != null && subStat.consumers.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
        assertEquals(subStats.consumers.size(), 1);

        producer.close();
    }

    @Test(timeOut = 20000)
    public void testFunctionAutomaticSubCleanup() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":" + getClass().getClassLoader().getResource("pulsar-functions-api-examples.jar").getFile();
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespacePortion);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setInputs(Collections.singleton(sourceTopic));
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setOutput(sinkTopic);
        functionConfig.setCleanupSubscription(false);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                FunctionConfig configure = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return configure != null && configure.getCleanupSubscription() != null;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar source consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        // test update cleanup subscription
        functionConfig.setCleanupSubscription(true);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertTrue(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(
                        InstanceUtils.getDefaultSubscriptionName(tenant, namespacePortion, functionName));
                return subStats.unackedMessages == 0 && subStats.msgThroughputOut == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        FunctionStatus functionStatus = admin.functions().getFunctionStatus(tenant, namespacePortion,
                functionName);

        int numInstances = functionStatus.getNumInstances();
        assertEquals(numInstances, 1);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData status
                = functionStatus.getInstances().get(0).getStatus();

        double count = status.getNumReceived();
        double success = status.getNumSuccessfullyProcessed();
        String ownerWorkerId = status.getWorkerId();
        assertEquals((int)count, totalMsgs);
        assertEquals((int) success, totalMsgs);
        assertEquals(ownerWorkerId, workerId);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 0);


        /** test do not cleanup subscription **/
        functionConfig.setCleanupSubscription(false);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar source consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        retryStrategically((test) -> {
            try {
                FunctionConfig result = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return result.getCleanupSubscription() == false;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        // test update another config and making sure that subscription cleanup remains unchanged
        functionConfig.setParallelism(2);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                FunctionConfig result = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return result.getParallelism() == 2 && result.getCleanupSubscription() == false;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);
    }


    public static String getPrometheusMetrics(int metricsPort) throws IOException {
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
    private static Map<String, Metric> parseMetrics(String metrics) {
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
            m.value = Double.valueOf(matcher.group(3));
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