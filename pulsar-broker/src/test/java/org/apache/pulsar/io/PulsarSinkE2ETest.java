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
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
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
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData.DataDigest;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarSinkE2ETest {
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
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String primaryHost;
    String workerId;

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

    private static final Logger log = LoggerFactory.getLogger(PulsarSinkE2ETest.class);

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

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
        config.setWebServicePort(brokerWebServicePort);
        config.setWebServicePortTls(brokerWebServiceTlsPort);
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(brokerServicePort);
        config.setBrokerServicePortTls(brokerServiceTlsPort);
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setTlsEnabled(true);
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
        primaryHost = String.format("http://%s:%d", InetAddress.getLocalHost().getHostName(), brokerWebServicePort);

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
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePortTls());
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
        workerConfig.setTlsTrustCertsFilePath(TLS_CLIENT_CERT_FILE_PATH);

        workerConfig.setAuthenticationEnabled(true);
        workerConfig.setAuthorizationEnabled(true);

        return new WorkerService(workerConfig);
    }

    /**
     * Validates pulsar sink e2e functionality on functions.
     *
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testE2EPulsarSink() throws Exception {

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
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopic).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(sinkTopic).subscriptionName("sub").subscribe();

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails functionDetails = createSinkConfig(jarFilePathUrl, tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionDetails, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data.getBytes()).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats.unackedMessages == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);

        Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
        String receivedPropertyValue = msg.getProperty(propertyKey);
        assertEquals(propertyValue, receivedPropertyValue);

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).subscriptions.values().iterator().next().unackedMessages,
                totalMsgs);

    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStats() throws Exception {

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
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails functionDetails = createSinkConfig(jarFilePathUrl, tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionDetails, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data.getBytes()).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats.unackedMessages == 0 && subStats.msgThroughputOut == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        FunctionRuntimeManager functionRuntimeManager = functionsWorkerService.getFunctionRuntimeManager();
        functionRuntimeManager.updateRates();
        FunctionStatusList functionStats = functionRuntimeManager.getAllFunctionStatus(tenant, namespacePortion,
                functionName, null);

        int numInstances = functionStats.getFunctionStatusListCount();
        assertEquals(numInstances, 1);

        FunctionStatus stats = functionStats.getFunctionStatusListList().get(0);
        Map<String, DataDigest> metricsData = stats.getMetrics().getMetricsMap();

        double count = metricsData.get(JavaInstanceRunnable.METRICS_TOTAL_PROCESSED).getCount();
        double success = metricsData.get(JavaInstanceRunnable.METRICS_TOTAL_SUCCESS).getCount();
        String ownerWorkerId = stats.getWorkerId();
        assertEquals((int) count, totalMsgs);
        assertEquals((int) success, totalMsgs);
        assertEquals(ownerWorkerId, workerId);
    }

    protected static FunctionDetails createSinkConfig(String jarFile, String tenant, String namespace, String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {

        File file = new File(jarFile);
        try {
            Reflections.loadJar(file);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to load user jar " + file, e);
        }
        String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);
        Class<?> typeArg = byte[].class;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setTenant(tenant);
        functionDetailsBuilder.setNamespace(namespace);
        functionDetailsBuilder.setName(functionName);
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(1);
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        functionDetailsBuilder.setProcessingGuarantees(ProcessingGuarantees.EFFECTIVELY_ONCE);

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.FAILOVER);
        sourceSpecBuilder.setTypeClassName(typeArg.getName());
        sourceSpecBuilder.setTopicsPattern(sourceTopicPattern);
        sourceSpecBuilder.setSubscriptionName(subscriptionName);
        sourceSpecBuilder.putTopicsToSerDeClassName(sourceTopicPattern, "");
        functionDetailsBuilder.setAutoAck(true);
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        // sinkSpecBuilder.setClassName(PulsarSink.class.getName());
        sinkSpecBuilder.setTopic(sinkTopic);
        Map<String, Object> sinkConfigMap = Maps.newHashMap();
        sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfigMap));
        sinkSpecBuilder.setTypeClassName(typeArg.getName());
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        return functionDetailsBuilder.build();
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

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails functionDetails = createSinkConfig(jarFilePathUrl, tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        try {
            admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);
            assertTrue(validRoleName);
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException ne) {
            assertFalse(validRoleName);
        }
    }

    /**
     * Test to verify: function-server loads jar using file-url and derives type-args classes if not provided
     * @throws Exception
     */
    @Test(timeOut = 20000)
    public void testFileUrlFunctionWithoutPassingTypeArgs() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        String jarFilePathUrl = Utils.FILE + ":"
                + IdentityFunction.class.getProtectionDomain().getCodeSource().getLocation().getPath();

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setTenant(tenant);
        functionDetailsBuilder.setNamespace(namespacePortion);
        functionDetailsBuilder.setName(functionName);
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(1);
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());

        Class<?>[] typeArgs = org.apache.pulsar.functions.utils.Utils.getFunctionTypes(new IdentityFunction(), false);

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.FAILOVER);
        sourceSpecBuilder.putTopicsToSerDeClassName(sinkTopic, TopicSchema.DEFAULT_SERDE);
        functionDetailsBuilder.setAutoAck(true);
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        sinkSpecBuilder.setTopic(sinkTopic);
        Map<String, Object> sinkConfigMap = Maps.newHashMap();
        sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfigMap));
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        FunctionDetails functionDetails = functionDetailsBuilder.build();
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);

        FunctionDetails functionMetadata = admin.functions().getFunction(tenant, namespacePortion, functionName);

        assertEquals(functionMetadata.getSource().getTypeClassName(), typeArgs[0].getName());
        assertEquals(functionMetadata.getSink().getTypeClassName(), typeArgs[1].getName());

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
        Producer<byte[]> producer = pulsarClient.newProducer().topic(sourceTopic).create();

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails functionDetails = createSinkConfig(jarFilePathUrl, tenant, namespacePortion, functionName,
                sourceTopicName, sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
                return subStats != null && subStats.consumers.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);

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
        }, 5, 150);

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
        }, 5, 150);

        subStats = admin.topics().getStats(sourceTopic).subscriptions.get(subscriptionName);
        assertEquals(subStats.consumers.size(), 1);

        producer.close();
    }
}