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
package org.apache.pulsar.functions.worker;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
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
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Pulsar function state
 */
@Slf4j
@Test(groups = "functions-worker")
public class PulsarFunctionPublishTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    PulsarWorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";
    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }

    @BeforeMethod
    void setup(Method method) throws Exception {
        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(ServiceConfiguration.class);
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser", "admin");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
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
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);

        functionsWorkerService = createPulsarFunctionWorker(config);

        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, workerConfig, functionWorkerService, (exitCode) -> {
        });
        pulsar.start();

        String brokerServiceUrl = pulsar.getWebServiceAddressTls();
        urlTls = new URL(brokerServiceUrl);


        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);

        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                        .allowTlsInsecureConnection(true).authentication(authTls).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        ClusterData clusterData = ClusterData.builder().serviceUrlTls(urlTls.toString()).build();
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        if (isNotBlank(workerConfig.getBrokerClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getBrokerClientAuthenticationParameters())) {
            clientBuilder.enableTls(workerConfig.isUseTls());
            clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
            clientBuilder.authentication(workerConfig.getBrokerClientAuthenticationPlugin(),
                    workerConfig.getBrokerClientAuthenticationParameters());
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = TenantInfo.builder()
                .adminRoles(Collections.singleton("superUser"))
                .allowedClusters(Collections.singleton("use"))
                .build();
        admin.tenants().updateTenant(tenant, propAdmin);

        System.setProperty(JAVA_INSTANCE_JAR_PROPERTY,
                FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        while (!functionsWorkerService.getLeaderService().isLeader()) {
            Thread.sleep(1000);
        }
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        try {
            log.info("--- Shutting down ---");
            pulsarClient.close();
            admin.close();
            functionsWorkerService.stop();
            pulsar.close();
            bkEnsemble.stop();
        } finally {
            if (tempDirectory != null) {
                tempDirectory.delete();
            }
        }
    }

    private PulsarWorkerService createPulsarFunctionWorker(ServiceConfiguration config) {

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
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePortTls().get());
        workerConfig.setPulsarWebServiceUrl("https://127.0.0.1:" + config.getWebServicePortTls().get());
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

    protected static FunctionConfig createFunctionConfig(String tenant, String namespace, String functionName, String sourceTopic, String publishTopic, String subscriptionName) {

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setInputs(Collections.singleton(sourceTopic));
        functionConfig.setAutoAck(true);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.TypedMessageBuilderPublish");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        Map<String, Object> userConfig = new HashMap<>();
        userConfig.put("publish-topic", publishTopic);
        functionConfig.setUserConfig(userConfig);
        functionConfig.setCleanupSubscription(true);
        return functionConfig;
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionState() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String publishTopic = "persistent://" + replNamespace + "/publishtopic";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(publishTopic).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, publishTopic, subscriptionName);

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "foo";
            producer.newMessage().property(propertyKey, propertyValue).key(String.valueOf(i)).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        retryStrategically((test) -> {
            try {
                FunctionStatsImpl functionStat = (FunctionStatsImpl)
                        admin.functions().getFunctionStats(tenant, namespacePortion, functionName);
                return functionStat.getProcessedSuccessfullyTotal() == 5;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedPropertyValue = msg.getProperty(propertyKey);
            assertEquals(propertyValue, receivedPropertyValue);
            assertEquals(msg.getProperty("input_topic"), sourceTopic);
            assertEquals(msg.getKey(), String.valueOf(i));
        }

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();
    }

    @Test
    public void testMultipleAddress() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String publishTopic = "persistent://" + replNamespace + "/publishtopic";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, publishTopic, subscriptionName);

        Map<String, String> authParams = new HashMap<>();
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        Authentication authTls = new AuthenticationTls();
        authTls.configure(authParams);
        String secondAddress = pulsar.getWebServiceAddressTls().replace("https://", "");

        //set multi webService url
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddressTls() + "," + secondAddress)
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .allowTlsInsecureConnection(true).authentication(authTls)
                .build();

        File jarFile = getPulsarApiExamplesJar();
        Assert.assertTrue(jarFile.exists() && jarFile.isFile());
        pulsarAdmin.functions().createFunction(functionConfig, jarFile.getAbsolutePath());
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionBKCleanup() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String publishTopic = "persistent://" + replNamespace + "/publishtopic";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(publishTopic).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, publishTopic, subscriptionName);

        File jarFile = getPulsarApiExamplesJar();
        Assert.assertTrue(jarFile.exists() && jarFile.isFile());
        admin.functions().createFunction(functionConfig, jarFile.getAbsolutePath());

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "foo";
            producer.newMessage().property(propertyKey, propertyValue).key(String.valueOf(i)).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        retryStrategically((test) -> {
            try {
                FunctionStatsImpl functionStat = (FunctionStatsImpl)
                        admin.functions().getFunctionStats(tenant, namespacePortion, functionName);
                return functionStat.getProcessedSuccessfullyTotal() == 5;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedPropertyValue = msg.getProperty(propertyKey);
            assertEquals(propertyValue, receivedPropertyValue);
            assertEquals(msg.getProperty("input_topic"), sourceTopic);
            assertEquals(msg.getKey(), String.valueOf(i));
        }

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
          totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();

        DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(workerConfig);

        // check if all function files are deleted from BK
        String url = String.format("distributedlog://%s/pulsar/functions", "127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        log.info("dlog url: {}", url);
        URI dlogUri = URI.create(url);

        Namespace dlogNamespace = NamespaceBuilder.newBuilder()
                .conf(dlogConf)
                .clientId("function-worker-" + workerConfig.getWorkerId())
                .uri(dlogUri)
                .build();

        List<String> files = new LinkedList<>();
        dlogNamespace.getLogs(String.format("%s/%s/%s", tenant, namespacePortion, functionName)).forEachRemaining(new java.util.function.Consumer<String>() {
            @Override
            public void accept(String s) {
                files.add(s);
            }
        });

        assertEquals(files.size(), 0, "BK files left over: " + files);

    }

    @Test
    public void testUpdateFunctionUserConfig() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String publishTopic = "persistent://" + replNamespace + "/publishtopic";
        final String functionName = "test-update-user-config";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, publishTopic, subscriptionName);

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        Map<String, Object> userConfig = functionConfig.getUserConfig();
        functionConfig.setUserConfig(null);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
        FunctionConfig updatefunctionConfig1 = admin.functions().getFunction(tenant, namespacePortion, functionName);
        Assert.assertEquals(userConfig, updatefunctionConfig1.getUserConfig());

        Map<String, Object> newUserConfig = new HashMap<>();
        newUserConfig.put("publish-topic", publishTopic);
        newUserConfig.put("test", "test");
        updatefunctionConfig1.setUserConfig(newUserConfig);
        admin.functions().updateFunctionWithUrl(updatefunctionConfig1, jarFilePathUrl);
        FunctionConfig updatefunctionConfig2 = admin.functions().getFunction(tenant, namespacePortion, functionName);
        Assert.assertEquals(updatefunctionConfig2.getUserConfig(), updatefunctionConfig1.getUserConfig());
    }
}
