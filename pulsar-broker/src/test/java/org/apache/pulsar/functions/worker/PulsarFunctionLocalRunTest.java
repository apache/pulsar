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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 */
@Test(groups = { "flaky" })
public class PulsarFunctionLocalRunTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL urlTls;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private static final String CLUSTER = "local";

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";
    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";

    private static final String SYSTEM_PROPERTY_NAME_NAR_FILE_PATH = "pulsar-io-data-generator.nar.path";
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    public static File getPulsarIODataGeneratorNar() {
        return new File(Objects.requireNonNull(System.getProperty(SYSTEM_PROPERTY_NAME_NAR_FILE_PATH)
                , "pulsar-io-data-generator.nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_NAR_FILE_PATH + " system property"));
    }

    private static final String SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_JAR_FILE_PATH =
            "pulsar-functions-api-examples.jar.path";

    public static File getPulsarApiExamplesJar() {
        return new File(Objects.requireNonNull(
                System.getProperty(SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_JAR_FILE_PATH)
                , "pulsar-functions-api-examples.jar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_FUNCTIONS_API_EXAMPLES_JAR_FILE_PATH + " system property"));
    }

    private static final String SYSTEM_PROPERTY_NAME_BATCH_NAR_FILE_PATH = "pulsar-io-batch-data-generator.nar.path";

    public static File getPulsarIOBatchDataGeneratorNar() {
        return new File(Objects.requireNonNull(System.getProperty(SYSTEM_PROPERTY_NAME_BATCH_NAR_FILE_PATH)
                , "pulsar-io-batch-data-generator.nar file location must be specified with "
                        + SYSTEM_PROPERTY_NAME_BATCH_NAR_FILE_PATH + " system property"));
    }


    private URLClassLoader pulsarApiExamplesClassLoader;
    private Class<?> avroTestObjectClass;


    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionLocalRunTest.class);
    private FileServer fileServer;

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @BeforeClass(alwaysRun = true)
    void loadPulsarApiExamples() throws MalformedURLException, ClassNotFoundException {
        pulsarApiExamplesClassLoader = new URLClassLoader(new URL[]{getPulsarApiExamplesJar().toURI().toURL()},
                Thread.currentThread().getContextClassLoader());
        avroTestObjectClass = pulsarApiExamplesClassLoader.loadClass("org.apache.pulsar.functions.api.examples.pojo.AvroTestObject");
    }

    @AfterClass(alwaysRun = true)
    void closeClassLoader() throws IOException {
        if (pulsarApiExamplesClassLoader != null) {
            pulsarApiExamplesClassLoader.close();
            pulsarApiExamplesClassLoader = null;
        }
    }

    @BeforeMethod(alwaysRun = true)
    void setup(Method method) throws Exception {
        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(ServiceConfiguration.class);
        config.setClusterName(CLUSTER);
        Set<String> superUsers = Sets.newHashSet("superUser", "admin");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
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

        workerConfig = createWorkerConfig(config);

        // populate builtin connectors folder
        if (Arrays.asList(method.getAnnotation(Test.class).groups()).contains("builtin")) {
            File connectorsDir = new File(workerConfig.getConnectorsDirectory());

            File file = getPulsarIODataGeneratorNar();
            Files.copy(file.toPath(), new File(connectorsDir, file.getName()).toPath());
        }

        Optional<WorkerService> functionWorkerService = Optional.empty();
        pulsar = new PulsarService(config, workerConfig, functionWorkerService, (exitCode) -> {});
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

        // create cluster metadata
        ClusterData clusterData = ClusterData.builder().serviceUrl(urlTls.toString()).build();
        admin.clusters().createCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl());
        if (isNotBlank(workerConfig.getBrokerClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getBrokerClientAuthenticationParameters())) {
            clientBuilder.enableTls(workerConfig.isUseTls());
            clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
            clientBuilder.authentication(workerConfig.getBrokerClientAuthenticationPlugin(),
                    workerConfig.getBrokerClientAuthenticationParameters());
            clientBuilder.serviceUrl(pulsar.getBrokerServiceUrlTls());
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = TenantInfo.builder()
                .adminRoles(Collections.singleton("superUser"))
                .allowedClusters(Sets.newHashSet(Lists.newArrayList(CLUSTER)))
                .build();
        admin.tenants().createTenant(tenant, propAdmin);

        // setting up simple web sever to test submitting function via URL
        fileServer = new FileServer();
        fileServer.serveFile("/pulsar-io-data-generator.nar", getPulsarIODataGeneratorNar());
        fileServer.serveFile("/pulsar-functions-api-examples.jar", getPulsarApiExamplesJar());
        fileServer.start();
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        try {
            log.info("--- Shutting down ---");
            fileServer.stop();
            pulsarClient.close();
            admin.close();
            pulsar.close();
            bkEnsemble.stop();
        } finally {
            if (tempDirectory != null) {
                tempDirectory.delete();
            }
        }
    }

    protected WorkerConfig createWorkerConfig(ServiceConfiguration config) {

        System.setProperty(JAVA_INSTANCE_JAR_PROPERTY,
                FutureUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        WorkerConfig workerConfig = new WorkerConfig();
        tempDirectory = PulsarFunctionTestTemporaryDirectory.create(getClass().getSimpleName());
        tempDirectory.useTemporaryDirectoriesForWorkerConfig(workerConfig);
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName(CLUSTER), Map.class));
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
        return workerConfig;
    }

    protected static FunctionConfig createFunctionConfig(String tenant,
                                                         String namespace,
                                                         String functionName,
                                                         String sourceTopic,
                                                         String sinkTopic,
                                                         String subscriptionName) {

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

    private static SourceConfig createSourceConfig(String tenant,
                                                   String namespace,
                                                   String functionName,
                                                   String sinkTopic) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(functionName);
        sourceConfig.setParallelism(1);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setTopicName(sinkTopic);
        return sourceConfig;
    }

    private static SinkConfig createSinkConfig(String tenant,
                                               String namespace,
                                               String functionName,
                                               String sourceTopic,
                                               String subName) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(functionName);
        sinkConfig.setParallelism(1);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().build()));
        sinkConfig.setSourceSubscriptionName(subName);
        sinkConfig.setCleanupSubscription(true);
        sinkConfig.setConfigs(new HashMap<>());
        return sinkConfig;
    }
    /**
     * Validates pulsar sink e2e functionality on functions.
     *
     * @throws Exception
     */
    private void testE2EPulsarFunctionLocalRun(String jarFilePathUrl, int parallelism) throws Exception {

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
        functionConfig.setParallelism(parallelism);
        functionConfig.setRetainOrdering(true);
        int metricsPort = FunctionCommon.findAvailablePort();
        @Cleanup
        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .metricsPortStart(metricsPort)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls()).build();
        localRunner.start(false);

        Assert.assertTrue(retryStrategically((test) -> {
            try {

                boolean result = false;
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                if (topicStats.getSubscriptions().containsKey(subscriptionName)
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().size() == parallelism) {
                    for (ConsumerStats consumerStats : topicStats.getSubscriptions().get(subscriptionName).getConsumers()) {
                        result = consumerStats.getAvailablePermits() == 1000
                                && consumerStats.getMetadata() != null
                                && consumerStats.getMetadata().containsKey("id")
                                && consumerStats.getMetadata().get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, functionName));
                    }
                }
                return result;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));
        // validate pulsar sink consumer has started on the topic
        TopicStats stats = admin.topics().getStats(sourceTopic);
        assertTrue(stats.getSubscriptions().get(subscriptionName) != null
                && !stats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty());

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        for (int i = 0; i < totalMsgs; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedPropertyValue = msg.getProperty(propertyKey);
            assertEquals(propertyValue, receivedPropertyValue);
            assertEquals(msg.getValue(),  "my-message-" + i + "!");
        }

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                totalMsgs);

        // validate prometheus metrics
        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(metricsPort);
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metricsMap = new HashMap<>();
        Arrays.asList(prometheusMetrics.split("\n")).forEach(line -> {
            if (line.startsWith("pulsar_function_processed_successfully_total")) {
                Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(line);
                assertFalse(metrics.isEmpty());
                PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_function_processed_successfully_total");
                if (m != null) {
                    metricsMap.put(m.tags.get("instance_id"), m);
                }
            }
        });
        Assert.assertEquals(metricsMap.size(), parallelism);

        double totalMsgRecv = 0.0;
        for (int i = 0; i < parallelism; i++) {
            PulsarFunctionTestUtils.Metric m = metricsMap.get(String.valueOf(i));
            Assert.assertNotNull(m);
            assertEquals(m.tags.get("cluster"), config.getClusterName());
            assertEquals(m.tags.get("instance_id"), String.valueOf(i));
            assertEquals(m.tags.get("name"), functionName);
            assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
            assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
            totalMsgRecv += m.value;
        }
        Assert.assertEquals(totalMsgRecv, totalMsgs);

        // stop functions
        localRunner.stop();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                return topicStats.getSubscriptions().get(subscriptionName) != null
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats.getSubscriptions().get(subscriptionName) != null
                && topicStats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty());

        retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).getPublishers().size() == 0);
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == 404) {
                    return true;
                }
                return false;
            }
        }, 10, 150);

        try {
            assertEquals(admin.topics().getStats(sinkTopic).getPublishers().size(), 0);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() != 404) {
                fail();
            }
        }
    }

    protected void testE2EPulsarFunctionLocalRun(String jarFilePathUrl) throws Exception {
        testE2EPulsarFunctionLocalRun(jarFilePathUrl, 1);
    }

    private void testAvroFunctionLocalRun(String jarFilePathUrl) throws Exception {

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


        Schema schema = Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(true)
                .withJSR310ConversionEnabled(true)
                .withPojo(avroTestObjectClass).build());
        //use AVRO schema
        admin.schemas().createSchema(sourceTopic, schema.getSchemaInfo());
        // please note that in this test the sink topic schema is different from the schema of the source topic

        //produce message to sourceTopic
        Producer<Object> producer = pulsarClient.newProducer(schema).topic(sourceTopic).create();
        //consume message from sinkTopic
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME()).topic(sinkTopic).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopic, sinkTopic, subscriptionName);
        //set jsr310ConversionEnabled、alwaysAllowNull
        Map<String,String> schemaInput = new HashMap<>();
        schemaInput.put(sourceTopic, "{\"schemaType\":\"AVRO\",\"schemaProperties\":{\"__jsr310ConversionEnabled\":\"true\",\"__alwaysAllowNull\":\"true\"}}");
        Map<String, String> schemaOutput = new HashMap<>();
        schemaOutput.put(sinkTopic, "{\"schemaType\":\"AVRO\",\"schemaProperties\":{\"__jsr310ConversionEnabled\":\"true\",\"__alwaysAllowNull\":\"true\"}}");

        functionConfig.setCustomSchemaInputs(schemaInput);
        functionConfig.setCustomSchemaOutputs(schemaOutput);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        if (jarFilePathUrl == null) {
            functionConfig.setClassName("org.apache.pulsar.functions.api.examples.AvroSchemaTestFunction");
        } else {
            functionConfig.setJar(jarFilePathUrl);
        }

        @Cleanup
        LocalRunner localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
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
                TopicStats stats = admin.topics().getStats(sourceTopic);
                return stats.getSubscriptions().get(subscriptionName) != null
                        && !stats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        int totalMsgs = 5;
        Method setBaseValueMethod = avroTestObjectClass.getMethod("setBaseValue", new Class[]{int.class});
        for (int i = 0; i < totalMsgs; i++) {
            Object avroTestObject = avroTestObjectClass.getDeclaredConstructor().newInstance();
            setBaseValueMethod.invoke(avroTestObject, i);
            producer.newMessage().property(propertyKey, propertyValue)
                    .value(avroTestObject).send();
        }

        //consume message from sinkTopic
        for (int i = 0; i < totalMsgs; i++) {
            Message<GenericRecord> msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedPropertyValue = msg.getProperty(propertyKey);
            assertEquals(propertyValue, receivedPropertyValue);
            assertEquals(msg.getValue().getField("baseValue"),  10 + i);
            consumer.acknowledge(msg);
        }

        // validate pulsar-sink consumer has consumed all messages
        assertNotEquals(admin.topics().getStats(sinkTopic).getSubscriptions().values().iterator().next().getUnackedMessages(), 0);
        localRunner.stop();

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                return topicStats.getSubscriptions().get(subscriptionName) != null
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150);

        //change the schema, the function should not run, resulting in no messages to consume
        schemaInput.put(sourceTopic, "{\"schemaType\":\"AVRO\",\"schemaProperties\":{\"__jsr310ConversionEnabled\":\"false\",\"__alwaysAllowNull\":\"false\"}}");
        localRunner = LocalRunner.builder()
                .functionConfig(functionConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls()).build();
        localRunner.start(false);

        producer.newMessage().property(propertyKey, propertyValue).value(avroTestObjectClass
                .getDeclaredConstructor().newInstance()).send();
        Message<GenericRecord> msg = consumer.receive(2, TimeUnit.SECONDS);
        Assert.assertNull(msg);

        producer.close();
        consumer.close();
        localRunner.stop();
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionLocalRun() throws Throwable {
        runWithPulsarFunctionsClassLoader(() -> testE2EPulsarFunctionLocalRun(null));
    }

    @Test(timeOut = 30000)
    public void testAvroFunctionLocalRun() throws Throwable {
        runWithPulsarFunctionsClassLoader(() -> testAvroFunctionLocalRun(null));
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionLocalRunWithJar() throws Exception {
        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        testE2EPulsarFunctionLocalRun(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testE2EPulsarFunctionLocalRunURL() throws Exception {
        testE2EPulsarFunctionLocalRun(fileServer.getUrl("/pulsar-functions-api-examples.jar"));
    }

    @Test(timeOut = 40000)
    public void testE2EPulsarFunctionLocalRunMultipleInstances() throws Throwable {
        runWithPulsarFunctionsClassLoader(() -> testE2EPulsarFunctionLocalRun(null, 2));
    }

    private void testPulsarSourceLocalRun(String jarFilePathUrl, int parallelism) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sourceName = "PulsarSource-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList(CLUSTER));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        SourceConfig sourceConfig = createSourceConfig(tenant, namespacePortion, sourceName, sinkTopic);
        if (jarFilePathUrl == null || !jarFilePathUrl.endsWith(".nar")) {
            sourceConfig.setClassName("org.apache.pulsar.io.datagenerator.DataGeneratorSource");
        }

        sourceConfig.setArchive(jarFilePathUrl);
        sourceConfig.setParallelism(parallelism);
        int metricsPort = FunctionCommon.findAvailablePort();
        @Cleanup
        LocalRunner localRunner = LocalRunner.builder()
                .sourceConfig(sourceConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls())
                .connectorsDirectory(workerConfig.getConnectorsDirectory())
                .metricsPortStart(metricsPort)
                .build();

        localRunner.start(false);

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).getPublishers().size() == parallelism;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 10, 150));

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                boolean result = false;
                TopicStats sourceStats = admin.topics().getStats(sinkTopic);
                if (sourceStats.getPublishers().size() == parallelism) {
                    for (PublisherStats publisher : sourceStats.getPublishers()) {
                        result = publisher.getMetadata() != null
                                && publisher.getMetadata().containsKey("id")
                                && publisher.getMetadata().get("id").equals(String.format("%s/%s/%s", tenant, namespacePortion, sourceName));
                    }
                }

                return result;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).getPublishers().size() == parallelism)
                        && (admin.topics().getInternalStats(sinkTopic, false).numberOfEntries > 4);
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));
        assertEquals(admin.topics().getStats(sinkTopic).getPublishers().size(), parallelism);

        // validate prometheus metrics
        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(metricsPort);
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metricsMap = new HashMap<>();
        Arrays.asList(prometheusMetrics.split("\n")).forEach(line -> {
            if (line.startsWith("pulsar_source_written_total")) {
                Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(line);
                assertFalse(metrics.isEmpty());
                PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_source_written_total");
                if (m != null) {
                    metricsMap.put(m.tags.get("instance_id"), m);
                }
            }
        });
        Assert.assertEquals(metricsMap.size(), parallelism);

        for (int i = 0; i < parallelism; i++) {
            PulsarFunctionTestUtils.Metric m = metricsMap.get(String.valueOf(i));
            Assert.assertNotNull(m);
            assertEquals(m.tags.get("cluster"), config.getClusterName());
            assertEquals(m.tags.get("instance_id"), String.valueOf(i));
            assertEquals(m.tags.get("name"), sourceName);
            assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
            assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sourceName));
            assertTrue(m.value > 0.0);
        }

        localRunner.stop();

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                return (admin.topics().getStats(sinkTopic).getPublishers().size() == 0);
            } catch (PulsarAdminException e) {
                return e.getStatusCode() == 404;
            }
        }, 10, 150));

        try {
            assertEquals(admin.topics().getStats(sinkTopic).getPublishers().size(), 0);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() != 404) {
                fail();
            }
        }
    }

    private void testPulsarSourceLocalRun(String jarFilePathUrl) throws Exception {
        testPulsarSourceLocalRun(jarFilePathUrl, 1);
    }

    @Test(timeOut = 20000, groups = "builtin")
    public void testPulsarSourceStatsBuiltin() throws Exception {
        testPulsarSourceLocalRun(String.format("%s://data-generator", Utils.BUILTIN));
    }

    @Test(timeOut = 20000)
    public void testPulsarSourceLocalRunNoArchive() throws Throwable {
        runWithNarClassLoader(() -> testPulsarSourceLocalRun(null));
    }

    @Test(timeOut = 20000)
    public void testPulsarSourceLocalRunWithFile() throws Exception {
        String jarFilePathUrl = getPulsarIODataGeneratorNar().toURI().toString();
        testPulsarSourceLocalRun(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSourceLocalRunWithUrl() throws Exception {
        testPulsarSourceLocalRun(fileServer.getUrl("/pulsar-io-data-generator.nar"));
    }

    @Test(timeOut = 40000)
    public void testPulsarSourceLocalRunMultipleInstances() throws Throwable {
        runWithNarClassLoader(() -> testPulsarSourceLocalRun(null, 2));
    }

    private void testPulsarSinkLocalRun(String jarFilePathUrl, int parallelism, String className) throws Exception {
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
        if (className != null) {
            sinkConfig.setClassName(className);
        } else if (jarFilePathUrl == null || !jarFilePathUrl.endsWith(".nar")) {
            sinkConfig.setClassName("org.apache.pulsar.io.datagenerator.DataGeneratorPrintSink");
        }

        sinkConfig.setArchive(jarFilePathUrl);
        sinkConfig.setParallelism(parallelism);
        int metricsPort = FunctionCommon.findAvailablePort();
        @Cleanup
        LocalRunner localRunner = LocalRunner.builder()
                .sinkConfig(sinkConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls())
                .connectorsDirectory(workerConfig.getConnectorsDirectory())
                .metricsPortStart(metricsPort)
                .build();

        localRunner.start(false);

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                boolean result = false;
                TopicStats topicStats = admin.topics().getStats(sourceTopic);
                if (topicStats.getSubscriptions().containsKey(subscriptionName)
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().size() == parallelism) {
                    for (ConsumerStats consumerStats : topicStats.getSubscriptions().get(subscriptionName).getConsumers()) {
                        result = consumerStats.getAvailablePermits() == 1000;
                    }
                }
                return result;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150));

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        Assert.assertTrue(retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200));

        // validate prometheus metrics
        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(metricsPort);
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metricsMap = new HashMap<>();
        Arrays.asList(prometheusMetrics.split("\n")).forEach(line -> {
            if (line.startsWith("pulsar_sink_written_total")) {
                Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(line);
                assertFalse(metrics.isEmpty());
                PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_sink_written_total");
                if (m != null) {
                    metricsMap.put(m.tags.get("instance_id"), m);
                }
            } else if (line.startsWith("pulsar_sink_sink_exceptions_total")) {
                Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(line);
                assertFalse(metrics.isEmpty());
                PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_sink_sink_exceptions_total");
                if (m == null) {
                    m = metrics.get("pulsar_sink_sink_exceptions_1min_total");
                }
                assertEquals(m.value, 0);
            }
        });
        Assert.assertEquals(metricsMap.size(), parallelism);

        double totalNumRecvMsg = 0;
        for (int i = 0; i < parallelism; i++) {
            PulsarFunctionTestUtils.Metric m = metricsMap.get(String.valueOf(i));
            Assert.assertNotNull(m);
            assertEquals(m.tags.get("cluster"), config.getClusterName());
            assertEquals(m.tags.get("instance_id"), String.valueOf(i));
            assertEquals(m.tags.get("name"), sinkName);
            assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
            assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
            totalNumRecvMsg += m.value;
        }
        assertEquals(totalNumRecvMsg, totalMsgs);

        // stop sink
        localRunner.stop();

        Assert.assertTrue(retryStrategically((test) -> {
            try {
                TopicStats stats = admin.topics().getStats(sourceTopic);
                return stats.getSubscriptions().get(subscriptionName) != null
                        && stats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 20, 150));

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertTrue(topicStats.getSubscriptions().get(subscriptionName) != null
                && topicStats.getSubscriptions().get(subscriptionName).getConsumers().isEmpty());

    }

    private void testPulsarSinkLocalRun(String jarFilePathUrl) throws Exception {
        testPulsarSinkLocalRun(jarFilePathUrl, 1);
    }

    private void testPulsarSinkLocalRun(String jarFilePathUrl, int parallelism) throws Exception {
        testPulsarSinkLocalRun(jarFilePathUrl, parallelism, null);
    }

    @Test(timeOut = 20000, groups = "builtin")
    public void testPulsarSinkStatsBuiltin() throws Exception {
        testPulsarSinkLocalRun(String.format("%s://data-generator", Utils.BUILTIN));
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsNoArchive() throws Throwable {
        runWithNarClassLoader(() -> testPulsarSinkLocalRun(null));
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsWithFile() throws Exception {
        String jarFilePathUrl = getPulsarIODataGeneratorNar().toURI().toString();
        testPulsarSinkLocalRun(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSinkStatsWithUrl() throws Exception {
        testPulsarSinkLocalRun(fileServer.getUrl("/pulsar-io-data-generator.nar"));
    }

    @Test(timeOut = 40000)
    public void testPulsarSinkStatsMultipleInstances() throws Throwable {
        runWithNarClassLoader(() -> testPulsarSinkLocalRun(null, 2));
    }

    public static class StatsNullSink implements Sink<ByteBuffer> {
        volatile long bytesTotal = 0;

        @Override
        public void open(Map map, final SinkContext sinkContext) throws Exception {

        }

        @Override
        public void write(Record<ByteBuffer> record) throws Exception {
            bytesTotal += record.getValue().capacity();
            record.ack();
        }

        @Override
        public void close() throws Exception {

        }
    }

    @Test
    public void testPulsarSinkStatsByteBufferType() throws Throwable {
        runWithNarClassLoader(() -> testPulsarSinkLocalRun(null, 1, StatsNullSink.class.getName()));
    }
    
    public static class TestErrorSink implements Sink<byte[]> {
        private Map config;
        @Override
        public void open(Map map, final SinkContext sinkContext) throws Exception {
            config = map;
            if (map.containsKey("throwErrorOpen")) {
                throw new Exception("error on open");
            }
        }

        @Override
        public void write(Record<byte[]> record) throws Exception {
            if (config.containsKey("throwErrorWrite")) {
                throw new Exception("error on write");
            }
            record.ack();
        }

        @Override
        public void close() throws Exception {
            if (config.containsKey("throwErrorClose")) {
                throw new Exception("error on close");
            }
        }
    }

    @Test(timeOut = 20000)
    public void testExitOnError() throws Throwable{

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

        sinkConfig.setClassName(TestErrorSink.class.getName());

        int metricsPort = FunctionCommon.findAvailablePort();

        LocalRunner.LocalRunnerBuilder localRunnerBuilder = LocalRunner.builder()
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls())
                .connectorsDirectory(workerConfig.getConnectorsDirectory())
                .metricsPortStart(metricsPort)
                .exitOnError(true);

        sinkConfig.getConfigs().put("throwErrorOpen", true);
        localRunnerBuilder.sinkConfig(sinkConfig);
        LocalRunner localRunner = localRunnerBuilder.build();
        localRunner.start(true);

        sinkConfig.getConfigs().put("throwErrorWrite", true);
        localRunnerBuilder.sinkConfig(sinkConfig);
        localRunner = localRunnerBuilder.build();
        localRunner.start(true);
    }

    private void runWithNarClassLoader(Assert.ThrowingRunnable throwingRunnable) throws Throwable {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try (NarClassLoader classLoader = NarClassLoaderBuilder.builder()
                .narFile(getPulsarIODataGeneratorNar())
                .parentClassLoader(originalClassLoader)
                .extractionDirectory(NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR)
                .build()) {
            try {
                Thread.currentThread().setContextClassLoader(classLoader);
                throwingRunnable.run();
            } finally {
                Thread.currentThread().setContextClassLoader(originalClassLoader);
            }
        }
    }

    protected void runWithPulsarFunctionsClassLoader(Assert.ThrowingRunnable throwingRunnable) throws Throwable {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pulsarApiExamplesClassLoader);
            throwingRunnable.run();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
}
