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
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.io.PulsarFunctionE2ETest;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functions-worker")
public class PulsarFunctionE2ESecurityTest {

    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL brokerWebServiceUrl;
    PulsarService pulsar;
    PulsarAdmin superUserAdmin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    PulsarWorkerService functionsWorkerService;
    final String TENANT = "external-repl-prop";
    final String TENANT2 = "tenant2";

    final String NAMESPACE = "test-ns";
    String pulsarFunctionsNamespace = TENANT + "/pulsar-function-admin";
    String primaryHost;
    String workerId;

    private SecretKey secretKey;

    private static final String SUBJECT = "my-test-subject";
    private static final String ADMIN_SUBJECT = "superUser";
    private static final String ANONYMOUS_ROLE = "anonymousUser";

    private static final Logger log = LoggerFactory.getLogger(PulsarFunctionE2ETest.class);
    private String adminToken;
    private String brokerServiceUrl;
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @DataProvider(name = "validRoleName")
    public Object[][] validRoleName() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet(ADMIN_SUBJECT);
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setAdvertisedAddress("localhost");
        config.setAllowAutoTopicCreationType("non-partitioned");

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        config.setAuthenticationEnabled(true);
        config.setAuthenticationProviders(providers);
        config.setAuthorizationEnabled(true);
        config.setAuthorizationProvider(PulsarAuthorizationProvider.class.getName());
        config.setAnonymousUserRole(ANONYMOUS_ROLE);
        secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey",
                AuthTokenUtils.encodeKeyBase64(secretKey));
        config.setProperties(properties);

        adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_SUBJECT, Optional.empty());

        config.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        config.setBrokerClientAuthenticationParameters(
                "token:" +  adminToken);
        functionsWorkerService = createPulsarFunctionWorker(config);
        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, workerConfig, functionWorkerService, (exitCode) -> {});
        pulsar.start();

        brokerServiceUrl = pulsar.getWebServiceAddress();
        brokerWebServiceUrl = new URL(brokerServiceUrl);

        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("token:" +  adminToken);

        superUserAdmin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).authentication(authToken).build());

        brokerStatsClient = superUserAdmin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        ClusterData clusterData = ClusterData.builder().serviceUrl(brokerWebServiceUrl.toString()).build();
        superUserAdmin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl())
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        if (isNotBlank(workerConfig.getBrokerClientAuthenticationPlugin())
                && isNotBlank(workerConfig.getBrokerClientAuthenticationParameters())) {
            clientBuilder.authentication(workerConfig.getBrokerClientAuthenticationPlugin(),
                    workerConfig.getBrokerClientAuthenticationParameters());
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = TenantInfo.builder()
                .adminRoles(Collections.singleton(ADMIN_SUBJECT))
                .allowedClusters(Collections.singleton("use"))
                .build();
        superUserAdmin.tenants().updateTenant(TENANT, propAdmin);


        final String replNamespace = TENANT + "/" + NAMESPACE;
        superUserAdmin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        superUserAdmin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create another test tenant and namespace
        propAdmin = TenantInfo.builder()
                .allowedClusters(Collections.singleton("use"))
                .build();
        superUserAdmin.tenants().createTenant(TENANT2, propAdmin);
        superUserAdmin.namespaces().createNamespace( TENANT2 + "/" + NAMESPACE);

        while (!functionsWorkerService.getLeaderService().isLeader()) {
            Thread.sleep(1000);
        }
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        try {
            log.info("--- Shutting down ---");
            pulsarClient.close();
            superUserAdmin.close();
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
                ObjectMapperFactory.getThreadLocal().convertValue(new ThreadRuntimeFactoryConfig().setThreadGroupName("use"), Map.class));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort().get());
        workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + config.getWebServicePort().get());
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

        workerConfig.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        workerConfig.setBrokerClientAuthenticationParameters(
                String.format("token:%s", adminToken));

        workerConfig.setAuthenticationEnabled(config.isAuthenticationEnabled());
        workerConfig.setAuthenticationProviders(config.getAuthenticationProviders());
        workerConfig.setAuthorizationEnabled(config.isAuthorizationEnabled());
        workerConfig.setAuthorizationProvider(config.getAuthorizationProvider());

        PulsarWorkerService workerService = new PulsarWorkerService();
        workerService.init(workerConfig, null, false);
        return workerService;
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

    @Test
    public void testAuthorizationWithAnonymousUser() throws Exception {

        final String replNamespace = TENANT + "/" + NAMESPACE;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";



        try (PulsarAdmin admin1 = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).build())
        ) {

            String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();

            FunctionConfig functionConfig = createFunctionConfig(TENANT, NAMESPACE, functionName,
                    sourceTopic, sinkTopic, subscriptionName);

            FunctionConfig functionConfig2 =  createFunctionConfig(TENANT2, NAMESPACE, functionName,
                    sourceTopic, sinkTopic, subscriptionName);

            // creating function should fail since admin1 doesn't have permissions granted yet
            try {
                admin1.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
                fail("client admin shouldn't have permissions to create function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            // grant permissions to annoynmous role
            Set<AuthAction> actions = new HashSet<>();
            actions.add(AuthAction.functions);
            actions.add(AuthAction.produce);
            actions.add(AuthAction.consume);
            superUserAdmin.namespaces().grantPermissionOnNamespace(replNamespace, ANONYMOUS_ROLE, actions);

            // user should be able to create function now
            admin1.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

            // should still fail on a different namespace
            try {
                admin1.functions().createFunctionWithUrl(functionConfig2, jarFilePathUrl);
                fail("client admin shouldn't have permissions to create function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            assertTrue(retryStrategically((test) -> {
                try {
                    return admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName).getNumRunning() == 1
                            && admin1.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 50, 150));
            // validate pulsar sink consumer has started on the topic
            assertEquals(admin1.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

            // create a producer that creates a topic at broker
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
                 Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sub").subscribe()) {

                int totalMsgs = 5;
                for (int i = 0; i < totalMsgs; i++) {
                    String data = "my-message-" + i;
                    producer.newMessage().property(propertyKey, propertyValue).value(data).send();
                }
                retryStrategically((test) -> {
                    try {
                        SubscriptionStats subStats = admin1.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                        return subStats.getUnackedMessages() == 0;
                    } catch (PulsarAdminException e) {
                        return false;
                    }
                }, 50, 150);

                Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
                String receivedPropertyValue = msg.getProperty(propertyKey);
                assertEquals(propertyValue, receivedPropertyValue);


                // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked
                // messages
                // due to publish failure
                assertNotEquals(admin1.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                        totalMsgs);

                // test update functions
                functionConfig.setParallelism(2);
                functionConfig2.setParallelism(2);

                try {
                    admin1.functions().updateFunctionWithUrl(functionConfig2, jarFilePathUrl);
                    fail("client admin shouldn't have permissions to update function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }

                admin1.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

                assertTrue(retryStrategically((test) -> {
                    try {
                        return admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName).getNumRunning() == 2;
                    } catch (PulsarAdminException e) {
                        return false;
                    }
                }, 50, 150));

                // test getFunctionInfo
                try {
                    admin1.functions().getFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to get function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunction(TENANT, NAMESPACE, functionName);

                // test getFunctionInstanceStatus
                try {
                    admin1.functions().getFunctionStatus(TENANT2, NAMESPACE, functionName, 0);
                    fail("client admin shouldn't have permissions to get function status");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName, 0);

                // test getFunctionStatus
                try {
                    admin1.functions().getFunctionStatus(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to get function status");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName);

                // test getFunctionStats
                try {
                    admin1.functions().getFunctionStats(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to get function stats");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunctionStats(TENANT, NAMESPACE, functionName);

                // test getFunctionInstanceStats
                try {
                    admin1.functions().getFunctionStats(TENANT2, NAMESPACE, functionName, 0);
                    fail("client admin shouldn't have permissions to get function stats");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunctionStats(TENANT, NAMESPACE, functionName, 0);

                // test listFunctions
                try {
                    admin1.functions().getFunctions(TENANT2, NAMESPACE);
                    fail("client admin shouldn't have permissions to list functions");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().getFunctions(TENANT, NAMESPACE);

                // test triggerFunction
                try {
                    admin1.functions().triggerFunction(TENANT2, NAMESPACE, functionName, sourceTopic, "foo", null);
                    fail("client admin shouldn't have permissions to trigger function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().triggerFunction(TENANT, NAMESPACE, functionName, sourceTopic, "foo", null);

                // test restartFunctionInstance
                try {
                    admin1.functions().restartFunction(TENANT2, NAMESPACE, functionName, 0);
                    fail("client admin shouldn't have permissions to restart function instance");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().restartFunction(TENANT, NAMESPACE, functionName, 0);

                // test restartFunctionInstances
                try {
                    admin1.functions().restartFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to restart function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

                // test stopFunction instance
                try {
                    admin1.functions().stopFunction(TENANT2, NAMESPACE, functionName, 0);
                    fail("client admin shouldn't have permissions to stop function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().stopFunction(TENANT, NAMESPACE, functionName, 0);

                // test stopFunction all instance
                try {
                    admin1.functions().stopFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to restart function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().stopFunction(TENANT, NAMESPACE, functionName);

                // test startFunction instance
                try {
                    admin1.functions().startFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to restart function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

                // test startFunction all instances
                try {
                    admin1.functions().restartFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to restart function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }
                admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

                // delete functions
                try {
                    admin1.functions().deleteFunction(TENANT2, NAMESPACE, functionName);
                    fail("client admin shouldn't have permissions to delete function");
                } catch (PulsarAdminException.NotAuthorizedException e) {

                }

                try {
                    admin1.functions().deleteFunction(TENANT, NAMESPACE, functionName);
                } catch (PulsarAdminException e) {
                    // This happens because the request becomes outdated. Lets retry again
                    admin1.functions().deleteFunction(TENANT, NAMESPACE, functionName);
                }

                assertTrue(retryStrategically((test) -> {
                    try {
                        TopicStats stats = admin1.topics().getStats(sourceTopic);
                        boolean done = stats.getSubscriptions().size() == 0;
                        if (!done) {
                            log.info("Topic subscription is not cleaned up yet : {}", stats);
                        }
                        return done;
                    } catch (PulsarAdminException e) {
                        return false;
                    }
                }, 100, 150));
            }
        }
    }

    @Test
    public void testAuthorization() throws Exception {
        String token1 = AuthTokenUtils.createToken(secretKey, SUBJECT, Optional.empty());
        String token2 = AuthTokenUtils.createToken(secretKey, "wrong-subject", Optional.empty());

        final String replNamespace = TENANT + "/" + NAMESPACE;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";


        // create user admin client
        AuthenticationToken authToken1 = new AuthenticationToken();
        authToken1.configure("token:" +  token1);

        AuthenticationToken authToken2 = new AuthenticationToken();
        authToken2.configure("token:" +  token2);

        try(PulsarAdmin admin1 = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).authentication(authToken1).build());
            PulsarAdmin admin2 = spy(
                    PulsarAdmin.builder().serviceHttpUrl(brokerServiceUrl).authentication(authToken2).build())
        ) {

            String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();

            FunctionConfig functionConfig = createFunctionConfig(TENANT, NAMESPACE, functionName,
                    sourceTopic, sinkTopic, subscriptionName);

            // creating function should fail since admin1 doesn't have permissions granted yet
            try {
                admin1.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
                fail("client admin shouldn't have permissions to create function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            // grant permissions to admin1
            Set<AuthAction> actions = new HashSet<>();
            actions.add(AuthAction.functions);
            actions.add(AuthAction.produce);
            actions.add(AuthAction.consume);
            superUserAdmin.namespaces().grantPermissionOnNamespace(replNamespace, SUBJECT, actions);

            // user should be able to create function now
            admin1.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

            // admin2 should still fail
            try {
                admin2.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
                fail("client admin shouldn't have permissions to create function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            // creating on another tenant should also fail
            try {
                admin2.functions().createFunctionWithUrl(createFunctionConfig(TENANT2, NAMESPACE, functionName,
                        sourceTopic, sinkTopic, subscriptionName), jarFilePathUrl);
                fail("client admin shouldn't have permissions to create function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            assertTrue(retryStrategically((test) -> {
                try {
                    return admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName).getNumRunning() == 1
                            && admin1.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 50, 150));
            // validate pulsar sink consumer has started on the topic
            assertEquals(admin1.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

            // create a producer that creates a topic at broker
            try(Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sub").subscribe()) {

                int totalMsgs = 5;
                for (int i = 0; i < totalMsgs; i++) {
                    String data = "my-message-" + i;
                    producer.newMessage().property(propertyKey, propertyValue).value(data).send();
                }
                retryStrategically((test) -> {
                    try {
                        SubscriptionStats subStats = admin1.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                        return subStats.getUnackedMessages() == 0;
                    } catch (PulsarAdminException e) {
                        return false;
                    }
                }, 50, 150);

                Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
                String receivedPropertyValue = msg.getProperty(propertyKey);
                assertEquals(propertyValue, receivedPropertyValue);


                // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked
                // messages
                // due to publish failure
                assertNotEquals(admin1.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                        totalMsgs);
            }

            // test update functions
            functionConfig.setParallelism(2);
            // admin2 should still fail
            try {
                admin2.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
                fail("client admin shouldn't have permissions to update function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            admin1.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

            assertTrue(retryStrategically((test) -> {
                try {
                    return admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName).getNumRunning() == 2;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 50, 150));

            // test getFunctionInfo
            try {
                admin2.functions().getFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to get function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunction(TENANT, NAMESPACE, functionName);

            // test getFunctionInstanceStatus
            try {
                admin2.functions().getFunctionStatus(TENANT, NAMESPACE, functionName, 0);
                fail("client admin shouldn't have permissions to get function status");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName, 0);

            // test getFunctionStatus
            try {
                admin2.functions().getFunctionStatus(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to get function status");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunctionStatus(TENANT, NAMESPACE, functionName);

            // test getFunctionStats
            try {
                admin2.functions().getFunctionStats(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to get function stats");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunctionStats(TENANT, NAMESPACE, functionName);

            // test getFunctionInstanceStats
            try {
                admin2.functions().getFunctionStats(TENANT, NAMESPACE, functionName, 0);
                fail("client admin shouldn't have permissions to get function stats");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunctionStats(TENANT, NAMESPACE, functionName, 0);

            // test listFunctions
            try {
                admin2.functions().getFunctions(TENANT, NAMESPACE);
                fail("client admin shouldn't have permissions to list functions");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().getFunctions(TENANT, NAMESPACE);

            // test triggerFunction
            try {
                admin2.functions().triggerFunction(TENANT, NAMESPACE, functionName, sourceTopic, "foo", null);
                fail("client admin shouldn't have permissions to trigger function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().triggerFunction(TENANT, NAMESPACE, functionName, sourceTopic, "foo", null);

            // test restartFunctionInstance
            try {
                admin2.functions().restartFunction(TENANT, NAMESPACE, functionName, 0);
                fail("client admin shouldn't have permissions to restart function instance");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().restartFunction(TENANT, NAMESPACE, functionName, 0);

            // test restartFunctionInstances
            try {
                admin2.functions().restartFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to restart function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

            // test stopFunction instance
            try {
                admin2.functions().stopFunction(TENANT, NAMESPACE, functionName, 0);
                fail("client admin shouldn't have permissions to stop function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().stopFunction(TENANT, NAMESPACE, functionName, 0);

            // test stopFunction all instance
            try {
                admin2.functions().stopFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to restart function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().stopFunction(TENANT, NAMESPACE, functionName);

            // test startFunction instance
            try {
                admin2.functions().startFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to restart function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

            // test startFunction all instances
            try {
                admin2.functions().restartFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to restart function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }
            admin1.functions().restartFunction(TENANT, NAMESPACE, functionName);

            // delete functions
            // admin2 should still fail
            try {
                admin2.functions().deleteFunction(TENANT, NAMESPACE, functionName);
                fail("client admin shouldn't have permissions to delete function");
            } catch (PulsarAdminException.NotAuthorizedException e) {

            }

            try {
                admin1.functions().deleteFunction(TENANT, NAMESPACE, functionName);
            } catch (PulsarAdminException e) {
                // This happens because the request becomes outdated. Lets retry again
                admin1.functions().deleteFunction(TENANT, NAMESPACE, functionName);
            }

            assertTrue(retryStrategically((test) -> {
                try {
                    TopicStats stats = admin1.topics().getStats(sourceTopic);
                    boolean done = stats.getSubscriptions().size() == 0;
                    if (!done) {
                        log.info("Topic subscription is not cleaned up yet : {}", stats);
                    }
                    return done;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 50, 150));
        }
    }
}
