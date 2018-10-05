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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarWorkerAssignmentTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
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
    private final int brokerServicePort = PortManager.nextFreePort();
    private final int workerServicePort = PortManager.nextFreePort();

    private static final Logger log = LoggerFactory.getLogger(PulsarWorkerAssignmentTest.class);

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        String brokerServiceUrl = "http://127.0.0.1:" + brokerServicePort;
        String brokerWeServiceUrl = "http://127.0.0.1:" + brokerWebServicePort;

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        Set<String> superUsers = Sets.newHashSet("superUser");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(brokerWebServicePort);
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(brokerServicePort);
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());

        functionsWorkerService = createPulsarFunctionWorker(config);
        Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService);
        pulsar.start();

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerWeServiceUrl).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = String.format("http://%s:%d", InetAddress.getLocalHost().getHostName(), brokerWebServicePort);

        // update cluster metadata
        ClusterData clusterData = new ClusterData(brokerServiceUrl);
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = new TenantInfo();
        propAdmin.getAdminRoles().add("superUser");
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() {
        log.info("--- Shutting down ---");
        try {
            pulsarClient.close();
            admin.close();
            functionsWorkerService.stop();
            pulsar.close();
            bkEnsemble.stop();
        } catch (Exception e) {
            log.warn("Encountered errors at shutting down PulsarWorkerAssignmentTest", e);
        }
    }

    private WorkerService createPulsarFunctionWorker(ServiceConfiguration config) {
        workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsNamespace(pulsarFunctionsNamespace);
        workerConfig.setSchedulerClassName(
                org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler.class.getName());
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("use"));
        // worker talks to local broker
        workerConfig.setPulsarServiceUrl("pulsar://127.0.0.1:" + config.getBrokerServicePort());
        workerConfig.setPulsarWebServiceUrl("http://127.0.0.1:" + config.getWebServicePort());
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
        workerConfig.setTopicCompactionFrequencySec(1);

        return new WorkerService(workerConfig);
    }

    @Test
    public void testFunctionAssignments() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String functionName = "assign";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails.Builder functionDetailsBuilder = createFunctionDetails(jarFilePathUrl, tenant, namespacePortion,
                functionName, "my.*", sinkTopic, subscriptionName);
        functionDetailsBuilder.setParallelism(2);
        FunctionDetails functionDetails = functionDetailsBuilder.build();

        // (1) Create function with 2 instance
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).subscriptions.size() == 1
                        && admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers
                                .size() == 2;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate 2 instances have been started
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.size(), 1);
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers.size(), 2);

        // (2) Update function with 1 instance
        functionDetailsBuilder.setParallelism(1);
        functionDetails = functionDetailsBuilder.build();
        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionDetails, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).subscriptions.size() == 1
                        && admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers
                                .size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sinkTopic).subscriptions.values().iterator().next().consumers.size(), 1);
    }

    @Test(timeOut=20000)
    public void testFunctionAssignmentsWithRestart() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String baseFunctionName = "assign-restart";
        final String subscriptionName = "test-sub";
        final int totalFunctions = 5;
        final int parallelism = 2;
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final FunctionRuntimeManager runtimeManager = functionsWorkerService.getFunctionRuntimeManager();

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails.Builder functionDetailsBuilder = null;
        // (1) Register functions with 2 instances
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionDetailsBuilder = createFunctionDetails(jarFilePathUrl, tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionDetailsBuilder.setParallelism(parallelism);
            // set-auto-ack prop =true
            functionDetailsBuilder.setAutoAck(true);
            FunctionDetails functionDetails = functionDetailsBuilder.build();
            admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);
        }
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager.getCurrentAssignments().values().iterator().next();
                return assgn.size() == (totalFunctions * parallelism);
            } catch (Exception e) {
                return false;
            }
        }, 5, 150);

        // Validate registered assignments
        Map<String, Assignment> assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), (totalFunctions * parallelism));

        // (2) Update function with prop=auto-ack and Delete 2 functions
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionDetailsBuilder = createFunctionDetails(jarFilePathUrl, tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionDetailsBuilder.setParallelism(parallelism);
            // set-auto-ack prop =false
            functionDetailsBuilder.setAutoAck(false);
            FunctionDetails functionDetails = functionDetailsBuilder.build();
            admin.functions().updateFunctionWithUrl(functionDetails, jarFilePathUrl);
        }

        int totalDeletedFunction = 2;
        for (int i = (totalFunctions - 1); i >= (totalFunctions - totalDeletedFunction); i--) {
            String functionName = baseFunctionName + i;
            admin.functions().deleteFunction(tenant, namespacePortion, functionName);
        }
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager.getCurrentAssignments().values().iterator().next();
                return assgn.size() == ((totalFunctions - totalDeletedFunction) * parallelism);
            } catch (Exception e) {
                return false;
            }
        }, 5, 150);

        // Validate registered assignments
        assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

        // (3) Restart worker service and check registered functions
        URI dlUri = functionsWorkerService.getDlogUri();
        functionsWorkerService.stop();
        functionsWorkerService = new WorkerService(workerConfig);
        functionsWorkerService.start(dlUri);
        FunctionRuntimeManager runtimeManager2 = functionsWorkerService.getFunctionRuntimeManager();
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager2.getCurrentAssignments().values().iterator().next();
                return assgn.size() == ((totalFunctions - totalDeletedFunction) * parallelism);
            } catch (Exception e) {
                return false;
            }
        }, 5, 150);

        // Validate registered assignments
        assignments = runtimeManager2.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

        // validate updated function prop = auto-ack=false and instnaceid
        for (int i = 0; i < (totalFunctions - totalDeletedFunction); i++) {
            String functionName = baseFunctionName + i;
            assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getAutoAck());
        }
    }

    protected static FunctionDetails.Builder createFunctionDetails(String jarFile, String tenant, String namespace,
            String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {

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

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
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

        return functionDetailsBuilder;
    }

}