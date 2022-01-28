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
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 *
 */
@Slf4j
@Test(groups = "functions-worker")
public class PulsarWorkerAssignmentTest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    PulsarWorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    final String pulsarFunctionsNamespace = tenant + "/pulsar-function-admin";
    String primaryHost;
    String workerId;
    private PulsarFunctionTestTemporaryDirectory tempDirectory;

    @BeforeMethod(timeOut = 60000)
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        config = spy(ServiceConfiguration.class);
        config.setClusterName("use");
        final Set<String> superUsers = Sets.newHashSet("superUser", "admin");
        config.setSuperUserRoles(superUsers);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setAdvertisedAddress("localhost");

        functionsWorkerService = createPulsarFunctionWorker(config);
        final Optional<WorkerService> functionWorkerService = Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, workerConfig, functionWorkerService, (exitCode) -> {});
        pulsar.start();

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build());

        brokerStatsClient = admin.brokerStats();
        primaryHost = pulsar.getWebServiceAddress();

        // update cluster metadata
        final ClusterData clusterData = ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build();
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        final ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        pulsarClient = clientBuilder.build();

        TenantInfo propAdmin = TenantInfo.builder()
                .adminRoles(Collections.singleton("superUser"))
                .allowedClusters(Collections.singleton("use"))
                .build();
        admin.tenants().updateTenant(tenant, propAdmin);

        Thread.sleep(100);
    }

    @AfterMethod(alwaysRun = true)
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
        final String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        workerId = "c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort();
        workerConfig.setWorkerHostname(hostname);
        workerConfig.setWorkerId(workerId);
        workerConfig.setTopicCompactionFrequencySec(1);

        PulsarWorkerService workerService = new PulsarWorkerService();
        workerService.init(workerConfig, null, false);
        return workerService;
    }

    @Test(timeOut = 60000, enabled = false)
    public void testFunctionAssignments() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String functionName = "assign";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        final Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        final String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion,
                functionName, "my.*", sinkTopic, subscriptionName);
        functionConfig.setParallelism(2);

        // (1) Create function with 2 instance
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).getSubscriptions().size() == 1
                        && admin.topics().getStats(sinkTopic).getSubscriptions().values().iterator().next().getConsumers()
                                .size() == 2;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate 2 instances have been started
        assertEquals(admin.topics().getStats(sinkTopic).getSubscriptions().size(), 1);
        assertEquals(admin.topics().getStats(sinkTopic).getSubscriptions().values().iterator().next().getConsumers().size(), 2);

        // (2) Update function with 1 instance
        functionConfig.setParallelism(1);
        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sinkTopic).getSubscriptions().size() == 1
                        && admin.topics().getStats(sinkTopic).getSubscriptions().values().iterator().next().getConsumers()
                                .size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        log.info("admin.topics().getStats(sinkTopic): {}", new Gson().toJson(admin.topics().getStats(sinkTopic)));
        assertEquals(admin.topics().getStats(sinkTopic).getSubscriptions().values().iterator().next().getConsumers().size(), 1);
    }

    @Test(timeOut = 60000, enabled = false)
    public void testFunctionAssignmentsWithRestart() throws Exception {

        final String namespacePortion = "assignment-test";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/my-topic1";
        final String logTopic = "persistent://" + replNamespace + "/log-topic";
        final String baseFunctionName = "assign-restart";
        final String subscriptionName = "test-sub";
        final int totalFunctions = 5;
        final int parallelism = 2;
        admin.namespaces().createNamespace(replNamespace);
        final Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final FunctionRuntimeManager runtimeManager = functionsWorkerService.getFunctionRuntimeManager();

        final String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig;
        // (1) Register functions with 2 instances
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionConfig.setParallelism(parallelism);
            // don't set any log topic
            admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        }
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager.getCurrentAssignments().values().iterator().next();
                return assgn.size() == (totalFunctions * parallelism);
            } catch (Exception e) {
                return false;
            }
        }, 50, 150);

        // Validate registered assignments
        Map<String, Assignment> assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), (totalFunctions * parallelism));

        // (2) Update function with prop=auto-ack and Delete 2 functions
        for (int i = 0; i < totalFunctions; i++) {
            String functionName = baseFunctionName + i;
            functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                    "my.*", sinkTopic, subscriptionName);
            functionConfig.setParallelism(parallelism);
            // Now set the log topic
            functionConfig.setLogTopic(logTopic);
            admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);
        }

        final int totalDeletedFunction = 2;
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
        }, 50, 150);

        // Validate registered assignments
        assignments = runtimeManager.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

        // (3) Restart worker service and check registered functions
        final URI dlUri = functionsWorkerService.getDlogUri();
        functionsWorkerService.stop();
        functionsWorkerService = new PulsarWorkerService();
        functionsWorkerService.init(workerConfig, dlUri, false);
        functionsWorkerService.start(new AuthenticationService(PulsarConfigurationLoader.convertFrom(workerConfig)),
                null,
                ErrorNotifier.getDefaultImpl());
        final FunctionRuntimeManager runtimeManager2 = functionsWorkerService.getFunctionRuntimeManager();
        retryStrategically((test) -> {
            try {
                Map<String, Assignment> assgn = runtimeManager2.getCurrentAssignments().values().iterator().next();
                return assgn.size() == ((totalFunctions - totalDeletedFunction) * parallelism);
            } catch (Exception e) {
                return false;
            }
        }, 50, 150);

        // Validate registered assignments
        assignments = runtimeManager2.getCurrentAssignments().values().iterator().next();
        assertEquals(assignments.size(), ((totalFunctions - totalDeletedFunction) * parallelism));

        // validate updated function prop = auto-ack=false and instance id
        for (int i = 0; i < (totalFunctions - totalDeletedFunction); i++) {
            final String functionName = baseFunctionName + i;
            assertEquals(admin.functions().getFunction(tenant, namespacePortion, functionName).getLogTopic(), logTopic);
        }
    }

    protected static FunctionConfig createFunctionConfig(String tenant,
                                                         String namespace,
                                                         String functionName,
                                                         String sourceTopic,
                                                         String sinkTopic,
                                                         String subscriptionName) {

        final String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);

        final FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setParallelism(1);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");

        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        functionConfig.setTopicsPattern(sourceTopicPattern);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setAutoAck(true);
        functionConfig.setOutput(sinkTopic);

        return functionConfig;
    }

}
