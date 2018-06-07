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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import io.netty.util.concurrent.DefaultThreadFactory;
import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test Pulsar sink on function
 *
 */
public class PulsarSinkE2ETest {
    LocalBookkeeperEnsemble bkEnsemble;

    ServiceConfiguration config;
    WorkerConfig workerConfig;
    URL url;
    PulsarService pulsar;
    PulsarAdmin admin;
    PulsarClient pulsarClient;
    BrokerStats brokerStatsClient;
    WorkerServer functionsWorkerServer;
    WorkerService functionsWorkerService;
    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    String primaryHost;
    ExecutorService executor;
    ExecutorService workerExecutor;

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int brokerWebServicePort = PortManager.nextFreePort();
    private final int brokerServicePort = PortManager.nextFreePort();
    private final int workerServicePort = PortManager.nextFreePort();
    private static final Logger log = LoggerFactory.getLogger(PulsarSinkE2ETest.class);

    @BeforeMethod
    void setup(Method method) throws Exception {

        log.info("--- Setting up method {} ---", method.getName());

        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        workerExecutor = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-worker-test"));

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, PortManager.nextFreePort());
        bkEnsemble.start();

        String hostHttpUrl = "http://127.0.0.1" + ":";

        config = spy(new ServiceConfiguration());
        config.setClusterName("use");
        config.setWebServicePort(brokerWebServicePort);
        config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config.setBrokerServicePort(brokerServicePort);
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        functionsWorkerService = createPulsarFunctionWorker(config);
        url = new URL(hostHttpUrl + brokerWebServicePort);
        boolean isFunctionWebServerRequired = method.getName()
                .equals("testExternalReplicatorRedirectionToWorkerService");
        Optional<WorkerService> functionWorkerService = isFunctionWebServerRequired ? Optional.ofNullable(null)
                : Optional.of(functionsWorkerService);
        pulsar = new PulsarService(config, functionWorkerService);
        pulsar.start();
        admin = new PulsarAdmin(url, (Authentication) null);
        brokerStatsClient = admin.brokerStats();
        primaryHost = String.format("http://%s:%d", InetAddress.getLocalHost().getHostName(), brokerWebServicePort);

        // update cluster metadata
        ClusterData clusterData = new ClusterData(url.toString());
        admin.clusters().updateCluster(config.getClusterName(), clusterData);

        pulsarClient = PulsarClient.builder().serviceUrl(url.toString()).statsInterval(0, TimeUnit.SECONDS).build();

        TenantInfo propAdmin = new TenantInfo();
        propAdmin.setAllowedClusters(Sets.newHashSet(Lists.newArrayList("use")));
        admin.tenants().updateTenant(tenant, propAdmin);

        if (isFunctionWebServerRequired) {
            URI dlogURI = Utils.initializeDlogNamespace(config.getZookeeperServers(), "/ledgers");
            functionsWorkerService.start(dlogURI);
            functionsWorkerServer = new WorkerServer(functionsWorkerService);
            workerExecutor.submit(functionsWorkerServer);
        }
        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        if (executor != null) {
            executor.shutdown();
        }
        if (workerExecutor != null) {
            workerExecutor.shutdown();
        }
        admin.close();
        pulsar.close();
        functionsWorkerService.stop();
        bkEnsemble.stop();
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
        workerConfig.setWorkerHostname(hostname);
        workerConfig
                .setWorkerId("c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort());
        return new WorkerService(workerConfig);
    }

    /**
     * Validates pulsar sink e2e functionality on functions.  
     * 
     * @throws Exception
     */
    @Test
    public void testE2EPulsarSink() throws Exception {

        final String namespacePortion = "myReplNs";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(sourceTopic);
        Producer<byte[]> producer = producerBuilder.create();

        String jarFilePathUrl = Utils.FILE + ":"
                + PulsarSink.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        FunctionDetails functionDetails = createSinkConfig(jarFilePathUrl, tenant, namespacePortion, "PulsarSink-test");
        admin.functions().createFunctionWithUrl(functionDetails, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar sink consumer has started on the topic
        Assert.assertEquals(admin.topics().getStats(sourceTopic).subscriptions.size(), 1);

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).subscriptions.values()
                        .iterator().next();
                return subStats.unackedMessages == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        Assert.assertEquals(admin.topics().getStats(sourceTopic).subscriptions.values().iterator()
                .next().unackedMessages, 0);

    }

    protected FunctionDetails createSinkConfig(String jarFile, String tenant, String namespace, String sinkName) {

        File file = new File(jarFile);
        try {
            Reflections.loadJar(file);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to load user jar " + file, e);
        }
        String sourceTopicPattern = String.format("persistent://%s/%s/my.*", tenant, namespace);
        Class<?> typeArg = byte[].class;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setTenant(tenant);
        functionDetailsBuilder.setNamespace(namespace);
        functionDetailsBuilder.setName(sinkName);
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(1);
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        sourceSpecBuilder.setTypeClassName(byte[].class.getName());
        sourceSpecBuilder.setTopicsPattern(sourceTopicPattern);
        sourceSpecBuilder.putTopicsToSerDeClassName(sourceTopicPattern, DefaultSerDe.class.getName());
        functionDetailsBuilder.setAutoAck(true);
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec
        SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
        //sinkSpecBuilder.setClassName(PulsarSink.class.getName());
        sinkSpecBuilder.setTopic(String.format("persistent://%s/%s/%s", tenant, namespace, "output"));
        Map<String, Object> sinkConfigMap = Maps.newHashMap();
        sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfigMap));
        sinkSpecBuilder.setTypeClassName(typeArg.getName());
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        return functionDetailsBuilder.build();
    }
}
