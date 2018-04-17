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
package org.apache.pulsar.broker.service.replicator;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.Utils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.WorkerServer;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorProvider;
import org.apache.pulsar.replicator.auth.DefaultAuthParamKeyStore;
import org.apache.pulsar.replicator.function.ReplicatorFunction;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.util.concurrent.DefaultThreadFactory;
import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test External replicator e2e functionality
 *
 */
public class ReplicatorFunctionTest {
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
    private static final Logger log = LoggerFactory.getLogger(ReplicatorFunctionTest.class);

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
        config.setFunctionWorkerServiceUrl(hostHttpUrl + workerServicePort);
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
        String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getAdvertisedAddress());
        workerConfig.setWorkerHostname(hostname);
        workerConfig
                .setWorkerId("c-" + config.getClusterName() + "-fw-" + hostname + "-" + workerConfig.getWorkerPort());
        return new WorkerService(workerConfig);
    }

    @Test(timeOut = 20000)
    public void testExternalReplicatorE2E() throws Exception {

        final String namespacePortion = "myReplNs";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String regionName = "us-east";
        final String topicName = "pulsarTopic";
        final String kinesisReplicatorTopic = "persistent://" + replNamespace + "/" + topicName;
        final ReplicatorType replicatorType = ReplicatorType.Kinesis;
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPolicies replicatorPolicies = new ReplicatorPolicies();
        Map<String, String> topicMapping = Maps.newHashMap();
        Map<String, String> replicationProperties = Maps.newHashMap();
        Map<String, String> authParamData = Maps.newHashMap();
        topicMapping.put(topicName, "KineisFunction:us-west-2");
        authParamData.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "ak");
        authParamData.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "sk");
        replicatorPolicies.topicNameMapping = topicMapping;
        replicatorPolicies.replicationProperties = replicationProperties;
        replicatorPolicies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        ReplicatorPoliciesRequest replicatorPolicieRequest = new ReplicatorPoliciesRequest();
        replicatorPolicieRequest.replicatorPolicies = replicatorPolicies;
        replicatorPolicieRequest.authParamData = authParamData;

        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);

        log.info("Regestering replicator");
        admin.persistentTopics().registerReplicator(kinesisReplicatorTopic, replicatorType, regionName);

        // (1) start replicator and verify replicator-function is started.
        String replicatorTopic = ReplicatorFunction.getFunctionTopicName(replicatorType, namespacePortion);
        retryStrategically((test) -> {
            try {
                return admin.nonPersistentTopics().getStats(replicatorTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // subscription must be created and consumer must be started
        Map<String, NonPersistentSubscriptionStats> subscriptons = admin.nonPersistentTopics().getStats(replicatorTopic)
                .getSubscriptions();
        assertEquals(subscriptons.size(), 1);
        Entry<String, NonPersistentSubscriptionStats> subscriptionEntry = subscriptons.entrySet().iterator().next();
        assertEquals(subscriptionEntry.getValue().consumers.size(), 1);

        // (2) Verify replicator-provider (kinesis-producer) started consumer on pulsar
        // topic
        retryStrategically((test) -> {
            try {
                return admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        Map<String, SubscriptionStats> subscriptionStat = admin.persistentTopics()
                .getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscriptionStat.size(), 1);
        Entry<String, SubscriptionStats> replSubscriptionEntry = subscriptionStat.entrySet().iterator().next();
        assertEquals(replSubscriptionEntry.getValue().consumers.size(), 1);

        // (3) Stop replicator-provider: which removes consumer from the subscription
        log.info("Stopping replicator");
        admin.persistentTopics().updateReplicator(kinesisReplicatorTopic, replicatorType.Kinesis, regionName,
                Action.Stop);
        retryStrategically((test) -> {
            try {
                Map<String, SubscriptionStats> replProviderSubscription = admin.persistentTopics()
                        .getStats(kinesisReplicatorTopic).subscriptions;
                return replProviderSubscription.size() == 1
                        && replProviderSubscription.entrySet().iterator().next().getValue().consumers.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        Map<String, SubscriptionStats> subscription = admin.persistentTopics()
                .getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscription.size(), 1);
        assertEquals(subscription.entrySet().iterator().next().getValue().consumers.size(), 0);

        // (4) Start replicator-provider: which creates a new consumer for a
        // subscription
        log.info("Starting replicator");
        admin.persistentTopics().updateReplicator(kinesisReplicatorTopic, replicatorType.Kinesis, regionName,
                Action.Start);
        retryStrategically((test) -> {
            try {
                Map<String, SubscriptionStats> stats = admin.persistentTopics()
                        .getStats(kinesisReplicatorTopic).subscriptions;
                return stats.size() == 1 && stats.entrySet().iterator().next().getValue().consumers.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        subscriptionStat = admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscriptionStat.size(), 1);
        replSubscriptionEntry = subscriptionStat.entrySet().iterator().next();
        assertEquals(replSubscriptionEntry.getValue().consumers.size(), 1);

        // (5) Re-Start replicator-provider: which creates a new consumer for a
        // subscription
        log.info("Restarting replicator");
        admin.persistentTopics().updateReplicator(kinesisReplicatorTopic, replicatorType.Kinesis, regionName,
                Action.Restart);
        retryStrategically((test) -> {
            try {
                Map<String, SubscriptionStats> stats = admin.persistentTopics()
                        .getStats(kinesisReplicatorTopic).subscriptions;
                return stats.size() == 1 && stats.entrySet().iterator().next().getValue().consumers.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        subscriptionStat = admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscriptionStat.size(), 1);
        replSubscriptionEntry = subscriptionStat.entrySet().iterator().next();
        assertEquals(replSubscriptionEntry.getValue().consumers.size(), 1);

        // (6) Delete Replicator
        log.info("Deleting replicator");
        admin.persistentTopics().deregisterReplicator(kinesisReplicatorTopic, replicatorType, regionName);
        retryStrategically((test) -> {
            try {
                return admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        subscription = admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscription.size(), 0);
    }

    @Test(enabled = false) // TODO: test takes longer time while closing bk (flush out jar) : enable once we have mock
                           // bk for storing function jar
    public void testExternalReplicatorRedirectionToWorkerService() throws Exception {

        final String namespacePortion = "myReplNs";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String topicName = "pulsarTopic";
        final String kinesisReplicatorTopic = "persistent://" + replNamespace + "/" + topicName;
        final ReplicatorType replicatorType = ReplicatorType.Kinesis;
        final String regionName = "us-east";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPolicies replicatorPolicies = new ReplicatorPolicies();
        Map<String, String> topicMapping = Maps.newHashMap();
        Map<String, String> replicationProperties = Maps.newHashMap();
        Map<String, String> authParamData = Maps.newHashMap();
        topicMapping.put(topicName, "KineisFunction:us-west-2");
        authParamData.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "ak");
        authParamData.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "sk");
        replicatorPolicies.topicNameMapping = topicMapping;
        replicatorPolicies.replicationProperties = replicationProperties;
        replicatorPolicies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        ReplicatorPoliciesRequest replicatorPolicieRequest = new ReplicatorPoliciesRequest();
        replicatorPolicieRequest.replicatorPolicies = replicatorPolicies;
        replicatorPolicieRequest.authParamData = authParamData;

        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);

        admin.persistentTopics().registerReplicator(kinesisReplicatorTopic, replicatorType, regionName);

        // (1) start replicator and verify replicator-function is started.
        String replicatorTopic = ReplicatorFunction.getFunctionTopicName(replicatorType, namespacePortion);
        retryStrategically((test) -> {
            try {
                return admin.nonPersistentTopics().getStats(replicatorTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        // subscription must be created and consumer must be started
        Map<String, NonPersistentSubscriptionStats> subscriptons = admin.nonPersistentTopics().getStats(replicatorTopic)
                .getSubscriptions();
        assertEquals(subscriptons.size(), 1);
        Entry<String, NonPersistentSubscriptionStats> subscriptionEntry = subscriptons.entrySet().iterator().next();
        assertEquals(subscriptionEntry.getValue().consumers.size(), 1);

        // (2) delete replicator
        admin.persistentTopics().deregisterReplicator(kinesisReplicatorTopic, replicatorType, regionName);
        retryStrategically((test) -> {
            try {
                return admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        Map<String, SubscriptionStats> subscription = admin.persistentTopics()
                .getStats(kinesisReplicatorTopic).subscriptions;
        assertEquals(subscription.size(), 0);
    }

}
