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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A service component contains everything to run a worker except rest server.
 */
@Slf4j
@Getter
public class WorkerService {

    private final WorkerConfig workerConfig;

    private PulsarClient client;
    private FunctionRuntimeManager functionRuntimeManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private ClusterServiceCoordinator clusterServiceCoordinator;
    // dlog namespace for storing function jars in bookkeeper
    private Namespace dlogNamespace;
    // storage client for accessing state storage for functions
    private StorageAdminClient stateStoreAdminClient;
    private MembershipManager membershipManager;
    private SchedulerManager schedulerManager;
    private volatile boolean isInitialized = false;
    private final ScheduledExecutorService statsUpdater;
    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ConnectorsManager connectorsManager;
    private FunctionsManager functionsManager;
    private PulsarAdmin brokerAdmin;
    private PulsarAdmin functionAdmin;
    private final MetricsGenerator metricsGenerator;
    @VisibleForTesting
    private URI dlogUri;
    private LeaderService leaderService;
    private FunctionAssignmentTailer functionAssignmentTailer;
    private final WorkerStatsManager workerStatsManager;

    public WorkerService(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.statsUpdater = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-stats-updater"));
        this.metricsGenerator = new MetricsGenerator(this.statsUpdater, workerConfig);
        workerStatsManager = new WorkerStatsManager(workerConfig);
    }

    public void start(URI dlogUri,
                      AuthenticationService authenticationService,
                      AuthorizationService authorizationService,
                      ErrorNotifier errorNotifier) throws InterruptedException {

        workerStatsManager.startupTimeStart();
        log.info("/** Starting worker id={} **/", workerConfig.getWorkerId());

        try {
            log.info("Worker Configs: {}", new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(workerConfig));
        } catch (JsonProcessingException e) {
            log.warn("Failed to print worker configs with error {}", e.getMessage(), e);
        }

        try {
            // create the dlog namespace for storing function packages
            dlogUri = dlogUri;
            DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(workerConfig);
            try {
                this.dlogNamespace = NamespaceBuilder.newBuilder()
                        .conf(dlogConf)
                        .clientId("function-worker-" + workerConfig.getWorkerId())
                        .uri(dlogUri)
                        .build();
            } catch (Exception e) {
                log.error("Failed to initialize dlog namespace {} for storing function packages",
                        dlogUri, e);
                throw new RuntimeException(e);
            }

            // create the state storage client for accessing function state
            if (workerConfig.getStateStorageServiceUrl() != null) {
                StorageClientSettings clientSettings = StorageClientSettings.newBuilder()
                        .serviceUri(workerConfig.getStateStorageServiceUrl())
                        .build();
                this.stateStoreAdminClient = StorageClientBuilder.newBuilder()
                        .withSettings(clientSettings)
                        .buildAdmin();
            }

            final String functionWebServiceUrl = StringUtils.isNotBlank(workerConfig.getFunctionWebServiceUrl())
                    ? workerConfig.getFunctionWebServiceUrl()
                    : workerConfig.getWorkerWebAddress();

            if (workerConfig.isAuthenticationEnabled()) {
                // for compatible, if user do not define brokerClientTrustCertsFilePath, we will use tlsTrustCertsFilePath,
                // otherwise we will use brokerClientTrustCertsFilePath
                final String pulsarClientTlsTrustCertsFilePath;
                if (StringUtils.isNotBlank(workerConfig.getBrokerClientTrustCertsFilePath())) {
                    pulsarClientTlsTrustCertsFilePath = workerConfig.getBrokerClientTrustCertsFilePath();
                } else {
                    pulsarClientTlsTrustCertsFilePath = workerConfig.getTlsTrustCertsFilePath();
                }

                this.brokerAdmin = WorkerUtils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl(),
                    workerConfig.getBrokerClientAuthenticationPlugin(), workerConfig.getBrokerClientAuthenticationParameters(),
                    pulsarClientTlsTrustCertsFilePath, workerConfig.isTlsAllowInsecureConnection(),
                    workerConfig.isTlsEnableHostnameVerification());

                this.functionAdmin = WorkerUtils.getPulsarAdminClient(functionWebServiceUrl,
                    workerConfig.getBrokerClientAuthenticationPlugin(), workerConfig.getBrokerClientAuthenticationParameters(),
                    workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection(),
                    workerConfig.isTlsEnableHostnameVerification());

                this.client = WorkerUtils.getPulsarClient(workerConfig.getPulsarServiceUrl(),
                        workerConfig.getBrokerClientAuthenticationPlugin(),
                        workerConfig.getBrokerClientAuthenticationParameters(),
                        workerConfig.isUseTls(), pulsarClientTlsTrustCertsFilePath,
                        workerConfig.isTlsAllowInsecureConnection(), workerConfig.isTlsEnableHostnameVerification());
            } else {
                this.brokerAdmin = WorkerUtils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl());

                this.functionAdmin = WorkerUtils.getPulsarAdminClient(functionWebServiceUrl);

                this.client = WorkerUtils.getPulsarClient(workerConfig.getPulsarServiceUrl());
            }

            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getFunctionAssignmentTopic());
            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getClusterCoordinationTopic());
            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getFunctionMetadataTopic());
            //create scheduler manager
            this.schedulerManager = new SchedulerManager(workerConfig, client, brokerAdmin, workerStatsManager, errorNotifier);

            //create function meta data manager
            this.functionMetaDataManager = new FunctionMetaDataManager(
                    this.workerConfig, this.schedulerManager, this.client, errorNotifier);

            this.connectorsManager = new ConnectorsManager(workerConfig);
            this.functionsManager = new FunctionsManager(workerConfig);

            //create membership manager
            String coordinationTopic = workerConfig.getClusterCoordinationTopic();
            if (!brokerAdmin.topics().getSubscriptions(coordinationTopic).contains(MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION)) {
                brokerAdmin.topics().createSubscription(coordinationTopic, MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION, MessageId.earliest);
            }
            this.membershipManager = new MembershipManager(this, client, brokerAdmin);

            // create function runtime manager
            this.functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    this,
                    dlogNamespace,
                    membershipManager,
                    connectorsManager,
                    functionsManager,
                    functionMetaDataManager,
                    workerStatsManager,
                    errorNotifier);


            // initialize function assignment tailer that reads from the assignment topic
            this.functionAssignmentTailer = new FunctionAssignmentTailer(
                    functionRuntimeManager,
                    client.newReader(),
                    workerConfig,
                    errorNotifier);

            // Start worker early in the worker service init process so that functions don't get re-assigned because
            // initialize operations of FunctionRuntimeManager and FunctionMetadataManger might take a while
            this.leaderService = new LeaderService(this,
              client,
              functionAssignmentTailer,
              schedulerManager,
              functionRuntimeManager,
              functionMetaDataManager,
              errorNotifier);

            log.info("/** Start Leader Service **/");
            leaderService.start();

            // initialize function metadata manager
            log.info("/** Initializing Metdata Manager **/");
            functionMetaDataManager.initialize();

            // initialize function runtime manager
            log.info("/** Initializing Runtime Manager **/");

            MessageId lastAssignmentMessageId = functionRuntimeManager.initialize();

            // Setting references to managers in scheduler
            schedulerManager.setFunctionMetaDataManager(functionMetaDataManager);
            schedulerManager.setFunctionRuntimeManager(functionRuntimeManager);
            schedulerManager.setMembershipManager(membershipManager);
            schedulerManager.setLeaderService(leaderService);

            this.authenticationService = authenticationService;

            this.authorizationService = authorizationService;

            // Start function assignment tailer
            log.info("/** Starting Function Assignment Tailer **/");
            functionAssignmentTailer.startFromMessage(lastAssignmentMessageId);
            
            // start function metadata manager
            log.info("/** Starting Metdata Manager **/");
            functionMetaDataManager.start();

            // Starting cluster services
            this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                    workerConfig.getWorkerId(),
                    leaderService);

            clusterServiceCoordinator.addTask("membership-monitor",
                    workerConfig.getFailureCheckFreqMs(),
                    () -> {
                        // computing a new schedule and checking for failures cannot happen concurrently
                        // both paths of code modify internally cached assignments map in function runtime manager
                        try {
                            schedulerManager.getSchedulerLock().lock();
                            membershipManager.checkFailures(
                                    functionMetaDataManager, functionRuntimeManager, schedulerManager);
                        } finally {
                            schedulerManager.getSchedulerLock().unlock();
                        }
                    });

            if (workerConfig.getRebalanceCheckFreqSec() > 0) {
                clusterServiceCoordinator.addTask("rebalance-periodic-check",
                        workerConfig.getRebalanceCheckFreqSec() * 1000,
                        () -> {
                            try {
                                schedulerManager.rebalanceIfNotInprogress().get();
                            } catch (SchedulerManager.RebalanceInProgressException e) {
                                log.info("Scheduled for rebalance but rebalance is already in progress. Ignoring.");
                            } catch (Exception e) {
                                log.warn("Encountered error when running scheduled rebalance", e);
                            }
                        });
            }

            log.info("/** Starting Cluster Service Coordinator **/");
            clusterServiceCoordinator.start();

            // indicate function worker service is done initializing
            this.isInitialized = true;

            log.info("/** Started worker id={} **/", workerConfig.getWorkerId());

            workerStatsManager.setFunctionRuntimeManager(functionRuntimeManager);
            workerStatsManager.setFunctionMetaDataManager(functionMetaDataManager);
            workerStatsManager.setLeaderService(leaderService);
            workerStatsManager.startupTimeEnd();
        } catch (Throwable t) {
            log.error("Error Starting up in worker", t);
            throw new RuntimeException(t);
        }
    }

    public void stop() {
        if (null != functionMetaDataManager) {
            try {
                functionMetaDataManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function metadata manager", e);
            }
        }

        if (null != functionAssignmentTailer) {
            try {
                functionAssignmentTailer.close();
            } catch (Exception e) {
                log.warn("Failed to close function assignment tailer", e);
            }
        }

        if (null != functionRuntimeManager) {
            try {
                functionRuntimeManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function runtime manager", e);
            }
        }

        if (null != clusterServiceCoordinator) {
            clusterServiceCoordinator.close();
        }

        if (null != membershipManager) {
            membershipManager.close();
        }

        if (null != schedulerManager) {
            schedulerManager.close();
        }

        if (null != leaderService) {
            try {
                leaderService.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close leader service", e);
            }
        }

        if (null != client) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar client", e);
            }
        }

        if (null != brokerAdmin) {
            brokerAdmin.close();
        }

        if (null != functionAdmin) {
            functionAdmin.close();
        }

        if (null != stateStoreAdminClient) {
            stateStoreAdminClient.close();
        }

        if (null != dlogNamespace) {
            dlogNamespace.close();
        }

        if (statsUpdater != null) {
            statsUpdater.shutdownNow();
        }
    }

}
