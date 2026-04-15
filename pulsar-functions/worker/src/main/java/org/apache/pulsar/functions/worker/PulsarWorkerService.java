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

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import static org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.instance.state.StateStoreProvider;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImplV2;
import org.apache.pulsar.functions.worker.rest.api.SinksImpl;
import org.apache.pulsar.functions.worker.rest.api.SourcesImpl;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.apache.pulsar.functions.worker.service.api.Workers;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;

/**
 * A service component contains everything to run a worker except rest server.
 */
@CustomLog
@Getter
public class PulsarWorkerService implements WorkerService {

    public interface PulsarClientCreator {

        PulsarAdmin newPulsarAdmin(String pulsarServiceUrl, WorkerConfig workerConfig);

        PulsarClient newPulsarClient(String pulsarServiceUrl, WorkerConfig workerConfig);

    }

    private WorkerConfig workerConfig;

    private PulsarClient client;
    private FunctionRuntimeManager functionRuntimeManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private ClusterServiceCoordinator clusterServiceCoordinator;
    // dlog namespace for storing function jars in bookkeeper
    private Namespace dlogNamespace;
    // storage client for accessing state storage for functions
    private MembershipManager membershipManager;
    private SchedulerManager schedulerManager;
    private volatile boolean isInitialized = false;
    private ScheduledExecutorService statsUpdater;
    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private ConnectorsManager connectorsManager;
    private FunctionsManager functionsManager;
    private PulsarAdmin brokerAdmin;
    private PulsarAdmin functionAdmin;
    private MetricsGenerator metricsGenerator;
    private PulsarWorkerOpenTelemetry openTelemetry;
    @VisibleForTesting
    private URI dlogUri;
    private LeaderService leaderService;
    private FunctionAssignmentTailer functionAssignmentTailer;
    private WorkerStatsManager workerStatsManager;
    private Functions<PulsarWorkerService> functions;
    private FunctionsV2<PulsarWorkerService> functionsV2;
    private Sinks<PulsarWorkerService> sinks;
    private Sources<PulsarWorkerService> sources;
    private Workers<PulsarWorkerService> workers;
    @Getter
    private PackageUrlValidator packageUrlValidator;
    private final PulsarClientCreator clientCreator;
    private StateStoreProvider stateStoreProvider;

    @SuppressWarnings("deprecation")
    public PulsarWorkerService() {
        this.clientCreator = new PulsarClientCreator() {
            @Override
            public PulsarAdmin newPulsarAdmin(String pulsarServiceUrl, WorkerConfig workerConfig) {
                // using isBrokerClientAuthenticationEnabled instead of isAuthenticationEnabled in function-worker
                final String brokerClientAuthenticationPlugin;
                final String brokerClientAuthenticationParameters;
                if (workerConfig.isBrokerClientAuthenticationEnabled()) {
                    brokerClientAuthenticationPlugin = workerConfig.getBrokerClientAuthenticationPlugin();
                    brokerClientAuthenticationParameters = workerConfig.getBrokerClientAuthenticationParameters();
                } else {
                    brokerClientAuthenticationPlugin = null;
                    brokerClientAuthenticationParameters = null;
                }
                return WorkerUtils.getPulsarAdminClient(
                        pulsarServiceUrl,
                        brokerClientAuthenticationPlugin,
                        brokerClientAuthenticationParameters,
                        workerConfig.getBrokerClientTrustCertsFilePath(),
                        workerConfig.isTlsAllowInsecureConnection(),
                        workerConfig.isTlsEnableHostnameVerification(),
                        workerConfig);
            }

            @Override
            public PulsarClient newPulsarClient(String pulsarServiceUrl, WorkerConfig workerConfig) {
                // using isBrokerClientAuthenticationEnabled instead of isAuthenticationEnabled in function-worker
                final String brokerClientAuthenticationPlugin;
                final String brokerClientAuthenticationParameters;
                if (workerConfig.isBrokerClientAuthenticationEnabled()) {
                    brokerClientAuthenticationPlugin = workerConfig.getBrokerClientAuthenticationPlugin();
                    brokerClientAuthenticationParameters = workerConfig.getBrokerClientAuthenticationParameters();
                } else {
                    brokerClientAuthenticationPlugin = null;
                    brokerClientAuthenticationParameters = null;
                }
                return WorkerUtils.getPulsarClient(
                        pulsarServiceUrl,
                        brokerClientAuthenticationPlugin,
                        brokerClientAuthenticationParameters,
                        workerConfig.isUseTls(),
                        workerConfig.getBrokerClientTrustCertsFilePath(),
                        workerConfig.isTlsAllowInsecureConnection(),
                        workerConfig.isTlsEnableHostnameVerification(),
                        workerConfig);
            }
        };
    }

    public PulsarWorkerService(PulsarClientCreator clientCreator) {
        this.clientCreator = clientCreator;
    }

    @Override
    public void generateFunctionsStats(SimpleTextOutputStream out) {
        FunctionsStatsGenerator.generate(
            this, out
        );
    }

    public void init(WorkerConfig workerConfig,
                     URI dlogUri,
                     boolean runAsStandalone) {
        this.statsUpdater = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-stats-updater"));
        this.metricsGenerator = new MetricsGenerator(this.statsUpdater, workerConfig);
        this.openTelemetry = new PulsarWorkerOpenTelemetry(workerConfig);
        this.workerConfig = workerConfig;
        this.dlogUri = dlogUri;
        this.workerStatsManager = new WorkerStatsManager(workerConfig, runAsStandalone);
        this.functions = new FunctionsImpl(() -> PulsarWorkerService.this);
        this.functionsV2 = new FunctionsImplV2(() -> PulsarWorkerService.this);
        this.sinks = new SinksImpl(() -> PulsarWorkerService.this);
        this.sources = new SourcesImpl(() -> PulsarWorkerService.this);
        this.workers = new WorkerImpl(() -> PulsarWorkerService.this);
        this.packageUrlValidator = new PackageUrlValidator(workerConfig);
    }

    @Override
    public void initAsStandalone(WorkerConfig workerConfig) throws Exception {
        URI dlogUri = initializeStandaloneWorkerService(clientCreator, workerConfig);
        init(workerConfig, dlogUri, true);
    }

    private static URI initializeStandaloneWorkerService(PulsarClientCreator clientCreator,
                                                         WorkerConfig workerConfig) throws Exception {
        // initializing pulsar functions namespace
        PulsarAdmin admin = clientCreator.newPulsarAdmin(workerConfig.getPulsarWebServiceUrl(), workerConfig);
        InternalConfigurationData internalConf;
        // make sure pulsar broker is up
        log.info().attr("serviceUrl", workerConfig.getPulsarWebServiceUrl())
                .log("Checking if pulsar service is up");
        int maxRetries = workerConfig.getInitialBrokerReconnectMaxRetries();
        int retries = 0;
        while (true) {
            try {
                admin.clusters().getClusters();
                break;
            } catch (PulsarAdminException e) {
                log.warn().exception(e)
                        .log("Failed to retrieve clusters from pulsar service");
                log.warn().attr("serviceUrl", workerConfig.getPulsarWebServiceUrl())
                        .log("Retry to connect to Pulsar service");
                if (retries >= maxRetries) {
                    log.error().attr("namespace",
                            workerConfig.getPulsarFunctionsNamespace())
                            .attr("maxRetries", maxRetries)
                            .exception(e)
                            .log("Failed to connect to Pulsar service");
                    throw e;
                }
                retries++;
                Thread.sleep(1000L);
            }
        }

        // getting namespace policy
        log.info("Initializing Pulsar Functions namespace...");
        try {
            try {
                admin.namespaces().getPolicies(workerConfig.getPulsarFunctionsNamespace());
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    // if not found than create
                    try {
                        Policies policies = createFunctionsNamespacePolicies(workerConfig.getPulsarFunctionsCluster());
                        admin.namespaces().createNamespace(workerConfig.getPulsarFunctionsNamespace(),
                                policies);
                    } catch (PulsarAdminException e1) {
                        // prevent race condition with other workers starting up
                        if (e1.getStatusCode() != Response.Status.CONFLICT.getStatusCode()) {
                            log.error().attr("namespace",
                                    workerConfig.getPulsarFunctionsNamespace())
                                    .exception(e1)
                                    .log("Failed to create namespace for pulsar functions");
                            throw e1;
                        }
                    }
                } else {
                    log.error().attr("namespace",
                            workerConfig.getPulsarFunctionsNamespace())
                            .exception(e)
                            .log("Failed to get retention policy for pulsar function namespace");
                    throw e;
                }
            }
            try {
                internalConf = admin.brokers().getInternalConfigurationData();
            } catch (PulsarAdminException e) {
                log.error().exception(e).log("Failed to retrieve broker internal configuration");
                throw e;
            }
        } finally {
            admin.close();
        }

        // initialize the dlog namespace
        URI dlogURI;
        try {
            if (workerConfig.isInitializedDlogMetadata()) {
                String metadataStoreUrl = removeIdentifierFromMetadataURL(internalConf.getMetadataStoreUrl());
                dlogURI = WorkerUtils.newDlogNamespaceURI(metadataStoreUrl);
            } else {
                dlogURI = WorkerUtils.initializeDlogNamespace(internalConf);
            }
        } catch (IOException ioe) {
            log.error().attr("metadataStoreUrl", internalConf.getMetadataStoreUrl())
                    .attr("metadataServiceUri",
                            internalConf.getBookkeeperMetadataServiceUri())
                    .exception(ioe)
                    .log("Failed to initialize dlog namespace for storing function packages");
            throw ioe;
        }

        return dlogURI;
    }

    @Override
    public void initInBroker(ServiceConfiguration brokerConfig,
                             WorkerConfig workerConfig,
                             PulsarResources pulsarResources,
                             InternalConfigurationData internalConf) throws Exception {

        String namespace = workerConfig.getPulsarFunctionsNamespace();
        String[] a = workerConfig.getPulsarFunctionsNamespace().split("/");
        String tenant = a[0];
        String cluster = workerConfig.getPulsarFunctionsCluster();

        /*
        multiple brokers may be trying to create the property, cluster, and namespace
        for function worker service this in parallel. The function worker service uses the namespace
        to create topics for internal function
        */

        // create tenant for function worker service
        try {
            NamedEntity.checkName(tenant);
            pulsarResources.getTenantResources().createTenant(tenant,
                    new TenantInfoImpl(Sets.newHashSet(workerConfig.getSuperUserRoles()), Sets.newHashSet(cluster)));
            log.info().attr("tenant", tenant)
                    .log("Created tenant for function worker");
        } catch (AlreadyExistsException e) {
            log.debug().attr("cluster", cluster).exception(e)
                    .log("Failed to create already existing property for function worker service");
        } catch (IllegalArgumentException e) {
            log.error().attr("cluster", cluster).exception(e)
                    .log("Failed to create property with invalid name for function worker service");
            throw e;
        } catch (Exception e) {
            log.error().attr("cluster", cluster).exception(e)
                    .log("Failed to create property for function worker");
            throw e;
        }

        // create cluster for function worker service
        try {
            NamedEntity.checkName(cluster);
            ClusterDataImpl clusterData = ClusterDataImpl.builder()
                    .serviceUrl(workerConfig.getPulsarWebServiceUrl())
                    .brokerServiceUrl(workerConfig.getPulsarServiceUrl())
                    .build();
            pulsarResources.getClusterResources().createCluster(cluster, clusterData);
            log.info().attr("cluster", cluster)
                    .log("Created cluster for function worker");
        } catch (AlreadyExistsException e) {
            log.debug().attr("cluster", cluster).exception(e)
                    .log("Failed to create already existing cluster for function worker service");
        } catch (IllegalArgumentException e) {
            log.error().attr("cluster", cluster).exception(e)
                    .log("Failed to create cluster with invalid name for function worker service");
            throw e;
        } catch (Exception e) {
            log.error().attr("cluster", cluster).exception(e)
                    .log("Failed to create cluster for function worker service");
            throw e;
        }

        // create namespace for function worker service
        try {
            Policies policies = createFunctionsNamespacePolicies(workerConfig.getPulsarFunctionsCluster());
            policies.bundles = getBundles(brokerConfig.getDefaultNumberOfNamespaceBundles());
            pulsarResources.getNamespaceResources().createPolicies(NamespaceName.get(namespace), policies);
            log.info().attr("namespace", namespace)
                    .log("Created namespace for function worker service");
        } catch (AlreadyExistsException e) {
            log.debug().attr("namespace", namespace)
                    .log("Failed to create already existing namespace for function worker service");
        } catch (Exception e) {
            log.error().attr("namespace", namespace).exception(e)
                    .log("Failed to create namespace");
            throw e;
        }

        URI dlogURI = null;
        if (brokerConfig.isMetadataStoreBackedByZookeeper()) {
            try {
                // initializing dlog namespace for function worker
                if (workerConfig.isInitializedDlogMetadata()) {
                    String metadataStoreUrl = removeIdentifierFromMetadataURL(internalConf.getMetadataStoreUrl());
                    dlogURI = WorkerUtils.newDlogNamespaceURI(metadataStoreUrl);
                } else {
                    dlogURI = WorkerUtils.initializeDlogNamespace(internalConf);
                }
            } catch (IOException ioe) {
                log.error().attr("metadataStoreUrl",
                        internalConf.getMetadataStoreUrl())
                        .attr("metadataServiceUri",
                                internalConf.getBookkeeperMetadataServiceUri())
                        .exception(ioe)
                        .log("Failed to initialize dlog namespace for storing function packages");
                throw ioe;
            }
        }

        init(workerConfig, dlogURI, false);

        log.info("Function worker service setup completed");
    }

    private static Policies createFunctionsNamespacePolicies(String pulsarFunctionsCluster) {
        Policies policies = new Policies();
        policies.retention_policies = new RetentionPolicies(-1, -1);
        policies.replication_clusters = Collections.singleton(pulsarFunctionsCluster);
        // override inactive_topic_policies so that it's always disabled
        policies.inactive_topic_policies = new InactiveTopicPolicies();
        return policies;
    }

    private void tryCreateNonPartitionedTopic(final String topic) throws PulsarAdminException {
        try {
            getBrokerAdmin().topics().createNonPartitionedTopic(topic);
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.ConflictException) {
                log.warn().attr("topic", topic)
                        .attr("error", e.getMessage())
                        .log("Failed to create topic");
            } else {
                throw e;
            }
        }
    }

    @Override
    public void start(AuthenticationService authenticationService,
                      AuthorizationService authorizationService,
                      ErrorNotifier errorNotifier) throws Exception {

        workerStatsManager.startupTimeStart();
        log.info().attr("workerId", workerConfig.getWorkerId())
                .log("/** Starting worker **/");
        log.info().attr("workerConfig", workerConfig).log("Worker Configs");

        try {
            if (dlogUri != null) {
                DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(workerConfig);
                try {
                    this.dlogNamespace = NamespaceBuilder.newBuilder()
                            .conf(dlogConf)
                            .clientId("function-worker-" + workerConfig.getWorkerId())
                            .uri(dlogUri)
                            .build();
                } catch (Exception e) {
                    log.error().attr("dlogUri", dlogUri).exception(e)
                            .log("Failed to initialize dlog namespace for storing function packages");
                    throw new RuntimeException(e);
                }
            }

            // create the state storage provider for accessing function state
            if (workerConfig.getStateStorageServiceUrl() != null) {
                this.stateStoreProvider =
                        (StateStoreProvider) Class.forName(workerConfig.getStateStorageProviderImplementation())
                                .getConstructor().newInstance();
                Map<String, Object> stateStoreProviderConfig = new HashMap<>();
                stateStoreProviderConfig.put(StateStoreProvider.STATE_STORAGE_SERVICE_URL,
                        workerConfig.getStateStorageServiceUrl());
                this.stateStoreProvider.init(stateStoreProviderConfig);
            }

            final String functionWebServiceUrl = StringUtils.isNotBlank(workerConfig.getFunctionWebServiceUrl())
                    ? workerConfig.getFunctionWebServiceUrl()
                    : (workerConfig.getTlsEnabled()
                        ? workerConfig.getWorkerWebAddressTls() : workerConfig.getWorkerWebAddress());

            this.brokerAdmin = clientCreator.newPulsarAdmin(workerConfig.getPulsarWebServiceUrl(), workerConfig);
            this.functionAdmin = clientCreator.newPulsarAdmin(functionWebServiceUrl, workerConfig);
            this.client = clientCreator.newPulsarClient(workerConfig.getPulsarServiceUrl(), workerConfig);

            tryCreateNonPartitionedTopic(workerConfig.getFunctionAssignmentTopic());
            tryCreateNonPartitionedTopic(workerConfig.getClusterCoordinationTopic());
            tryCreateNonPartitionedTopic(workerConfig.getFunctionMetadataTopic());
            //create scheduler manager
            this.schedulerManager = new SchedulerManager(workerConfig, client, getBrokerAdmin(), workerStatsManager,
                    errorNotifier);

            //create function meta data manager
            this.functionMetaDataManager = new FunctionMetaDataManager(
                    this.workerConfig, this.schedulerManager, this.client, errorNotifier);

            this.connectorsManager = new ConnectorsManager(workerConfig);
            this.functionsManager = new FunctionsManager(workerConfig);

            //create membership manager
            String coordinationTopic = workerConfig.getClusterCoordinationTopic();
            if (!getBrokerAdmin().topics().getSubscriptions(coordinationTopic)
                    .contains(MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION)) {
                getBrokerAdmin().topics()
                        .createSubscription(coordinationTopic, MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION,
                                MessageId.earliest);
            }
            this.membershipManager = new MembershipManager(this, client, getBrokerAdmin());

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
              membershipManager,
              errorNotifier);

            log.info("/** Start Leader Service **/");
            leaderService.start();

            // initialize function metadata manager
            log.info("/** Initializing Metadata Manager **/");
            functionMetaDataManager.initialize();

            // initialize function runtime manager
            log.info("/** Initializing Runtime Manager **/");

            MessageId lastAssignmentMessageId = functionRuntimeManager.initialize();
            Supplier<Boolean> checkIsStillLeader = WorkerUtils.getIsStillLeaderSupplier(membershipManager,
                    workerConfig.getWorkerId());

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
            log.info("/** Starting Metadata Manager **/");
            functionMetaDataManager.start();

            // Starting cluster services
            this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                    workerConfig.getWorkerId(),
                    leaderService,
                    checkIsStillLeader);

            clusterServiceCoordinator.addTask("membership-monitor",
                    workerConfig.getFailureCheckFreqMs(),
                    () -> {
                        // computing a new schedule and checking for failures cannot happen concurrently
                        // both paths of code modify internally cached assignments map in function runtime manager
                        schedulerManager.getSchedulerLock().lock();
                        try {
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
                                log.warn().exception(e).log("Encountered error when running scheduled rebalance");
                            }
                        });
            }

            if (workerConfig.getWorkerListProbeIntervalSec() > 0) {
                clusterServiceCoordinator.addTask("drain-worker-list-probe-periodic-check",
                        workerConfig.getWorkerListProbeIntervalSec() * 1000L,
                        () -> {
                                schedulerManager.updateWorkerDrainMap();
                        });
            }

            log.info("/** Starting Cluster Service Coordinator **/");
            clusterServiceCoordinator.start();

            // indicate function worker service is done initializing
            this.isInitialized = true;

            log.info().attr("workerId", workerConfig.getWorkerId())
                    .log("/** Started worker **/");

            workerStatsManager.setFunctionRuntimeManager(functionRuntimeManager);
            workerStatsManager.setFunctionMetaDataManager(functionMetaDataManager);
            workerStatsManager.setLeaderService(leaderService);
            workerStatsManager.setIsLeader(checkIsStillLeader);
            workerStatsManager.startupTimeEnd();
        } catch (Throwable t) {
            log.error().exception(t).log("Error Starting up in worker");
            throw new RuntimeException(t);
        }
    }

    @Override
    public void stop() {
        if (null != functionMetaDataManager) {
            try {
                functionMetaDataManager.close();
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to close function metadata manager");
            }
        }

        if (null != functionAssignmentTailer) {
            try {
                functionAssignmentTailer.close();
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to close function assignment tailer");
            }
        }

        if (null != functionRuntimeManager) {
            try {
                functionRuntimeManager.close();
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to close function runtime manager");
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
                log.warn().exception(e).log("Failed to close leader service");
            }
        }

        if (null != client) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn().exception(e).log("Failed to close pulsar client");
            }
        }

        if (null != getBrokerAdmin()) {
            getBrokerAdmin().close();
        }

        if (null != functionAdmin) {
            functionAdmin.close();
        }

        if (null != dlogNamespace) {
            dlogNamespace.close();
        }

        if (statsUpdater != null) {
            statsUpdater.shutdownNow();
        }

        if (null != stateStoreProvider) {
            stateStoreProvider.close();
        }

        if (null != openTelemetry) {
            openTelemetry.close();
        }

        if (null != functionsManager) {
            functionsManager.close();
        }

        if (null != connectorsManager) {
            connectorsManager.close();
        }
    }

}
