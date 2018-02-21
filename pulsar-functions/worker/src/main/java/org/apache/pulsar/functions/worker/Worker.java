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
import com.google.common.util.concurrent.AbstractService;

import java.io.IOException;
import java.net.URI;

import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.functions.worker.rest.WorkerServer;

import javax.ws.rs.core.Response;

@Slf4j
public class Worker extends AbstractService {

    private final WorkerConfig workerConfig;
    private PulsarClient client;
    private FunctionRuntimeManager functionRuntimeManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private ClusterServiceCoordinator clusterServiceCoordinator;
    private Thread serverThread;
    private Namespace dlogNamespace;
    private MembershipManager membershipManager;
    private SchedulerManager schedulerManager;

    public Worker(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
    }

    @Override
    protected void doStart() {
        try {
            doStartImpl();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected void doStartImpl() throws InterruptedException {
        log.info("Starting worker {}...", workerConfig.getWorkerId());
        try {
            log.info("Worker Configs: {}",new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(workerConfig));
        } catch (JsonProcessingException e) {
            log.warn("Failed to print worker configs with error {}", e.getMessage(), e);
        }

        // initializing pulsar functions namespace
        PulsarAdmin admin = Utils.getPulsarAdminClient(this.workerConfig.getPulsarWebServiceUrl());
        InternalConfigurationData internalConf;
        // make sure pulsar broker is up
        log.info("Checking if broker at {} is up...", this.workerConfig.getPulsarWebServiceUrl());
        int maxRetries = workerConfig.getInitialBrokerReconnectMaxRetries();
        int retries = 0;
        while (true) {
            try {
                admin.clusters().getClusters();
                break;
            } catch (PulsarAdminException e) {
                log.warn("Retry to connect to Pulsar broker at {}", this.workerConfig.getPulsarWebServiceUrl());
                if (retries >= maxRetries) {
                    log.error("Failed to connect to Pulsar broker at {} after {} attempts",
                            this.workerConfig.getPulsarFunctionsNamespace(), maxRetries);
                    throw new RuntimeException("Failed to connect to Pulsar broker");
                }
                retries ++;
                Thread.sleep(1000);
            }
        }

        // getting namespace policy
        log.info("Initializing Pulsar Functions namespace...");
        try {
            try {
                admin.namespaces().getPolicies(this.workerConfig.getPulsarFunctionsNamespace());
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                    // if not found than create
                    try {
                        admin.namespaces().createNamespace(this.workerConfig.getPulsarFunctionsNamespace());
                    } catch (PulsarAdminException e1) {
                        // prevent race condition with other workers starting up
                        if (e1.getStatusCode() != Response.Status.CONFLICT.getStatusCode()) {
                            log.error("Failed to create namespace {} for pulsar functions", this.workerConfig
                                .getPulsarFunctionsNamespace(), e1);
                            throw new RuntimeException(e1);
                        }
                    }
                    try {
                        admin.namespaces().setRetention(
                            this.workerConfig.getPulsarFunctionsNamespace(),
                            new RetentionPolicies(Integer.MAX_VALUE, Integer.MAX_VALUE));
                    } catch (PulsarAdminException e1) {
                        log.error("Failed to set retention policy for pulsar functions namespace", e);
                        throw new RuntimeException(e1);
                    }
                } else {
                    log.error("Failed to get retention policy for pulsar function namespace {}",
                        this.workerConfig.getPulsarFunctionsNamespace(), e);
                    throw new RuntimeException(e);
                }
            }
            try {
                internalConf = admin.brokers().getInternalConfigurationData();
            } catch (PulsarAdminException e) {
                log.error("Failed to retrieve broker internal configuration", e);
                throw new RuntimeException(e);
            }
        } finally {
            admin.close();
        }

        // initialize the dlog namespace
        // TODO: move this as part of pulsar cluster initialization later
        URI dlogUri;
        try {
            dlogUri = Utils.initializeDlogNamespace(
                internalConf.getZookeeperServers(),
                internalConf.getLedgersRootPath());
        } catch (IOException ioe) {
            log.error("Failed to initialize dlog namespace at zookeeper {} for storing function packages",
                    internalConf.getZookeeperServers(), ioe);
            throw new RuntimeException(ioe);
        }

        // create the dlog namespace for storing function packages
        DistributedLogConfiguration dlogConf = Utils.getDlogConf(workerConfig);
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

        // initialize the function metadata manager
        try {

            this.client = PulsarClient.create(this.workerConfig.getPulsarServiceUrl());
            log.info("Created Pulsar client");

            //create scheduler manager
            this.schedulerManager = new SchedulerManager(this.workerConfig, this.client);

            //create function meta data manager
            this.functionMetaDataManager = new FunctionMetaDataManager(
                    this.workerConfig, this.schedulerManager, this.client);

            //create membership manager
            this.membershipManager = new MembershipManager(this.workerConfig, this.client);

            // create function runtime manager
            this.functionRuntimeManager = new FunctionRuntimeManager(
                    this.workerConfig, this.client, this.dlogNamespace, this.membershipManager);

            // Setting references to managers in scheduler
            this.schedulerManager.setFunctionMetaDataManager(this.functionMetaDataManager);
            this.schedulerManager.setFunctionRuntimeManager(this.functionRuntimeManager);
            this.schedulerManager.setMembershipManager(this.membershipManager);

            // initialize function metadata manager
            this.functionMetaDataManager.initialize();

            // Starting cluster services
            log.info("Start cluster services...");
            this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                    this.workerConfig.getWorkerId(),
                    membershipManager);
            // start periodic snapshot routine
            this.clusterServiceCoordinator.addTask(
                    "snapshot",
                    this.workerConfig.getSnapshotFreqMs(),
                    () -> functionMetaDataManager.snapshot());

            this.clusterServiceCoordinator.addTask("membership-monitor",
                    this.workerConfig.getFailureCheckFreqMs(),
                    () -> membershipManager.checkFailures(
                            functionMetaDataManager, functionRuntimeManager, schedulerManager));

            this.clusterServiceCoordinator.start();

            // Start function runtime manager
            this.functionRuntimeManager.start();

        } catch (Exception e) {
            log.error("Error Starting up in worker", e);
            throw new RuntimeException(e);
        }

        WorkerServer server = new WorkerServer(
                this.workerConfig, this.functionMetaDataManager, this.functionRuntimeManager,
                this.membershipManager, this.dlogNamespace);
        this.serverThread = new Thread(server, server.getThreadName());

        log.info("Start worker server on port {}...", this.workerConfig.getWorkerPort());
        this.serverThread.start();
    }

    @Override
    protected void doStop() {
        if (null != serverThread) {
            serverThread.interrupt();
            try {
                serverThread.join();
            } catch (InterruptedException e) {
                log.warn("Worker server thread is interrupted", e);
            }
        }
        if (null != functionMetaDataManager) {
            try {
                functionMetaDataManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function metadata manager", e);
            }
        }
        if (null != functionRuntimeManager) {
            try {
                functionRuntimeManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function runtime manager", e);
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar client", e);
            }
        }

        if (null != clusterServiceCoordinator) {
            clusterServiceCoordinator.close();
        }

        if (null != membershipManager) {
            try {
                membershipManager.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close membership manager", e);
            }
        }

        if (null != schedulerManager) {
            schedulerManager.close();
        }
    }
}
