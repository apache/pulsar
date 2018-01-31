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
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.functions.worker.rest.WorkerServer;

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

    protected void doStartImpl() {
        log.info("Start worker {}...", workerConfig.getWorkerId());
        log.info("Worker Configs: {}", workerConfig);
        // initialize the dlog namespace
        // TODO: move this as part of pulsar cluster initialization later
        URI dlogUri;
        try {
            dlogUri = Utils.initializeDlogNamespace(workerConfig.getZookeeperServers());
        } catch (IOException ioe) {
            log.error("Failed to initialize dlog namespace at zookeeper {} for storing function packages",
                    workerConfig.getZookeeperServers(), ioe);
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

        // initializing pulsar functions namespace
        log.info("Initialize Pulsar functions namespace...");
        PulsarAdmin admin = Utils.getPulsarAdminClient(this.workerConfig.getPulsarWebServiceUrl());
        try {
            admin.namespaces().getPolicies(this.workerConfig.getPulsarFunctionsNamespace());
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // if not found than create
                try {
                    admin.namespaces().createNamespace(this.workerConfig.getPulsarFunctionsNamespace());
                } catch (PulsarAdminException e1) {
                    log.error("Failed to create namespace {} for pulsar functions", this.workerConfig
                            .getPulsarFunctionsNamespace(), e1);
                    throw new RuntimeException(e1);
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
        } finally {
            admin.close();
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

            // create function runtime manager
            this.functionRuntimeManager = new FunctionRuntimeManager(
                    this.workerConfig, this.client, this.dlogNamespace);

            //create membership manager
            this.membershipManager = new MembershipManager(this.workerConfig, this.schedulerManager, this.client);

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
            this.clusterServiceCoordinator.start();

            // Start function runtime manager
            this.functionRuntimeManager.start();

            //this.membershipManager.startMonitorMembership();
        } catch (Exception e) {
            log.error("Error Starting up in worker", e);
            throw new RuntimeException(e);
        }

        WorkerServer server = new WorkerServer(
                this.workerConfig, this.functionMetaDataManager, this.functionRuntimeManager, this.dlogNamespace);
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
