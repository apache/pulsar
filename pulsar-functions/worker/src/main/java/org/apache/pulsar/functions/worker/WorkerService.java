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
import java.net.URI;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

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
    private Namespace dlogNamespace;
    private MembershipManager membershipManager;
    private SchedulerManager schedulerManager;
    private boolean isInitialized = false;

    private ConnectorsManager connectorsManager;

    public WorkerService(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
    }

    public void start(URI dlogUri) throws InterruptedException {
        log.info("Starting worker {}...", workerConfig.getWorkerId());
        try {
            log.info("Worker Configs: {}", new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(workerConfig));
        } catch (JsonProcessingException e) {
            log.warn("Failed to print worker configs with error {}", e.getMessage(), e);
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

            ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(this.workerConfig.getPulsarServiceUrl());
            if (isNotBlank(workerConfig.getClientAuthenticationPlugin())
                    && isNotBlank(workerConfig.getClientAuthenticationParameters())) {
                clientBuilder.authentication(workerConfig.getClientAuthenticationPlugin(),
                        workerConfig.getClientAuthenticationParameters());
            }
            clientBuilder.enableTls(workerConfig.isUseTls());
            clientBuilder.allowTlsInsecureConnection(workerConfig.isTlsAllowInsecureConnection());
            clientBuilder.tlsTrustCertsFilePath(workerConfig.getTlsTrustCertsFilePath());
            clientBuilder.enableTlsHostnameVerification(workerConfig.isTlsHostnameVerificationEnable());
            this.client = clientBuilder.build();
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

            this.clusterServiceCoordinator.addTask("membership-monitor",
                    this.workerConfig.getFailureCheckFreqMs(),
                    () -> membershipManager.checkFailures(
                            functionMetaDataManager, functionRuntimeManager, schedulerManager));

            this.clusterServiceCoordinator.start();

            // Start function runtime manager
            this.functionRuntimeManager.start();

            // indicate function worker service is done intializing
            this.isInitialized = true;

            this.connectorsManager = new ConnectorsManager(workerConfig);

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
