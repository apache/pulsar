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
package org.apache.pulsar.functions.runtime.worker;

import com.google.common.util.concurrent.AbstractService;
import java.io.IOException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.runtime.worker.rest.WorkerServer;

@Slf4j
public class Worker extends AbstractService {

    private final WorkerConfig workerConfig;
    private final LimitsConfig limitsConfig;
    private PulsarClient client;
    private FunctionMetaDataManager functionMetaDataManager;
    private FunctionMetaDataTopicTailer functionMetaDataTopicTailer;
    private FunctionContainerFactory functionContainerFactory;
    private Thread serverThread;
    private Namespace dlogNamespace;

    public Worker(WorkerConfig workerConfig, LimitsConfig limitsConfig) {
        this.workerConfig = workerConfig;
        this.limitsConfig = limitsConfig;
    }

    @Override
    protected void doStart() {
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

        // initialize the function metadata manager
        try {
            log.info("Initialize function metadata manager ...");

            this.client = PulsarClient.create(workerConfig.getPulsarServiceUrl());
            ServiceRequestManager reqMgr = new ServiceRequestManager(
                client.createProducer(workerConfig.getFunctionMetadataTopic()));

            this.functionContainerFactory = new ThreadFunctionContainerFactory(limitsConfig.getMaxBufferedTuples(),
                    workerConfig.getPulsarServiceUrl(), new ClientConfiguration());

            this.functionMetaDataManager = new FunctionMetaDataManager(
                workerConfig, limitsConfig, reqMgr, this.functionContainerFactory, this.dlogNamespace);

            ConsumerConfiguration consumerConf = new ConsumerConfiguration();
            consumerConf.setSubscriptionType(SubscriptionType.Exclusive);

            this.functionMetaDataTopicTailer = new FunctionMetaDataTopicTailer(
                    functionMetaDataManager,
                client.subscribe(
                    workerConfig.getFunctionMetadataTopic(),
                    workerConfig.getFunctionMetadataTopicSubscription(),
                    consumerConf));

            log.info("Start worker {}...", workerConfig.getWorkerId());
            log.info("Worker Configs: {}", workerConfig);
            log.info("Limits Configs: {}", limitsConfig);
        } catch (Exception e) {
            log.error("Failed to create pulsar client to {}",
                workerConfig.getPulsarServiceUrl(), e);
            throw new RuntimeException(e);
        }

        WorkerServer server = new WorkerServer(workerConfig, functionMetaDataManager, dlogNamespace);
        this.serverThread = new Thread(server, server.getThreadName());

        log.info("Start worker server on port {}...", workerConfig.getWorkerPort());
        serverThread.start();
        log.info("Start worker function state consumer ...");
        functionMetaDataTopicTailer.start();
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
        if (null != functionMetaDataTopicTailer) {
            functionMetaDataTopicTailer.close();
        }
        if (null != functionMetaDataManager) {
            functionMetaDataManager.close();
        }
        if (null != client) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar client", e);
            }
        }
    }
}
