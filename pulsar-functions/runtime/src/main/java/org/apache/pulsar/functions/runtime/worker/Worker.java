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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
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

    public Worker(WorkerConfig workerConfig, LimitsConfig limitsConfig) {
        this.workerConfig = workerConfig;
        this.limitsConfig = limitsConfig;
    }

    @Override
    protected void doStart() {
        try {
            this.client = PulsarClient.create(workerConfig.getPulsarServiceUrl());
            ServiceRequestManager reqMgr = new ServiceRequestManager(
                client.createProducer(workerConfig.getFunctionMetadataTopic()));

            this.functionContainerFactory = new ThreadFunctionContainerFactory(limitsConfig.getMaxBufferedTuples(),
                    workerConfig.getPulsarServiceUrl(), new ClientConfiguration());

            this.functionMetaDataManager = new FunctionMetaDataManager(
                workerConfig, limitsConfig, reqMgr, this.functionContainerFactory);

            ConsumerConfiguration consumerConf = new ConsumerConfiguration();
            consumerConf.setSubscriptionType(SubscriptionType.Exclusive);

            this.functionMetaDataTopicTailer = new FunctionMetaDataTopicTailer(
                    functionMetaDataManager,
                client.subscribe(
                    workerConfig.getFunctionMetadataTopic(),
                    workerConfig.getFunctionMetadataTopicSubscription(),
                    consumerConf));

            log.info("Start worker {}...", workerConfig.getWorkerId());
        } catch (Exception e) {
            log.error("Failed to create pulsar client to {}",
                workerConfig.getPulsarServiceUrl(), e);
            throw new RuntimeException(e);
        }

        WorkerServer server = new WorkerServer(workerConfig, functionMetaDataManager);
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
