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
package org.apache.pulsar.functions.runtime.worker.rest;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.runtime.worker.FunctionStateManager;
import org.apache.pulsar.functions.runtime.worker.Utils;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequest;
import org.apache.pulsar.functions.runtime.worker.request.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FunctionStateListener implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(FunctionStateListener.class);

    PulsarClient client;
    Consumer consumer;

    private final WorkerConfig workerConfig;
    private final FunctionStateManager functionStateManager;

    public FunctionStateListener(WorkerConfig workerConfig, FunctionStateManager functionStateManager) throws PulsarClientException {
        this.workerConfig = workerConfig;
        this.functionStateManager = functionStateManager;
        this.client = PulsarClient.create(workerConfig.getPulsarServiceUrl());
        this.consumer = client.subscribe(workerConfig.getFunctionMetadataTopic(), workerConfig.getFunctionMetadataTopicSubscription());
    }

    /**
     * Listens for message from the FMT
     */
    @Override
    public void run() {
        try {
            while (true) {
                // Wait for a message
                Message msg = consumer.receive();

                try {
                    ServiceRequest serviceRequest = (ServiceRequest) Utils.getObject(msg.getData());
                    LOG.debug("Received Service Request: {}", serviceRequest);

                    switch(serviceRequest.getRequestType()) {
                        case UPDATE:
                            this.functionStateManager.processUpdate((UpdateRequest) serviceRequest);
                            break;
                        case DELETE:
                            this.functionStateManager.proccessDeregister((DeregisterRequest) serviceRequest);
                            break;
                        default:
                            LOG.warn("Received request with unrecognized type: {}", serviceRequest);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    LOG.error("Error occured at listener: {}", e.getMessage(), e);
                }

                // Acknowledge the message so that it can be deleted by broker
                consumer.acknowledgeAsync(msg);
            }
        } catch (PulsarClientException e) {
            LOG.error("Error receiving message from pulsar consumer", e);
        }
    }

    public String getThreadName() {
        return "worker-listener-thread-" + this.workerConfig.getWorkerId();
    }
}
