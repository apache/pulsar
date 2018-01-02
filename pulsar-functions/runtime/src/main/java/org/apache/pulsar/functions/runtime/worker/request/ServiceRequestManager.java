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
package org.apache.pulsar.functions.runtime.worker.request;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.runtime.worker.Utils;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ServiceRequestManager {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceRequestManager.class);

    PulsarClient client;
    Producer producer;

    public ServiceRequestManager(WorkerConfig workerConfig) throws PulsarClientException {
        String pulsarBrokerRootUrl = workerConfig.getPulsarServiceUrl();
        client = PulsarClient.create(pulsarBrokerRootUrl);
        String topic = workerConfig.getFunctionMetadataTopic();

        producer = client.createProducer(topic);
    }

    public CompletableFuture<MessageId> submitRequest(ServiceRequest serviceRequest) {
        LOG.debug("Submitting Service Request: {}", serviceRequest);
        byte[] bytes;
        try {
            bytes = Utils.toByteArray(serviceRequest);
        } catch (IOException e) {
            LOG.error("error serializing request: " + serviceRequest);
            throw new RuntimeException(e);
        }

        CompletableFuture<MessageId> messageIdCompletableFuture = send(bytes);

        return messageIdCompletableFuture;
    }

    public CompletableFuture<MessageId> send(byte[] message) {
        CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(message);

        return messageIdCompletableFuture;
    }
}
