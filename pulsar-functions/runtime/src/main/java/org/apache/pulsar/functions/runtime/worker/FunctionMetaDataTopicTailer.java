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

import java.io.IOException;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.functions.runtime.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.runtime.worker.request.MarkerRequest;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequest;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.runtime.worker.request.UpdateRequest;

@Slf4j
public class FunctionMetaDataTopicTailer
        implements java.util.function.Consumer<Message>, Function<Throwable, Void>, AutoCloseable {

    private final FunctionMetaDataManager functionMetaDataManager;
    private final Reader reader;

    public FunctionMetaDataTopicTailer(FunctionMetaDataManager functionMetaDataManager,
                                       Reader reader)
            throws PulsarClientException {
        this.functionMetaDataManager = functionMetaDataManager;
        this.reader = reader;
    }

    public void start() {
        initialize();
        receiveOne();
    }

    public void initialize() {
        log.info("Initializing Metadata state...");
        this.functionMetaDataManager.sendIntializationMarker();
    }

    private void receiveOne() {
        reader.readNextAsync()
            .thenAccept(this)
            .exceptionally(this);
    }

    @Override
    public void close() {
        log.info("Stopping function state consumer");
        try {
            reader.close();
        } catch (IOException e) {
            log.error("Failed to stop function state consumer", e);
        }
        log.info("Stopped function state consumer");
    }

    @Override
    public void accept(Message msg) {
        ServiceRequest serviceRequest;
        try {
            serviceRequest = (ServiceRequest) Utils.getObject(msg.getData());
        } catch (IOException | ClassNotFoundException e) {
            log.error("Received bad service request at message {}", msg.getMessageId(), e);
            // TODO: find a better way to handle bad request
            throw new RuntimeException(e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Received Service Request: {}", serviceRequest);
        }

        switch(serviceRequest.getRequestType()) {
            case MARKER:
                this.functionMetaDataManager.processInitializeMarker((MarkerRequest) serviceRequest);
                break;
            case UPDATE:
                this.functionMetaDataManager.processUpdate((UpdateRequest) serviceRequest);
                break;
            case DELETE:
                this.functionMetaDataManager.proccessDeregister((DeregisterRequest) serviceRequest);
                break;
            default:
                log.warn("Received request with unrecognized type: {}", serviceRequest);
        }

        // receive next request
        receiveOne();
    }

    @Override
    public Void apply(Throwable cause) {
        log.error("Failed to retrieve messages from function state topic", cause);
        // TODO: find a better way to handle consumer functions
        throw new RuntimeException(cause);
    }
}
