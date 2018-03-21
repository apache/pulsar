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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.functions.proto.Request;

import java.io.IOException;
import java.util.function.Function;

@Slf4j
public class FunctionAssignmentTailer
    implements java.util.function.Consumer<Message>, Function<Throwable, Void>, AutoCloseable {

        private final FunctionRuntimeManager functionRuntimeManager;
        private final Reader reader;

    public FunctionAssignmentTailer(FunctionRuntimeManager functionRuntimeManager,
                Reader reader)
            throws PulsarClientException {
            this.functionRuntimeManager = functionRuntimeManager;
            this.reader = reader;
        }

    public void start() {

        receiveOne();
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

        // check if latest
        boolean hasMessageAvailable;
        try {
            hasMessageAvailable = this.reader.hasMessageAvailable();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        if (!hasMessageAvailable) {
            Request.AssignmentsUpdate assignmentsUpdate;
            try {
                assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(msg.getData());
            } catch (IOException e) {
                log.error("Received bad assignment update at message {}", msg.getMessageId(), e);
                // TODO: find a better way to handle bad request
                throw new RuntimeException(e);
            }
            if (log.isDebugEnabled()) {
                log.debug("Received assignment update: {}", assignmentsUpdate);
            }

            this.functionRuntimeManager.processAssignmentUpdate(msg.getMessageId(), assignmentsUpdate);
        }
        // receive next request
        receiveOne();
    }

    @Override
    public Void apply(Throwable cause) {
        log.error("Failed to retrieve messages from assignment update topic", cause);
        // TODO: find a better way to handle consumer functions
        throw new RuntimeException(cause);
    }
}
