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

import java.io.IOException;
import java.util.function.Function;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.proto.Function.Assignment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FunctionAssignmentTailer
    implements java.util.function.Consumer<Message<byte[]>>, Function<Throwable, Void>, AutoCloseable {

    private final FunctionRuntimeManager functionRuntimeManager;
    private final Reader<byte[]> reader;
    private boolean closed = false;

    public FunctionAssignmentTailer(FunctionRuntimeManager functionRuntimeManager, Reader<byte[]> reader) {
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
        if (closed) {
            return;
        }
        log.info("Stopping function state consumer");
        try {
            closed = true;
            reader.close();
        } catch (IOException e) {
            log.error("Failed to stop function state consumer", e);
        }
        log.info("Stopped function state consumer");
    }

    public void processAssignment(Message<byte[]> msg) {
        if(msg.getData()==null || (msg.getData().length==0)) {
            log.info("Received assignment delete: {}", msg.getKey());
            this.functionRuntimeManager.deleteAssignment(msg.getKey());
        } else {
            Assignment assignment;
            try {
                assignment = Assignment.parseFrom(msg.getData());
            } catch (IOException e) {
                log.error("[{}] Received bad assignment update at message {}", reader.getTopic(), msg.getMessageId(),
                        e);
                // TODO: find a better way to handle bad request
                throw new RuntimeException(e);
            }
            log.info("Received assignment update: {}", assignment);
            this.functionRuntimeManager.processAssignment(assignment);
        }
    }

    @Override
    public void accept(Message<byte[]> msg) {
        processAssignment(msg);
        // receive next request
        receiveOne();
    }

    @Override
    public Void apply(Throwable cause) {
        Throwable realCause = FutureUtil.unwrapCompletionException(cause);
        if (realCause instanceof AlreadyClosedException) {
            // if reader is closed because tailer is closed, ignore the exception
            if (closed) {
                // ignore
                return null;
            } else {
                log.error("Reader of assignment update topic is closed unexpectedly", cause);
                throw new RuntimeException(
                    "Reader of assignment update topic is closed unexpectedly",
                    cause
                );
            }
        } else {
            log.error("Failed to retrieve messages from assignment update topic", cause);
            // TODO: find a better way to handle consumer functions
            throw new RuntimeException(cause);
        }
    }
}
