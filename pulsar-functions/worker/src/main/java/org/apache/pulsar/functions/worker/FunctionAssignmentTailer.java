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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.functions.proto.Function.Assignment;

import java.io.IOException;

@Slf4j
public class FunctionAssignmentTailer implements AutoCloseable {

    private final FunctionRuntimeManager functionRuntimeManager;
    @Getter
    private final Reader<byte[]> reader;
    private volatile boolean isRunning = false;

    private final Thread tailerThread;
    
    public FunctionAssignmentTailer(
            FunctionRuntimeManager functionRuntimeManager,
            ReaderBuilder readerBuilder,
            WorkerConfig workerConfig,
            ErrorNotifier errorNotifier) throws PulsarClientException {
        this.functionRuntimeManager = functionRuntimeManager;
        
        this.reader = readerBuilder
          .subscriptionRolePrefix(workerConfig.getWorkerId() + "-function-runtime-manager")
          .readerName(workerConfig.getWorkerId() + "-function-runtime-manager")
          .topic(workerConfig.getFunctionAssignmentTopic())
          .readCompacted(true)
          .startMessageId(MessageId.earliest)
          .create();
        
        this.tailerThread = new Thread(() -> {
            while(isRunning) {
                try {
                    Message<byte[]> msg = reader.readNext();
                    processAssignment(msg);
                } catch (Throwable th) {
                    if (isRunning) {
                        log.error("Encountered error in assignment tailer", th);
                        // trigger fatal error
                        isRunning = false;
                        errorNotifier.triggerError(th);
                    } else {
                        if (!(th instanceof InterruptedException || th.getCause() instanceof InterruptedException)) {
                            log.warn("Encountered error when assignment tailer is not running", th);
                        }
                    }

                }
            }
        });
        this.tailerThread.setName("assignment-tailer-thread");
    }

    public void start() {
        isRunning = true;
        tailerThread.start();
    }

    @Override
    public void close() {
        log.info("Stopping function assignment tailer");
        try {
            isRunning = false;
            if (tailerThread != null && tailerThread.isAlive()) {
                tailerThread.interrupt();
            }
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            log.error("Failed to stop function assignment tailer", e);
        }
        log.info("Stopped function assignment tailer");
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
                log.error("[{}] Received bad assignment update at message {}", reader.getTopic(), msg.getMessageId(), e);
                throw new RuntimeException(e);
            }
            log.info("Received assignment update: {}", assignment);
            this.functionRuntimeManager.processAssignment(assignment);
        }
    }
}
