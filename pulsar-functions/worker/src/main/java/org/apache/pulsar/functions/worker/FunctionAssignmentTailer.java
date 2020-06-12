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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.functions.proto.Function.Assignment;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for reading assignments from the 'assignments' functions internal topic.
 * Only functions worker leader writes to the topic while other workers read from the topic.
 * When a worker become a leader, the worker will read to the end of the assignments topic and close its reader to the topic.
 * Then the worker and new leader will be in charge of computing new assignments when necessary.
 * The leader does not need to listen to the assignments topic because it can just update its in memory assignments map directly
 * after it computes a new scheduling.  When a worker loses leadership, the worker is start reading from the assignments topic again.
 */
@Slf4j
public class FunctionAssignmentTailer implements AutoCloseable {

    private final FunctionRuntimeManager functionRuntimeManager;
    private final ReaderBuilder readerBuilder;
    private final WorkerConfig workerConfig;
    private final ErrorNotifier errorNotifier;
    @Getter
    private Reader<byte[]> reader;
    private volatile boolean isRunning = false;
    private volatile boolean exitOnEndOfTopic = false;
    private CompletableFuture<Void> hasExited;

    private Thread tailerThread;

    @Getter
    @Setter
    private MessageId lastMessageId = null;
    
    public FunctionAssignmentTailer(
            FunctionRuntimeManager functionRuntimeManager,
            ReaderBuilder readerBuilder,
            WorkerConfig workerConfig,
            ErrorNotifier errorNotifier) throws PulsarClientException {
        this.functionRuntimeManager = functionRuntimeManager;
        this.hasExited = new CompletableFuture<>();
        this.readerBuilder = readerBuilder;
        this.workerConfig = workerConfig;
        this.errorNotifier = errorNotifier;
        this.reader = createReader();
        
        this.tailerThread = getTailerThread();
    }

    public CompletableFuture<Void> triggerReadToTheEndAndExit() {
        exitOnEndOfTopic = true;
        return this.hasExited;
    }

    public synchronized void start() throws PulsarClientException {
        if (!isRunning) {
            isRunning = true;
            if (reader == null) {
                reader = createReader();
            }
            if (tailerThread == null || !tailerThread.isAlive()) {
                tailerThread = getTailerThread();
            }
            tailerThread.start();
        }
    }
    

    @Override
    public synchronized void close() {
        log.info("Closing function assignment tailer");
        try {
            isRunning = false;

            if (tailerThread != null) {
                while (true) {
                    tailerThread.interrupt();

                    try {
                        tailerThread.join(5000, 0);
                    } catch (InterruptedException e) {
                        log.warn("Waiting for assignment tailer thread to stop is interrupted", e);
                    }

                    if (tailerThread.isAlive()) {
                        log.warn("Assignment tailer thread is still alive.  Will attempt to interrupt again.");
                    } else {
                        break;
                    }
                }
                tailerThread = null;
            }
            if (reader != null) {
                reader.close();
                reader = null;
            }

            hasExited = new CompletableFuture<>();
            exitOnEndOfTopic = false;
            
        } catch (IOException e) {
            log.error("Failed to stop function assignment tailer", e);
        }
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
    
    private Reader<byte[]> createReader() throws PulsarClientException {
        MessageId startMessageId = lastMessageId == null ? MessageId.earliest : lastMessageId;
        log.info("Assignment tailer will start reading from message id {}", startMessageId);

        return readerBuilder
                .subscriptionRolePrefix(workerConfig.getWorkerId() + "-function-runtime-manager")
                .readerName(workerConfig.getWorkerId() + "-function-runtime-manager")
                .topic(workerConfig.getFunctionAssignmentTopic())
                .readCompacted(true)
                .startMessageId(startMessageId)
                .create();
    }

    private Thread getTailerThread() {
        Thread t = new Thread(() -> {
            while (isRunning) {
                try {
                    Message<byte[]> msg = reader.readNext(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        if (exitOnEndOfTopic && !reader.hasMessageAvailable()) {
                            break;
                        }
                    } else {
                        processAssignment(msg);
                        // keep track of the last message read
                        lastMessageId = msg.getMessageId();
                    }
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
            log.info("tailer thread exiting...");
            hasExited.complete(null);
        });
        t.setName("assignment-tailer-thread");
        return t;
    }
}
