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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Request.ServiceRequest;

@Slf4j
public class FunctionMetaDataTopicTailer
        implements Runnable, AutoCloseable {

    private final FunctionMetaDataManager functionMetaDataManager;
    @Getter
    private final Reader<byte[]> reader;
    private final Thread readerThread;
    private volatile boolean running;
    private ErrorNotifier errorNotifier;

    public FunctionMetaDataTopicTailer(FunctionMetaDataManager functionMetaDataManager,
                                       ReaderBuilder readerBuilder, WorkerConfig workerConfig,
                                       ErrorNotifier errorNotifier)
            throws PulsarClientException {
        this.functionMetaDataManager = functionMetaDataManager;
        this.reader = readerBuilder
                .topic(workerConfig.getFunctionMetadataTopic())
                .startMessageId(MessageId.earliest)
                .readerName(workerConfig.getWorkerId() + "-function-metadata-manager")
                .subscriptionRolePrefix(workerConfig.getWorkerId() + "-function-metadata-manager")
                .create();
        readerThread = new Thread(this);
        readerThread.setName("function-metadata-tailer-thread");
        this.errorNotifier = errorNotifier;
    }

    public void start() {
        running = true;
        readerThread.start();
    }

    @Override
    public void run() {
        while(running) {
            try {
                Message<byte[]> msg = reader.readNext();
                processRequest(msg);
            } catch (Throwable th) {
                if (running) {
                    log.error("Encountered error in metadata tailer", th);
                    // trigger fatal error
                    running = false;
                    errorNotifier.triggerError(th);
                } else {
                    if (!(th instanceof InterruptedException || th.getCause() instanceof InterruptedException)) {
                        log.warn("Encountered error when metadata tailer is not running", th);
                    }
                    return;
                }
            }
        }
    }

    @Override
    public void close() {
        log.info("Stopping function metadata tailer");
        try {
            running = false;
            if (readerThread != null && readerThread.isAlive()) {
                readerThread.interrupt();
            }
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            log.error("Failed to stop function metadata tailer", e);
        }
        log.info("Stopped function metadata tailer");
    }

    public void processRequest(Message<byte[]> msg) throws IOException {
        ServiceRequest serviceRequest = ServiceRequest.parseFrom(msg.getData());
        if (log.isDebugEnabled()) {
            log.debug("Received Service Request: {}", serviceRequest);
        }
        this.functionMetaDataManager.processRequest(msg.getMessageId(), serviceRequest);
    }
}
