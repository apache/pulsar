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

import com.google.common.util.concurrent.AbstractService;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.rest.WorkerServer;

@Slf4j
public class Worker extends AbstractService {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;
    private Thread serverThread;

    public Worker(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.workerService = new WorkerService(workerConfig);
    }

    @Override
    protected void doStart() {
        try {
            doStartImpl();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("Interrupted at starting worker", ie);
        } catch (Throwable t) {
            log.error("Failed to start worker", t);
        }
    }

    protected void doStartImpl() throws InterruptedException {
        workerService.start();
        WorkerServer server = new WorkerServer(workerService);
        this.serverThread = new Thread(server, server.getThreadName());

        log.info("Start worker server on port {}...", this.workerConfig.getWorkerPort());
        this.serverThread.start();
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
        workerService.stop();
    }
}
