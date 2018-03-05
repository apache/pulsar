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
package org.apache.pulsar.functions.worker.rest;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;

import java.net.BindException;
import java.net.URI;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;

@Slf4j
public class WorkerServer implements Runnable {

    private final WorkerConfig workerConfig;
    private final WorkerService workerService;

    private static String getErrorMessage(Server server, int port, Exception ex) {
        if (ex instanceof BindException) {
            final URI uri = server.getURI();
            return String.format("%s http://%s:%d", ex.getMessage(), uri.getHost(), port);
        }

        return ex.getMessage();
    }

    public WorkerServer(WorkerService workerService) {
        this.workerConfig = workerService.getWorkerConfig();
        this.workerService = workerService;
    }

    @Override
    public void run() {
        final Server server = new Server(this.workerConfig.getWorkerPort());

        List<Handler> handlers = new ArrayList<>(2);
        handlers.add(WorkerService.newServletContextHandler("/admin", workerService));
        handlers.add(WorkerService.newServletContextHandler("/admin/v2", workerService));
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));
        HandlerCollection handlerCollection = new HandlerCollection();
        handlerCollection.setHandlers(new Handler[] {
            contexts, new DefaultHandler()
        });
        server.setHandler(handlerCollection);

        try {
            server.start();

            log.info("Worker Server started at {}", server.getURI());

            server.join();
        } catch (Exception ex) {
            log.error("ex: {}", ex, ex);
            final String message = getErrorMessage(server, this.workerConfig.getWorkerPort(), ex);
            log.error(message);
            System.exit(1);
        } finally {
            server.destroy();
        }
    }

    public String getThreadName() {
        return "worker-server-thread-" + this.workerConfig.getWorkerId();
    }
}
