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

import org.apache.pulsar.functions.runtime.worker.FunctionStateManager;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.URI;

public class WorkerServer implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(WorkerServer.class);

    private WorkerConfig workerConfig;
    private FunctionStateManager functionStateManager;


    public WorkerServer(WorkerConfig workerConfig, FunctionStateManager functionStateManager) {
        this.workerConfig = workerConfig;
        this.functionStateManager = functionStateManager;
    }

    private static String getErrorMessage(Server server, int port, Exception ex) {
        if (ex instanceof BindException) {
            final URI uri = server.getURI();
            return String.format("%s http://%s:%d", ex.getMessage(), uri.getHost(), port);
        }

        return ex.getMessage();
    }

    @Override
    public void run() {
        final Server server = new Server(this.workerConfig.getWorkerPort());

        final ResourceConfig config = new ResourceConfig(Resources.get());

        final ServletContextHandler contextHandler =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

        contextHandler.setAttribute(BaseApiResource.ATTRIBUTE_WORKER_CONFIG, this.workerConfig);
        contextHandler.setAttribute(BaseApiResource.ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER, this.functionStateManager);
        contextHandler.setContextPath("/");

        server.setHandler(contextHandler);

        final ServletHolder apiServlet =
                new ServletHolder(new ServletContainer(config));

        contextHandler.addServlet(apiServlet, "/*");
        try {
            server.start();

            LOG.info("Worker Server started at {}", server.getURI());

            server.join();
        } catch (Exception ex) {
            final String message = getErrorMessage(server, this.workerConfig.getWorkerPort(), ex);
            LOG.error(message);
            System.exit(1);
        } finally {
            server.destroy();
        }
    }

    public String getThreadName() {
        return "worker-server-thread-" + this.workerConfig.getWorkerId();
    }
}
