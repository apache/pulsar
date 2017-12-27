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
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class BaseApiResource {

    public static final String ATTRIBUTE_WORKER_CONFIG = "config";
    public static final String ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER = "function-state-manager";
    public static final String ATTRIBUTE_WORKER_SERVICE_REQUEST_MANAGER = "worker-service-request-manager";

    private WorkerConfig workerConfig;
    private FunctionStateManager functionStateManager;
    private ServiceRequestManager serviceRequestManager;

    @Context
    protected ServletContext servletContext;

    public WorkerConfig getWorkerConfig() {
        if (this.workerConfig == null) {
            this.workerConfig = (WorkerConfig) servletContext.getAttribute(ATTRIBUTE_WORKER_CONFIG);
        }
        return this.workerConfig;
    }

    public FunctionStateManager getWorkerFunctionStateManager() {
        if (this.functionStateManager == null) {
            this.functionStateManager = (FunctionStateManager) servletContext.getAttribute(ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER);
        }
        return this.functionStateManager;
    }

    public ServiceRequestManager getWorkerServiceRequestManager() {
        if (this.serviceRequestManager == null) {
            this.serviceRequestManager = (ServiceRequestManager) servletContext.getAttribute(ATTRIBUTE_WORKER_SERVICE_REQUEST_MANAGER);
        }
        return this.serviceRequestManager;
    }
}
