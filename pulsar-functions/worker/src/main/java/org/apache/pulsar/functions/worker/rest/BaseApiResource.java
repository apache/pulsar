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

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerConfig;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class BaseApiResource {

    public static final String ATTRIBUTE_WORKER_CONFIG = "config";
    public static final String ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER = "function-state-manager";
    public static final String ATTRIBUTE_WORKER_DLOG_NAMESPACE = "distributedlog-namespace";

    private WorkerConfig workerConfig;
    private FunctionRuntimeManager functionRuntimeManager;
    private Namespace dlogNamespace;

    @Context
    protected ServletContext servletContext;

    public WorkerConfig getWorkerConfig() {
        if (this.workerConfig == null) {
            this.workerConfig = (WorkerConfig) servletContext.getAttribute(ATTRIBUTE_WORKER_CONFIG);
        }
        return this.workerConfig;
    }

    public FunctionRuntimeManager getWorkerFunctionStateManager() {
        if (this.functionRuntimeManager == null) {
            this.functionRuntimeManager = (FunctionRuntimeManager) servletContext.getAttribute(ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER);
        }
        return this.functionRuntimeManager;
    }

    public Namespace getDlogNamespace() {
        if (this.dlogNamespace == null) {
            this.dlogNamespace = (Namespace) servletContext.getAttribute(ATTRIBUTE_WORKER_DLOG_NAMESPACE);
        }
        return this.dlogNamespace;
    }
}
