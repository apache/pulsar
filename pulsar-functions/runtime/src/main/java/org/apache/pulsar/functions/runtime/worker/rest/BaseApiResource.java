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

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.functions.runtime.worker.FunctionActioner;
import org.apache.pulsar.functions.runtime.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

public class BaseApiResource {

    public static final String ATTRIBUTE_WORKER_CONFIG = "config";
    public static final String ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER = "function-state-manager";
    public static final String ATTRIBUTE_WORKER_DLOG_NAMESPACE = "distributedlog-namespace";
    public static final String ATTRIBUTE_FUNCTION_ACTIONER = "function-actioner";

    private WorkerConfig workerConfig;
    private FunctionMetaDataManager functionMetaDataManager;
    private Namespace dlogNamespace;
    private FunctionActioner functionActioner;

    @Context
    protected ServletContext servletContext;

    public WorkerConfig getWorkerConfig() {
        if (this.workerConfig == null) {
            this.workerConfig = (WorkerConfig) servletContext.getAttribute(ATTRIBUTE_WORKER_CONFIG);
        }
        return this.workerConfig;
    }

    public FunctionMetaDataManager getWorkerFunctionStateManager() {
        if (this.functionMetaDataManager == null) {
            this.functionMetaDataManager = (FunctionMetaDataManager) servletContext.getAttribute(ATTRIBUTE_WORKER_FUNCTION_STATE_MANAGER);
        }
        return this.functionMetaDataManager;
    }

    public Namespace getDlogNamespace() {
        if (this.dlogNamespace == null) {
            this.dlogNamespace = (Namespace) servletContext.getAttribute(ATTRIBUTE_WORKER_DLOG_NAMESPACE);
        }
        return this.dlogNamespace;
    }

    public FunctionActioner getFunctionActioner() {
        if (this.functionActioner == null) {
            this.functionActioner = (FunctionActioner) servletContext.getAttribute(ATTRIBUTE_FUNCTION_ACTIONER);
        }
        return this.functionActioner;
    }

}
