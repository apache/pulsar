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

import java.util.Optional;
import java.util.function.Supplier;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;

public class FunctionApiResource implements Supplier<WorkerService> {

    public static final String ATTRIBUTE_FUNCTION_WORKER = "function-worker";

    protected final FunctionsImpl functions;
    private WorkerService workerService;
    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;

    public FunctionApiResource() {
        this.functions = new FunctionsImpl(this);
    }

    @Override
    public synchronized WorkerService get() {
        if (this.workerService == null) {
            this.workerService = (WorkerService) servletContext.getAttribute(ATTRIBUTE_FUNCTION_WORKER);
        }
        return this.workerService;
    }

    public String clientAppId() {
        return httpRequest != null
                ? (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName)
                : null;
    }
}
