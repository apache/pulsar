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
package org.apache.pulsar.functions.worker.rest.api.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.functions.worker.WorkerService;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.service.api.Workers;

@Slf4j
@Path("/worker-stats")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/worker-stats", description = "Workers stats api", tags = "workers-stats")
public class WorkerStatsApiV2Resource implements Supplier<WorkerService> {

    public static final String ATTRIBUTE_WORKERSTATS_SERVICE = "worker-stats";

    private WorkerService workerService;
    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;

    @Override
    public synchronized WorkerService get() {
        if (this.workerService == null) {
            this.workerService = (WorkerService) servletContext.getAttribute(ATTRIBUTE_WORKERSTATS_SERVICE);
        }
        return this.workerService;
    }

    Workers<? extends WorkerService> workers() {
        return get().getWorkers();
    }

    AuthenticationParameters authParams() {
        return AuthenticationParameters.builder()
                .clientRole(clientAppId())
                .originalPrincipal(httpRequest.getHeader(FunctionApiResource.ORIGINAL_PRINCIPAL_HEADER))
                .clientAuthenticationDataSource((AuthenticationDataSource)
                        httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName))
                .build();
    }

    /**
     * @deprecated use {@link AuthenticationParameters} instead
     */
    @Deprecated
    public String clientAppId() {
        return httpRequest != null
                ? (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName)
                : null;
    }

    @GET
    @Path("/metrics")
    @ApiOperation(
            value = "Gets the metrics for Monitoring",
            notes = "Request should be executed by Monitoring agent on each worker to fetch the worker-metrics",
            response = org.apache.pulsar.common.stats.Metrics.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<org.apache.pulsar.common.stats.Metrics> getMetrics() throws Exception {
        return workers().getWorkerMetrics(authParams());
    }

    @GET
    @Path("/functionsmetrics")
    @ApiOperation(
            value = "Get metrics for all functions owned by worker",
            notes = "Requested should be executed by Monitoring agent on each worker to fetch the metrics",
            response = WorkerFunctionInstanceStats.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkerFunctionInstanceStats> getStats() throws IOException {
        return workers().getFunctionsMetrics(authParams());
    }
}
