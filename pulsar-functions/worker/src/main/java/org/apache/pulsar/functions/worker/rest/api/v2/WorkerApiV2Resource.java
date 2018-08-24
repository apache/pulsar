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

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication.Metrics;
import org.apache.pulsar.functions.worker.WorkerService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;

@Slf4j
@Path("/worker")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/worker", description = "Workers admin api", tags = "workers")
public class WorkerApiV2Resource implements Supplier<WorkerService> {

    public static final String ATTRIBUTE_WORKER_SERVICE = "worker";

    protected final WorkerImpl worker;
    private WorkerService workerService;
    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;

    public WorkerApiV2Resource() {
        this.worker = new WorkerImpl(this);
    }

    @Override
    public synchronized WorkerService get() {
        if (this.workerService == null) {
            this.workerService = (WorkerService) servletContext.getAttribute(ATTRIBUTE_WORKER_SERVICE);
        }
        return this.workerService;
    }

    public String clientAppId() {
        return httpRequest != null
                ? (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName)
                : null;
    }

    @GET
    @Path("/cluster")
    @ApiOperation(value = "Fetches information about the Pulsar cluster running Pulsar Functions")
    @ApiResponses(value = { @ApiResponse(code = 401, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "WorkerApiV2Resource service is not running") })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCluster() {
        log.info("SANJEEV CAME2");
        return worker.getCluster();
    }

    @GET
    @Path("/cluster/leader")
    @ApiOperation(value = "Fetches info about the leader node of the Pulsar cluster running Pulsar Functions")
    @ApiResponses(value = { @ApiResponse(code = 401, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "WorkerApiV2Resource service is not running") })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterLeader() {
        return worker.getClusterLeader();
    }

    @GET
    @Path("/assignments")
    @ApiOperation(value = "Fetches information about which Pulsar Functions are assigned to which Pulsar clusters",
            response = Function.Assignment.class,
            responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 401, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "WorkerApiV2Resource service is not running") })
    public Response getAssignments() {
        return worker.getAssignments();
    }
    
    @GET
    @Path("/metrics")
    @ApiOperation(value = "Gets the metrics for Monitoring", notes = "Request should be executed by Monitoring agent on each worker to fetch the worker-metrics", response = org.apache.pulsar.common.stats.Metrics.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 401, message = "Don't have admin permission") })
    public Collection<org.apache.pulsar.common.stats.Metrics> getMetrics() throws Exception {
        return worker.getWorkerMetrcis(clientAppId());
    }

    @GET
    @Path("/functionsmetrics")
    @ApiOperation(value = "Get metrics for all functions owned by worker", notes = "Requested should be executed by Monitoring agent on each worker to fetch the metrics", response = Metrics.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 503, message = "Worker service is not running") })
    public Response getFunctionsMetrics() throws IOException {
        return worker.getFunctionsMetrics(clientAppId());
    }
}
