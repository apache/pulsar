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
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import org.apache.pulsar.functions.worker.service.api.Workers;

@Slf4j
@Path("/worker")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(value = "/worker", description = "Workers admin api", tags = "workers")
public class WorkerApiV2Resource implements Supplier<WorkerService> {

    public static final String ATTRIBUTE_WORKER_SERVICE = "worker";

    private WorkerService workerService;
    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;
    @Context
    protected UriInfo uri;

    @Override
    public synchronized WorkerService get() {
        if (this.workerService == null) {
            this.workerService = (WorkerService) servletContext.getAttribute(ATTRIBUTE_WORKER_SERVICE);
        }
        return this.workerService;
    }

    Workers<? extends WorkerService> workers() {
        return get().getWorkers();
    }

    public String clientAppId() {
        return httpRequest != null
                ? (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName)
                : null;
    }

    @GET
    @ApiOperation(
            value = "Fetches information about the Pulsar cluster running Pulsar Functions",
            response = WorkerInfo.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Path("/cluster")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkerInfo> getCluster() {
        return workers().getCluster(clientAppId());
    }

    @GET
    @ApiOperation(
            value = "Fetches info about the leader node of the Pulsar cluster running Pulsar Functions",
            response = WorkerInfo.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Path("/cluster/leader")
    @Produces(MediaType.APPLICATION_JSON)
    public WorkerInfo getClusterLeader() {
        return workers().getClusterLeader(clientAppId());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about which Pulsar Functions are assigned to which Pulsar clusters",
            response = Map.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Path("/assignments")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Collection<String>> getAssignments() {
        return workers().getAssignments(clientAppId());
    }

    @GET
    @ApiOperation(
            value = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode",
            response = List.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout")
    })
    @Path("/connectors")
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return workers().getListOfConnectors(clientAppId());
    }

    @PUT
    @ApiOperation(
            value = "Triggers a rebalance of functions to workers"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout")
    })
    @Path("/rebalance")
    public void rebalance() {
        workers().rebalance(uri.getRequestUri(), clientAppId());
    }

    @PUT
    @ApiOperation(
            value = "Drains the specified worker, i.e., moves its work-assignments to other workers"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 409, message = "Drain already in progress"),
            @ApiResponse(code = 503, message = "Worker service is not ready")
    })
    @Path("/leader/drain")
    public void drainAtLeader(@QueryParam("workerId") String workerId) {
        workers().drain(uri.getRequestUri(), workerId, clientAppId(), true);
    }

    @PUT
    @ApiOperation(
            value = "Drains this worker, i.e., moves its work-assignments to other workers"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 409, message = "Drain already in progress"),
            @ApiResponse(code = 503, message = "Worker service is not ready")
    })
    @Path("/drain")
    public void drain() {
        workers().drain(uri.getRequestUri(), null, clientAppId(), false);
    }

    @GET
    @ApiOperation(
            value = "Get the status of the drain operation for the specified worker",
            response = LongRunningProcessStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 503, message = "Worker service is not ready")
    })
    @Path("/leader/drain")
    public LongRunningProcessStatus getDrainStatus(@QueryParam("workerId") String workerId) {
        return workers().getDrainStatus(uri.getRequestUri(), workerId, clientAppId(), true);
    }

    @GET
    @ApiOperation(
            value = "Get the status of the drain operation of this worker",
            response = LongRunningProcessStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 503, message = "Worker service is not ready")
    })
    @Path("/drain")
    public LongRunningProcessStatus getDrainStatus() {
        return workers().getDrainStatus(uri.getRequestUri(), null, clientAppId(), false);
    }

    @GET
    @ApiOperation(
            value = "Checks if this node is the leader and is ready to service requests",
            response = Boolean.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 503, message = "Worker service is not running")
    })
    @Path("/cluster/leader/ready")
    public Boolean isLeaderReady() {
        return workers().isLeaderReady(clientAppId());
    }
}
