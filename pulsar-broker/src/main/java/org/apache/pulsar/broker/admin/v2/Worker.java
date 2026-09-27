/*
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
package org.apache.pulsar.broker.admin.v2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Workers;

@Path("/worker")
public class Worker extends AdminResource implements Supplier<WorkerService> {

    Workers<? extends WorkerService> workers() {
        return validateAndGetWorkerService().getWorkers();
    }

    @Override
    public WorkerService get() {
        return validateAndGetWorkerService();
    }

    @GET
    @Operation(
            summary = "Fetches information about the Pulsar cluster running Pulsar Functions"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about the Pulsar cluster running Pulsar Functions",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = WorkerInfo.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Path("/cluster")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkerInfo> getCluster() {
        return workers().getCluster(authParams());
    }

    @GET
    @Operation(
            summary = "Fetches info about the leader node of the Pulsar cluster running Pulsar Functions"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches info about the leader node of the Pulsar cluster running Pulsar Functions",
                    content = @Content(schema = @Schema(implementation = WorkerInfo.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Path("/cluster/leader")
    @Produces(MediaType.APPLICATION_JSON)
    public WorkerInfo getClusterLeader() {
        return workers().getClusterLeader(authParams());
    }

    @GET
    @Operation(
            summary = "Fetches information about which Pulsar Functions are assigned to which Pulsar clusters",
            description = "Returns a map structure: Map<String, Set<String>>."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches information about which Pulsar Functions are assigned to which Pulsar "
                            + "clusters",
                    content = @Content(schema = @Schema(type = "object"),
                            additionalPropertiesArraySchema = @ArraySchema(
                            schema = @Schema(implementation = String.class), uniqueItems = true))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Path("/assignments")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Collection<String>> getAssignments() {
        return workers().getAssignments(authParams());
    }

    @GET
    @Operation(
            summary = "Fetches a list of supported Pulsar IO connectors currently running in cluster mode"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Fetches a list of supported Pulsar IO connectors currently running in cluster "
                            + "mode",
                    content = @Content(array = @ArraySchema(schema =
                            @Schema(implementation = ConnectorDefinition.class)))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Path("/connectors")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return workers().getListOfConnectors(authParams());
    }

    @PUT
    @Operation(
            summary = "Triggers a rebalance of functions to workers"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "408", description = "Request timeout")
    })
    @Path("/rebalance")
    public void rebalance() {
        workers().rebalance(uri.getRequestUri(), authParams());
    }

    @PUT
    @Operation(
            summary = "Drains the specified worker, i.e., moves its work-assignments to other workers"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "409", description = "Drain already in progress"),
            @ApiResponse(responseCode = "503", description = "Worker service is not ready")
    })
    @Path("/leader/drain")
    public void drainAtLeader(@QueryParam("workerId") String workerId) {
        workers().drain(uri.getRequestUri(), workerId, authParams(), true);
    }

    @PUT
    @Operation(
            summary = "Drains this worker, i.e., moves its work-assignments to other workers"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "400", description = "Invalid request"),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "408", description = "Request timeout"),
            @ApiResponse(responseCode = "409", description = "Drain already in progress"),
            @ApiResponse(responseCode = "503", description = "Worker service is not ready")
    })
    @Path("/drain")
    public void drain() {
        workers().drain(uri.getRequestUri(), null, authParams(), false);
    }

    @GET
    @Operation(
            summary = "Get the status of any ongoing drain operation at the specified worker"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the status of any ongoing drain operation at the specified worker",
                    content = @Content(schema = @Schema(implementation = LongRunningProcessStatus.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "503", description = "Worker service is not ready")
    })
    @Path("/leader/drain")
    public LongRunningProcessStatus getDrainStatusFromLeader(@QueryParam("workerId") String workerId) {
        return workers().getDrainStatus(uri.getRequestUri(), workerId, authParams(), true);
    }

    @GET
    @Operation(
            summary = "Get the status of any ongoing drain operation at this worker"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the status of any ongoing drain operation at this worker",
                    content = @Content(schema = @Schema(implementation = LongRunningProcessStatus.class))),
            @ApiResponse(responseCode = "403", description = "The requester doesn't have admin permissions"),
            @ApiResponse(responseCode = "503", description = "Worker service is not ready")
    })
    @Path("/drain")
    public LongRunningProcessStatus getDrainStatus() {
        return workers().getDrainStatus(uri.getRequestUri(), null, authParams(), false);
    }

    @GET
    @Operation(
            summary = "Checks if this node is the leader and is ready to service requests"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Checks if this node is the leader and is ready to service requests",
                    content = @Content(schema = @Schema(implementation = Boolean.class))),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Path("/cluster/leader/ready")
    public Boolean isLeaderReady() {
        return workers().isLeaderReady(authParams());
    }
}
