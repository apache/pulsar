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
package org.apache.pulsar.broker.admin.v2;

import com.sun.org.apache.xpath.internal.operations.Bool;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@Path("/worker")
public class Worker extends AdminResource implements Supplier<WorkerService> {

    private final WorkerImpl worker;

    public Worker() {
        this.worker = new WorkerImpl(this);
    }

    @Override
    public WorkerService get() {
        return pulsar().getWorkerService();
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
        return worker.getCluster(clientAppId());
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
        return worker.getClusterLeader(clientAppId());
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
        return worker.getAssignments(clientAppId());
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
    @Produces(MediaType.APPLICATION_JSON)
    public List<ConnectorDefinition> getConnectorsList() throws IOException {
        return worker.getListOfConnectors(clientAppId());
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
        worker.rebalance(uri.getRequestUri(), clientAppId());
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
        return worker.isLeaderReady(clientAppId());
    }
}
