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

import java.util.function.Supplier;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.worker.WorkerService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.rest.api.WorkerImpl;

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
            value = "Fetches information about the Pulsar cluster running Pulsar Functions"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions")

    })
    @Path("/cluster")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCluster() {
        return worker.getCluster();
    }

    @GET
    @ApiOperation(
            value = "Fetches info about the leader node of the Pulsar cluster running Pulsar Functions")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions")

    })
    @Path("/cluster/leader")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterLeader() {
        return worker.getClusterLeader();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about which Pulsar Functions are assigned to which Pulsar clusters",
            response = Function.Assignment.class,
            responseContainer = "Map"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions")
    })
    @Path("/assignments")
    public Response getAssignments() {
        return worker.getAssignments();
    }
}
