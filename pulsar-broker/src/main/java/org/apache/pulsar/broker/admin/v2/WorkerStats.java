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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Workers;

@Path("/worker-stats")
public class WorkerStats extends AdminResource {

    public Workers<? extends WorkerService> workers() {
        return validateAndGetWorkerService().getWorkers();
    }

    @GET
    @Path("/metrics")
    @Operation(
            summary = "Gets the metrics for Monitoring",
            description = "The request should be executed by a Monitoring agent on each worker "
                    + "to fetch the worker-metrics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Gets the metrics for Monitoring",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = org.apache.pulsar.common.stats.Metrics.class)))),
            @ApiResponse(responseCode = "401", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Collection<Metrics> getMetrics() throws Exception {
        return workers().getWorkerMetrics(authParams());
    }

    @GET
    @Path("/functionsmetrics")
    @Operation(
            summary = "Get metrics for all functions owned by worker",
            description = "The request should be executed by a Monitoring agent on each worker to fetch the metrics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get metrics for all functions owned by worker",
                    content = @Content(array = @ArraySchema(
                            schema = @Schema(implementation = WorkerFunctionInstanceStats.class)))),
            @ApiResponse(responseCode = "401", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "503", description = "Worker service is not running")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkerFunctionInstanceStats> getStats() throws IOException {
        return workers().getFunctionsMetrics(authParams());
    }
}
