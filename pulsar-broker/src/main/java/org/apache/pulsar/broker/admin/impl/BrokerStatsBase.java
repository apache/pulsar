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
package org.apache.pulsar.broker.admin.impl;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.StreamingOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.broker.stats.AllocatorStatsGenerator;
import org.apache.pulsar.broker.stats.BookieClientStatsGenerator;
import org.apache.pulsar.broker.stats.MBeanStatsGenerator;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;

public class BrokerStatsBase extends AdminResource {
    @GET
    @Path("/metrics")
    @Operation(summary = "Gets the metrics for Monitoring",
            description = "The request should be executed by the Monitoring agent on each broker to fetch the metrics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Gets the metrics for Monitoring",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = Metrics.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public Collection<Metrics> getMetrics() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            Collection<Metrics> metrics = pulsar().getMetricsGenerator().generate();
            return metrics;
        } catch (Exception e) {
            log.error().exception(e).log("Failed to generate metrics");
            throw new RestException(e);
        }
    }

    @GET
    @Path("/mbeans")
    @Operation(summary = "Get all the mbean details of this broker JVM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get all the mbean details of this broker JVM",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = Metrics.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public Collection<Metrics> getMBeans() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            Collection<Metrics> metrics = MBeanStatsGenerator.generate(pulsar());
            return metrics;
        } catch (Exception e) {
            log.error().exception(e).log("Failed to generate mbean stats");
            throw new RestException(e);
        }
    }

    @GET
    @Path("/destinations")
    @Operation(summary = "Get all the topic stats by namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get all the topic stats by namespace",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(type = "object", description = "Nested JSON object:"
                                    + " namespace -> bundle range -> persistent/non-persistent -> topic -> stats"))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public StreamingOutput getTopics2() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        return output -> pulsar().getBrokerService().getDimensionMetrics(statsBuf -> {
            try {
                output.write(statsBuf.array(), statsBuf.arrayOffset(), statsBuf.readableBytes());
            } catch (Exception e) {
                throw new WebApplicationException(e);
            }
        });
    }

    @GET
    @Path("/allocator-stats/{allocator}")
    @Operation(summary = "Get the stats for the Netty allocator. Available allocators are 'default' and 'ml-cache'")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the stats for the Netty allocator. Available allocators are 'default' "
                            + "and 'ml-cache'",
                    content = @Content(schema = @Schema(implementation = AllocatorStats.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public AllocatorStats getAllocatorStats(@PathParam("allocator") String allocatorName) throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();

        try {
            return AllocatorStatsGenerator.generate(allocatorName);
        } catch (IllegalArgumentException e) {
            throw new RestException(Status.NOT_ACCEPTABLE, e.getMessage());
        } catch (Exception e) {
            log.error().exception(e).log("Failed to generate allocator stats");
            throw new RestException(e);
        }
    }

    @GET
    @Path("/bookieops")
    @Operation(summary = "Get pending bookie client op stats by namespace",
            description = "Returns a nested map structure: Map<String, Map<String, PendingBookieOpsStats>>.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get pending bookie client op stats by namespace",
                    content = @Content(schema = @Schema(type = "object"),
                            additionalPropertiesSchema =
                            @Schema(additionalPropertiesSchema = PendingBookieOpsStats.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public Map<String, Map<String, PendingBookieOpsStats>> getPendingBookieOpsStats() {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            return BookieClientStatsGenerator.generate(pulsar());
        } catch (Exception e) {
            log.error()
                    .exception(e)
                    .log("Failed to generate pending bookie ops stats for topics");
            throw new RestException(e);
        }
    }

    @GET
    @Path("/load-report")
    @Operation(summary = "Get Load for this broker", description = "consists of topics stats & systemResourceUsage")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get Load for this broker",
                    content = @Content(schema = @Schema(implementation = LoadReport.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public LoadManagerReport getLoadReport() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            return (pulsar().getLoadManager().get()).generateLoadReport();
        } catch (Exception e) {
            log.error()
                    .exception(e)
                    .log("Failed to generate LoadReport for broker");
            throw new RestException(e);
        }
    }

    protected Map<Long, Collection<ResourceUnit>> internalBrokerResourceAvailability(NamespaceName namespace) {
        try {
            validateSuperUserAccess();
            LoadManager lm = pulsar().getLoadManager().get();
            if (lm instanceof SimpleLoadManagerImpl) {
                return ((SimpleLoadManagerImpl) lm).getResourceAvailabilityFor(namespace).asMap();
            } else {
                throw new RestException(Status.CONFLICT, lm.getClass().getName() + " does not support this operation");
            }
        } catch (Exception e) {
            log.error().exception(e).log("Unable to get Resource Availability");
            throw new RestException(e);
        }
    }
}
