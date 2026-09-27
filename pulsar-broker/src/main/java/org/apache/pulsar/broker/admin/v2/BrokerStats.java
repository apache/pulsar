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
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.pulsar.broker.admin.impl.BrokerStatsBase;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;

@Path("/broker-stats")
@Tag(name = "broker-stats", description = "Stats for broker")
@Produces(MediaType.APPLICATION_JSON)
@SuppressWarnings("deprecation")
public class BrokerStats extends BrokerStatsBase {

    @GET
    @Path("/topics")
    @Operation(
            summary = "Get all the topic stats by namespace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get all the topic stats by namespace",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(type = "object", description = "Nested JSON object:"
                                    + " namespace -> bundle range -> persistent/non-persistent -> topic -> stats"))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public StreamingOutput getTopics2() throws Exception {
        return super.getTopics2();
    }

    @GET
    @Path("/broker-resource-availability/{tenant}/{namespace}")
    @Operation(summary = "Broker availability report",
            description = "This API gives the current broker availability in "
            + "percent, each resource percentage usage is calculated and then "
            + "sum of all of the resource usage percent is called broker-resource-availability"
            + "<br/><br/>THIS API IS ONLY FOR USE BY TESTING FOR CONFIRMING NAMESPACE ALLOCATION ALGORITHM")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Returns broker resource availability as Map<Long, List<ResourceUnit>>."
                    + "Since `ResourceUnit` is an interface, its specific content is not determinable via class "
                    + "reflection. Refer to the source code or interface tests for detailed type definitions.",
            content = @Content(schema = @Schema(type = "object",
                    additionalPropertiesSchema = ResourceUnit.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "409", description = "Load-manager doesn't support operation") })
    public Map<Long, Collection<ResourceUnit>> getBrokerResourceAvailability(@PathParam("tenant") String tenant,
        @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalBrokerResourceAvailability(namespaceName);
    }
}
