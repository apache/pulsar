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
package org.apache.pulsar.websocket.admin.v2;

import static org.apache.pulsar.common.util.Codec.decode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Map;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.websocket.admin.WebSocketProxyStatsBase;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;

@Path("/proxy-stats")
@Tag(name = "proxy-stats", description = "Stats for web-socket proxy")
@Produces(MediaType.APPLICATION_JSON)
public class WebSocketProxyStatsV2 extends WebSocketProxyStatsBase {
    @GET
    @Path("/metrics")
    @Operation(summary = "Gets the metrics for Monitoring",
                  description = "The request should be executed by the Monitoring agent on each proxy "
                          + "to fetch the metrics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Gets the metrics for Monitoring",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = Metrics.class)))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public Collection<Metrics> internalGetMetrics() throws Exception {
        return super.internalGetMetrics();
    }

    @GET
    @Path("/{domain}/{tenant}/{namespace}/{topic}/stats")
    @Operation(summary = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Topic does not exist") })
    public ProxyTopicStat getStats(@PathParam("domain") String domain, @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic) {
        return super.internalGetStats(TopicName.get(domain, tenant, namespace, decode(encodedTopic)));
    }

    @GET
    @Path("/stats")
    @Operation(summary = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public Map<String, ProxyTopicStat> internalGetProxyStats() {
        return super.internalGetProxyStats();
    }
}
