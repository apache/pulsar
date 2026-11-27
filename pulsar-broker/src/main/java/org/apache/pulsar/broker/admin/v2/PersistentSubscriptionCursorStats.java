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

import static org.apache.pulsar.client.admin.internal.TopicsImpl.CURSOR_STATS_LOCAL_ONLY_HEADER;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedSubscriptionCursorInternalStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Kept separate from {@link PersistentTopics} because the non-persistent resource inherits its endpoints.
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "persistent topic", description = "Persistent topic admin apis")
public class PersistentSubscriptionCursorStats extends PersistentTopicsBase {
    private static final String ERROR_RESPONSES = "The resource returns errors as JSON with a reason field. "
            + "Requests rejected before reaching the resource use the standard admin HTTP response "
            + "from the authentication provider, rate limiter, or readiness check.";

    @Override
    protected String domain() {
        return TopicDomain.persistent.value();
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/internalStats")
    @Operation(summary = "Get the internal cursor stats for a subscription on a physical persistent topic.",
            description = ERROR_RESPONSES)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The subscription's internal cursor stats",
                    content = @Content(schema =
                            @Schema(implementation = ManagedLedgerInternalStats.CursorStats.class))),
            @ApiResponse(responseCode = "307", description = "Redirect to the owning broker"),
            @ApiResponse(responseCode = "401", description = "Authentication or GET_STATS authorization failed"),
            @ApiResponse(responseCode = "404", description = "Namespace, topic or subscription not found",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "412", description = "Invalid name, topic kind or ownership precondition, "
                    + "or the storage cursor requires includeIndividualDeletedMessages=true",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "429", description = "Broker HTTP request rate limit exceeded"),
            @ApiResponse(responseCode = "500", description = "Unexpected snapshot failure",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "503", description = "Broker not ready, or topic metadata or ownership "
                    + "unavailable") })
    public void getSubscriptionCursorInternalStats(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") String encodedSubName,
            @Parameter(description = "For internal ownership routing")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Parameter(description = "Include the full individually deleted message ranges")
            @QueryParam("includeIndividualDeletedMessages") @DefaultValue("false") boolean includeRanges,
            @Parameter(hidden = true)
            @HeaderParam(CURSOR_STATS_LOCAL_ONLY_HEADER) @DefaultValue("false") boolean localOnly) {
        getSubscriptionCursorStatsResponse(asyncResponse, tenant, namespace, encodedTopic, encodedSubName,
                subName -> internalGetSubscriptionCursorInternalStats(authoritative, subName, includeRanges,
                        localOnly));
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/partitioned-internalStats")
    @Operation(summary = "Get per-partition internal cursor stats for a subscription on a partitioned base topic.",
            description = ERROR_RESPONSES)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Complete or partial per-partition cursor stats",
                    content = @Content(schema =
                            @Schema(implementation = PartitionedSubscriptionCursorInternalStats.class))),
            @ApiResponse(responseCode = "307", description = "Redirect for global namespace routing"),
            @ApiResponse(responseCode = "401", description = "Authentication or GET_STATS authorization failed"),
            @ApiResponse(responseCode = "404", description = "Partition metadata or all partition cursors not found",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "412", description = "Invalid name, topic kind or routing precondition",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "429", description = "Broker HTTP request rate limit exceeded"),
            @ApiResponse(responseCode = "500", description = "Unexpected coordinator failure",
                    content = @Content(schema = @Schema(implementation = ErrorData.class))),
            @ApiResponse(responseCode = "503", description = "Broker not ready, metadata unavailable, "
                    + "or no partition cursor stats could be retrieved") })
    public void getPartitionedSubscriptionCursorInternalStats(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("subName") String encodedSubName,
            @Parameter(description = "Ignored for partitioned requests. Each physical partition routes to its owner.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Parameter(description = "Include the full individually deleted message ranges")
            @QueryParam("includeIndividualDeletedMessages") @DefaultValue("false") boolean includeRanges) {
        getSubscriptionCursorStatsResponse(asyncResponse, tenant, namespace, encodedTopic, encodedSubName,
                subName -> internalGetPartitionedSubscriptionCursorInternalStats(subName, includeRanges));
    }

    private <T> void getSubscriptionCursorStatsResponse(AsyncResponse asyncResponse, String tenant, String namespace,
                                                        String encodedTopic, String encodedSubName,
                                                        Function<String, CompletableFuture<T>> fetchStats) {
        FutureUtil.supplySafely(() -> {
            String subName;
            try {
                validateTopicName(tenant, namespace, encodedTopic);
                // Existing subscription routes apply form decoding after Jersey decodes the path segment.
                subName = Codec.decode(encodedSubName);
            } catch (IllegalArgumentException e) {
                throw new RestException(Response.Status.PRECONDITION_FAILED, "Topic or subscription name is not valid");
            }
            return fetchStats.apply(subName);
        }).whenComplete((stats, error) -> {
            if (error == null) {
                asyncResponse.resume(stats);
                return;
            }
            Throwable cause = FutureUtil.unwrapCompletionException(error);
            if (cause instanceof WebApplicationException webException
                    && webException.getResponse().getStatus() != 500) {
                asyncResponse.resume(webException);
            } else {
                log.error()
                        .attr("topic", topicName)
                        .attr("encodedSubscription", encodedSubName)
                        .exception(cause)
                        .log("Failed to get subscription cursor stats");
                asyncResponse.resume(new RestException(Response.Status.INTERNAL_SERVER_ERROR,
                        "Failed to get subscription cursor stats"));
            }
        });
    }
}
