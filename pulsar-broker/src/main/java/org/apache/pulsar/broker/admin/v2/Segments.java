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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.CustomLog;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Admin REST API for segment topic operations.
 *
 * <p>These endpoints route to the broker owning the segment's namespace bundle
 * via {@code validateTopicOwnershipAsync}.
 */
@CustomLog
@Path("/segments")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/segments", description = "Segment topic admin APIs", tags = "segments")
public class Segments extends AdminResource {

    private TopicName segmentTopicName(String tenant, String namespace,
                                       String encodedTopic, String descriptor) {
        return TopicName.get(TopicDomain.segment.value() + "://" + tenant + "/" + namespace + "/"
                + encodedTopic + "/" + descriptor);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}")
    @ApiOperation(value = "Create a segment topic on the owning broker.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Segment topic created successfully"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void createSegment(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @ApiParam(value = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Subscriptions to create on the new segment")
            List<String> subscriptions) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateTopicOwnershipAsync(segmentTopic, authoritative)
                .thenCompose(__ -> pulsar().getBrokerService().getOrCreateTopic(segmentTopic.toString()))
                .thenCompose(topic -> {
                    log.info().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .log("Created segment topic");
                    if (subscriptions == null || subscriptions.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // Create subscriptions at earliest position
                    List<CompletableFuture<Void>> futures = new java.util.ArrayList<>();
                    for (String sub : subscriptions) {
                        futures.add(topic.createSubscription(sub,
                                CommandSubscribe.InitialPosition.Earliest,
                                false, null)
                                .thenAccept(__ -> {}));
                    }
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
                })
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .exception(ex).log("Failed to create segment topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/terminate")
    @ApiOperation(value = "Terminate a segment topic so no more messages can be published.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Segment topic terminated successfully"),
            @ApiResponse(code = 404, message = "Segment topic not found"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void terminateSegment(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @ApiParam(value = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateTopicOwnershipAsync(segmentTopic, authoritative)
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Segment topic not found: " + segmentTopic);
                    }
                    if (optTopic.get() instanceof PersistentTopic pt) {
                        return pt.terminate().thenApply(__ -> null);
                    }
                    throw new RestException(Response.Status.BAD_REQUEST,
                            "Cannot terminate non-persistent topic: " + segmentTopic);
                })
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .log("Terminated segment topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .exception(ex).log("Failed to terminate segment topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}")
    @ApiOperation(value = "Delete a segment topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Segment topic deleted successfully"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void deleteSegment(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @ApiParam(value = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Force deletion")
            @QueryParam("force") @DefaultValue("false") boolean force) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateTopicOwnershipAsync(segmentTopic, authoritative)
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return optTopic.get().delete().thenApply(__ -> null);
                })
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .log("Deleted segment topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .exception(ex).log("Failed to delete segment topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }
}
