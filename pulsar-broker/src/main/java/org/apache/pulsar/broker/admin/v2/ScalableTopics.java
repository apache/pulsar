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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import lombok.CustomLog;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Admin REST API for scalable topics (topic:// domain).
 *
 * <p>Provides operations to create, delete, list, and inspect scalable topics,
 * as well as segment split/merge operations.
 */
@CustomLog
@Path("/scalable")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/scalable", description = "Scalable topic admin APIs", tags = "scalable topic")
public class ScalableTopics extends AdminResource {

    private ScalableTopicResources resources() {
        return pulsar().getPulsarResources().getScalableTopicResources();
    }

    // --- List ---

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the list of scalable topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        resources().listScalableTopicsAsync(namespaceName)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("namespace", namespaceName)
                            .exception(ex).log("Failed to list scalable topics");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Create ---

    @PUT
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Create a new scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Scalable topic created successfully"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 409, message = "Scalable topic already exists"),
            @ApiResponse(code = 412, message = "Invalid configuration"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void createScalableTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Number of initial segments")
            @QueryParam("numInitialSegments") @DefaultValue("1") int numInitialSegments,
            @ApiParam(value = "Key value pair properties for the topic metadata")
            Map<String, String> properties) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        if (numInitialSegments < 1) {
            asyncResponse.resume(new RestException(Response.Status.fromStatusCode(412),
                    "numInitialSegments must be >= 1"));
            return;
        }

        Map<String, String> props = properties != null ? properties : Map.of();
        ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(
                numInitialSegments, props);

        // Segment persistent topics are auto-created on demand when clients connect,
        // so we only need to store the metadata here.
        resources().createScalableTopicAsync(tn, metadata)
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("topic", tn)
                            .attr("numInitialSegments", numInitialSegments)
                            .log("Created scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof MetadataStoreException.AlreadyExistsException) {
                        asyncResponse.resume(new RestException(Response.Status.CONFLICT,
                                "Scalable topic already exists: " + tn));
                    } else {
                        log.error().attr("clientAppId", clientAppId()).attr("topic", tn)
                                .exception(ex).log("Failed to create scalable topic");
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    // --- Get metadata ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Get scalable topic metadata.", response = ScalableTopicMetadata.class)
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Scalable topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getScalableTopicMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        resources().getScalableTopicMetadataAsync(tn)
                .thenAccept(optMd -> {
                    if (optMd.isEmpty()) {
                        asyncResponse.resume(new RestException(Response.Status.NOT_FOUND,
                                "Scalable topic not found: " + tn));
                    } else {
                        asyncResponse.resume(optMd.get());
                    }
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("topic", tn)
                            .exception(ex).log("Failed to get metadata for scalable topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Delete ---

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Delete a scalable topic and all its segments.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Scalable topic deleted successfully"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Scalable topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void deleteScalableTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Force deletion even if topic has active subscriptions")
            @QueryParam("force") @DefaultValue("false") boolean force) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        resources().getScalableTopicMetadataAsync(tn)
                .thenCompose(optMd -> {
                    if (optMd.isEmpty()) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Scalable topic not found: " + tn);
                    }
                    // Delete metadata first, then best-effort clean up segment topics
                    return resources().deleteScalableTopicAsync(tn)
                            .thenCompose(__ -> deleteSegmentTopics(tn, optMd.get(), force));
                })
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("topic", tn)
                            .log("Deleted scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("topic", tn)
                            .exception(ex).log("Failed to delete scalable topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Stats ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get aggregated stats for a scalable topic.",
            response = org.apache.pulsar.common.policies.data.ScalableTopicStats.class)
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Scalable topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getStats(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        var scalableTopicService = pulsar().getBrokerService().getScalableTopicService();
        if (scalableTopicService == null) {
            asyncResponse.resume(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
            return;
        }

        scalableTopicService.getStats(tn)
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("topic", tn)
                            .exception(ex).log("Failed to get stats for scalable topic");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Subscription operations ---

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/subscriptions/{subscription}")
    @ApiOperation(value = "Create a subscription on a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Subscription created successfully"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Scalable topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void createSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @ApiParam(value = "Subscription type: STREAM (controller-managed, ordered) "
                    + "or QUEUE (direct per-segment attach, no controller coordination)")
            @QueryParam("type") @DefaultValue("STREAM")
                    org.apache.pulsar.broker.resources.SubscriptionType type) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        var scalableTopicService = pulsar().getBrokerService().getScalableTopicService();
        if (scalableTopicService == null) {
            asyncResponse.resume(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
            return;
        }

        redirectToControllerLeaderIfNeeded(tn)
                .thenCompose(__ -> scalableTopicService.createSubscription(tn, subscription, type))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .log("Created subscription on scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .exception(ex).log("Failed to create subscription");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscriptions/{subscription}")
    @ApiOperation(value = "Delete a subscription from a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Subscription deleted successfully"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission on the namespace"),
            @ApiResponse(code = 404, message = "Scalable topic or subscription doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void deleteSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription name", required = true)
            @PathParam("subscription") String subscription) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        var scalableTopicService = pulsar().getBrokerService().getScalableTopicService();
        if (scalableTopicService == null) {
            asyncResponse.resume(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
            return;
        }

        redirectToControllerLeaderIfNeeded(tn)
                .thenCompose(__ -> scalableTopicService.deleteSubscription(tn, subscription))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .log("Deleted subscription from scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .exception(ex).log("Failed to delete subscription");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Segment operations ---

    @POST
    @Path("/{tenant}/{namespace}/{topic}/split/{segmentId}")
    @ApiOperation(value = "Split a segment into two halves.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Segment split successfully"),
            @ApiResponse(code = 404, message = "Scalable topic or segment doesn't exist"),
            @ApiResponse(code = 412, message = "Segment is not active or cannot be split"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void splitSegment(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Segment ID to split", required = true)
            @PathParam("segmentId") long segmentId) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        var scalableTopicService = pulsar().getBrokerService().getScalableTopicService();
        if (scalableTopicService == null) {
            asyncResponse.resume(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
            return;
        }

        redirectToControllerLeaderIfNeeded(tn)
                .thenCompose(__ -> scalableTopicService.splitSegment(tn, segmentId))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("segmentId", segmentId).attr("topic", tn)
                            .log("Split segment of scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("segmentId", segmentId).attr("topic", tn)
                            .exception(ex).log("Failed to split segment");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/merge/{segmentId1}/{segmentId2}")
    @ApiOperation(value = "Merge two adjacent segments into one.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Segments merged successfully"),
            @ApiResponse(code = 404, message = "Scalable topic or segment doesn't exist"),
            @ApiResponse(code = 412, message = "Segments are not active or not adjacent"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void mergeSegments(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "First segment ID to merge", required = true)
            @PathParam("segmentId1") long segmentId1,
            @ApiParam(value = "Second segment ID to merge", required = true)
            @PathParam("segmentId2") long segmentId2) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        var scalableTopicService = pulsar().getBrokerService().getScalableTopicService();
        if (scalableTopicService == null) {
            asyncResponse.resume(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
            return;
        }

        redirectToControllerLeaderIfNeeded(tn)
                .thenCompose(__ -> scalableTopicService.mergeSegments(tn, segmentId1, segmentId2))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("segmentId1", segmentId1).attr("segmentId2", segmentId2)
                            .attr("topic", tn).log("Merged segments of scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("segmentId1", segmentId1).attr("segmentId2", segmentId2)
                            .attr("topic", tn).exception(ex)
                            .log("Failed to merge segments");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Internal helpers ---

    /**
     * If this broker is not the elected controller leader for {@code tn}, redirect the
     * request to the leader via HTTP 307. Read-only endpoints (like {@code getStats}) do
     * not need this guard and should not call it.
     *
     * <p>The leader brokerId is read from the controller lock znode and resolved to an HTTP
     * service URL via {@link org.apache.pulsar.broker.namespace.NamespaceService#createLookupResult}.
     * Returns a future that completes normally when the local broker is the leader (or no
     * leader is elected yet, in which case the caller should proceed and let {@code
     * getOrCreateController} participate in the election). The future completes
     * exceptionally with {@link WebApplicationException} wrapping a 307 redirect when the
     * request must be forwarded to another broker.
     */
    private CompletableFuture<Void> redirectToControllerLeaderIfNeeded(TopicName tn) {
        String lockPath = resources().controllerLockPath(tn);
        return resources().getStore().get(lockPath)
                .thenCompose(optValue -> {
                    if (optValue.isEmpty()) {
                        // No leader elected yet — let the caller's getOrCreateController run
                        // election. It will either become leader on this broker or fail with
                        // IllegalStateException, which the caller surfaces to the client.
                        return CompletableFuture.completedFuture(null);
                    }
                    String leaderBrokerId = deserializeLeaderBrokerId(optValue.get().getValue());
                    if (leaderBrokerId.equals(pulsar().getBrokerId())) {
                        // We are the leader — proceed with the operation locally.
                        return CompletableFuture.completedFuture(null);
                    }
                    // Someone else is the leader — redirect.
                    return pulsar().getNamespaceService()
                            .createLookupResult(leaderBrokerId, false, null)
                            .thenCompose(lookupResult -> {
                                String redirectUrl = isRequestHttps()
                                        ? lookupResult.getLookupData().getHttpUrlTls()
                                        : lookupResult.getLookupData().getHttpUrl();
                                if (redirectUrl == null) {
                                    return FutureUtil.failedFuture(new RestException(
                                            Response.Status.PRECONDITION_FAILED,
                                            "Controller leader broker " + leaderBrokerId
                                                    + " has no web service URL configured"));
                                }
                                try {
                                    URL url = new URL(redirectUrl);
                                    URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                            .host(url.getHost())
                                            .port(url.getPort())
                                            .build();
                                    log.debug().attr("topic", tn).attr("redirect", redirect)
                                            .log("Redirecting scalable-topic admin request to controller leader");
                                    return FutureUtil.failedFuture(new WebApplicationException(
                                            Response.temporaryRedirect(redirect).build()));
                                } catch (MalformedURLException ex) {
                                    return FutureUtil.failedFuture(new RestException(ex));
                                }
                            });
                });
    }

    /**
     * The controller-lock znode stores the leader's brokerId as a JSON-encoded string
     * (written by {@link org.apache.pulsar.metadata.api.coordination.LeaderElection#elect}
     * via Jackson), so the raw bytes include the JSON quotes. Decode it back to a plain
     * string.
     */
    private static String deserializeLeaderBrokerId(byte[] bytes) {
        try {
            return org.apache.pulsar.common.util.ObjectMapperFactory.getMapper()
                    .reader().readValue(bytes, String.class);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "Invalid controller-leader znode value: " + new String(bytes), e);
        }
    }

    /**
     * Best-effort delete underlying persistent topics for all segments.
     * Uses the internal admin client which handles cross-broker routing.
     */
    private CompletableFuture<Void> deleteSegmentTopics(TopicName parentTopic,
                                                         ScalableTopicMetadata metadata,
                                                         boolean force) {
        try {
            var admin = pulsar().getAdminClient();
            CompletableFuture<?>[] futures = metadata.getSegments().values().stream()
                    .map(seg -> {
                        String name = segmentPersistentName(parentTopic, seg);
                        return admin.topics().deleteAsync(name, force)
                                .exceptionally(ex -> {
                                    log.warn().attr("segment", name).exceptionMessage(ex)
                                            .log("Failed to delete segment topic");
                                    return null;
                                });
                    })
                    .toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(futures);
        } catch (Exception e) {
            log.warn().attr("topic", parentTopic).exceptionMessage(e)
                    .log("Failed to get admin client for segment cleanup");
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Convert a segment:// topic name to persistent:// for the underlying managed ledger topic.
     */
    private String segmentPersistentName(TopicName parentTopic, SegmentInfo segment) {
        TopicName segTopic = SegmentTopicName.fromParent(
                parentTopic, segment.hashRange(), segment.segmentId());
        return "persistent://" + segTopic.getTenant() + "/"
                + segTopic.getNamespacePortion() + "/"
                + segTopic.getLocalName();
    }
}
