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
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Encoded;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Admin REST API for segment topic operations.
 *
 * <p>These endpoints route to the broker owning the segment's namespace bundle
 * via {@code validateTopicOwnershipAsync}.
 *
 * <p><b>Authorization:</b> all endpoints in this resource are state-modifying
 * cross-broker coordination primitives invoked by the controller broker during
 * scalable-topic split/merge. They require <b>super-user</b> access. End users
 * (including tenant admins) should use the {@link ScalableTopics} resource
 * instead, which provides the user-facing operations on scalable topics.
 */
@CustomLog
@Path("/segments")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "segments", description = "Segment topic admin APIs")
public class Segments extends AdminResource {

    /** Preserve the v2 path interpretation used by older controller brokers. */
    protected String topicLocalName(String encodedTopic) {
        return encodedTopic;
    }

    protected TopicName segmentTopicName(String tenant, String namespace,
                                       String encodedTopic, String descriptor) {
        return TopicName.get(TopicDomain.segment.value() + "://" + tenant + "/" + namespace + "/"
                + topicLocalName(encodedTopic) + "/" + descriptor);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}")
    @Operation(summary = "Create a segment topic on the owning broker. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Segment topic created successfully"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void createSegment(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @RequestBody(description = "Subscriptions to create on the new segment")
            List<String> subscriptions) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                // Explicit create — segments don't go through the auto-create policy
                // (BrokerService.isAllowAutoTopicCreationAsync forbids segment auto-create).
                .thenCompose(__ -> pulsar().getBrokerService()
                        .getTopic(segmentTopic.toString(), true)
                        .thenApply(Optional::get))
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
    @Operation(summary = "Terminate a segment topic so no more messages can be published. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Segment topic terminated successfully"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "404", description = "Segment topic not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void terminateSegment(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
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

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/subscription/{subscription}")
    @Operation(summary = "Create a subscription cursor on the segment topic at the earliest"
            + " position. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Subscription cursor created (or already existed)"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void createSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getOrCreateTopic(segmentTopic.toString()))
                .thenCompose(topic -> topic.createSubscription(subscription,
                        CommandSubscribe.InitialPosition.Earliest, false, null))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription)
                            .log("Created subscription on segment topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription)
                            .exception(ex).log("Failed to create subscription on segment");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/subscription/{subscription}")
    @Operation(summary = "Delete a subscription cursor on the segment topic. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Subscription cursor deleted (or never existed)"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void deleteSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        // Topic not loaded → no cursor to delete. Idempotent success.
                        return CompletableFuture.completedFuture(null);
                    }
                    var sub = optTopic.get().getSubscription(subscription);
                    if (sub == null) {
                        // Subscription doesn't exist on this segment — idempotent success.
                        return CompletableFuture.completedFuture(null);
                    }
                    return sub.delete();
                })
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription)
                            .log("Deleted subscription on segment topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription)
                            .exception(ex).log("Failed to delete subscription on segment");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/subscription/{subscription}/backlog")
    @Operation(summary = "Number of unconsumed entries in the segment topic for the "
            + "given subscription. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "404", description = "Segment topic or subscription not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void getSubscriptionBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenAccept(optTopic -> {
                    if (optTopic.isEmpty()) {
                        // No topic loaded → no subscription cursor → no backlog. Returning
                        // 0 here would be wrong (caller might mark the segment drained on
                        // a topic that simply hasn't loaded yet); a 404 forces the caller
                        // to retry, which matches our drain-poll contract.
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Segment topic not loaded: " + segmentTopic);
                    }
                    var sub = optTopic.get().getSubscription(subscription);
                    if (sub == null) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Subscription not found on segment: " + subscription);
                    }
                    asyncResponse.resume(sub.getNumberOfEntriesInBacklog(false));
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .exception(ex).log("Failed to get segment subscription backlog");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/subscription/{subscription}/seek")
    @Operation(summary = "Reset the segment topic's subscription cursor to the given timestamp."
            + " Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Cursor reset successfully"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "404", description = "Segment topic or subscription not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void seekSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Wall-clock millis since the unix epoch", required = true)
            @QueryParam("timestamp") long timestampMs,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        // Segment topic not loaded on this owner — could be ownership
                        // churn or a transient unload. 503 so the caller retries (and so
                        // the parent-topic seek can't conflate this with the
                        // subscription-not-found case below, which is tolerated).
                        throw new RestException(Response.Status.SERVICE_UNAVAILABLE,
                                "Segment topic not loaded: " + segmentTopic);
                    }
                    var sub = optTopic.get().getSubscription(subscription);
                    if (sub == null) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Subscription not found on segment: " + subscription);
                    }
                    return sub.resetCursor(timestampMs)
                            .exceptionally(ex -> {
                                Throwable cause = ex instanceof java.util.concurrent.CompletionException
                                        ? ex.getCause() : ex;
                                if (cause instanceof org.apache.pulsar.broker.service.BrokerServiceException
                                        .SubscriptionInvalidCursorPosition) {
                                    // Empty managed ledger — no entries to seek to. The
                                    // cursor is already at the only valid position, so this
                                    // is a no-op (e.g. a freshly-split active child segment
                                    // that hasn't received any messages yet).
                                    log.debug().attr("segment", segmentTopic)
                                            .attr("subscription", subscription)
                                            .log("Empty segment, treating seek as no-op");
                                    return null;
                                }
                                throw org.apache.pulsar.common.util.FutureUtil
                                        .wrapToCompletionException(cause);
                            });
                })
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription).attr("timestampMs", timestampMs)
                            .exception(ex).log("Failed to seek segment subscription");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}/subscription/{subscription}/skip-all")
    @Operation(summary = "Skip every undelivered message on the segment topic's subscription —"
            + " advance the cursor to the end. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Backlog cleared successfully"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "404", description = "Segment topic or subscription not found"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void clearSubscriptionBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        // 503 vs 404 — see the rationale on the seek endpoint above. The
                        // distinction lets the parent-topic clear-backlog tolerate
                        // subscription-not-found while still surfacing transient unloads.
                        throw new RestException(Response.Status.SERVICE_UNAVAILABLE,
                                "Segment topic not loaded: " + segmentTopic);
                    }
                    var sub = optTopic.get().getSubscription(subscription);
                    if (sub == null) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                "Subscription not found on segment: " + subscription);
                    }
                    return sub.clearBacklog();
                })
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("segment", segmentTopic)
                            .attr("subscription", subscription)
                            .exception(ex).log("Failed to clear segment subscription backlog");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/{descriptor}")
    @Operation(summary = "Delete a segment topic. Super-user only.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Segment topic deleted successfully"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void deleteSegment(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the parent topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment descriptor (e.g. 0000-7fff-1)", required = true)
            @PathParam("descriptor") String descriptor,
            @Parameter(description = "Whether leader broker redirected this call to this broker.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Parameter(description = "Force deletion")
            @QueryParam("force") @DefaultValue("false") boolean force) {
        validateNamespaceName(tenant, namespace);
        TopicName segmentTopic = segmentTopicName(tenant, namespace, encodedTopic, descriptor);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateTopicOwnershipAsync(segmentTopic, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(segmentTopic.toString()))
                .thenCompose(optTopic -> {
                    if (optTopic.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    Topic t = optTopic.get();
                    return (force ? t.deleteForcefully() : t.delete()).thenApply(__ -> null);
                })
                .thenCompose(__ -> deleteSegmentTxnState(segmentTopic))
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

    /**
     * Delete what the transactions on a segment keep in the metadata store, which deleting the topic leaves in
     * place: the write records of the transactions still open on it, their ack records on its subscriptions, and
     * the state of the segment's transaction buffer. The buffer and the pending-ack store of each subscription
     * delete a transaction's records as they apply its outcome; left behind, they keep the coordinator from
     * garbage-collecting the transaction, and make a segment re-created with the same name recover it as open.
     * Only once the segment's data is gone: the aborted-txn records are what hides the data of aborted
     * transactions.
     *
     * <p>Not while the segment is still active in the layout of its topic: a client can then load it again (see
     * {@code BrokerService#isAllowAutoTopicCreationAsync}), and its new transaction buffer recovers from these
     * records. They are left to the delete that takes the segment out of the layout. Best effort: once the topic is
     * gone, none of the callers retries the delete.
     */
    private CompletableFuture<Void> deleteSegmentTxnState(TopicName segmentTopic) {
        if (!pulsar().getConfiguration().isTransactionCoordinatorEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        String segment = segmentTopic.toString();
        return isActiveInLayout(segmentTopic)
                .thenCompose(active -> {
                    if (active) {
                        log.debug().attr("segment", segment)
                                .log("Keeping the transaction state of a segment still active in its topic");
                        return CompletableFuture.completedFuture(null);
                    }
                    TxnMetadataStore txnStore = new TxnMetadataStore(pulsar().getLocalMetadataStore());
                    // Independently: the write and ack records are the ones that matter. A leftover watermark or
                    // aborted record is harmless to a later segment with the same name (ledger ids only grow, txn
                    // ids are unique).
                    return FutureUtil.waitForAll(List.of(
                            txnStore.deleteWriteOpsForSegment(segment),
                            txnStore.deleteAckOpsForSegment(segment),
                            txnStore.deleteAllSegmentState(segment)));
                })
                .exceptionally(ex -> {
                    log.warn().attr("segment", segment).exceptionMessage(ex)
                            .log("Failed to delete the transaction state of the deleted segment topic");
                    return null;
                });
    }

    /** Whether the segment is active in the layout of its topic. */
    private CompletableFuture<Boolean> isActiveInLayout(TopicName segmentTopic) {
        ScalableTopicResources resources = pulsar().getPulsarResources().getScalableTopicResources();
        if (resources == null) {
            return CompletableFuture.completedFuture(false);
        }
        // Not from the cache: the topic delete and the controller GC update the layout from another broker, and a
        // stale layout that still has the segment active would leave its state behind for good
        return resources.getScalableTopicMetadataAsync(SegmentTopicName.getParentTopicName(segmentTopic), true)
                .thenApply(optMd -> optMd
                        .map(md -> md.getSegments().get(SegmentTopicName.getSegmentId(segmentTopic)))
                        .map(seg -> seg.isActive())
                        .orElse(false));
    }
}
