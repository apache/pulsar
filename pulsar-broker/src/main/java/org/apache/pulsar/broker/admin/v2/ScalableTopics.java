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
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.broker.service.scalable.ScalableTopicService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;
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
@Tag(name = "scalable topic", description = "Scalable topic admin APIs")
public class ScalableTopics extends AdminResource {

    private ScalableTopicResources resources() {
        return pulsar().getPulsarResources().getScalableTopicResources();
    }

    // --- List ---

    @GET
    @Path("/{tenant}/{namespace}")
    @Operation(summary = "Get the list of scalable topics under a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the list of scalable topics under a namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Filter to topics whose properties contain every key=value pair."
                    + " Each repetition of the parameter adds one filter (AND semantics).")
            @QueryParam("property") List<String> properties) {
        validateNamespaceName(tenant, namespace);
        Map<String, String> propertyFilters = parseKeyValuePairs(properties);
        validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GET_TOPICS)
                .thenCompose(__ -> propertyFilters.isEmpty()
                        ? resources().listScalableTopicsAsync(namespaceName)
                        : resources().findScalableTopicsByPropertiesAsync(
                                namespaceName, propertyFilters))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId()).attr("namespace", namespaceName)
                            .exception(ex).log("Failed to list scalable topics");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    /**
     * Parse {@code key=value} entries from a list of query parameter values into a map.
     * Accepts {@code null} / empty input. Rejects malformed entries (no {@code =}, empty
     * key, or empty value) with a 412.
     */
    private static Map<String, String> parseKeyValuePairs(List<String> entries) {
        if (entries == null || entries.isEmpty()) {
            return Map.of();
        }
        Map<String, String> result = new java.util.LinkedHashMap<>(entries.size());
        for (String entry : entries) {
            if (entry == null || entry.isEmpty()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq <= 0 || eq == entry.length() - 1) {
                throw new RestException(Response.Status.fromStatusCode(412),
                        "property filter must be in the form key=value, got: " + entry);
            }
            result.put(entry.substring(0, eq), entry.substring(eq + 1));
        }
        return result;
    }

    // --- Create ---

    @PUT
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(summary = "Create a new scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Scalable topic created successfully"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "409", description = "Scalable topic already exists"),
            @ApiResponse(responseCode = "412", description = "Invalid configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void createScalableTopic(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Number of initial segments")
            @QueryParam("numInitialSegments") @DefaultValue("1") int numInitialSegments,
            @RequestBody(description = "Key value pair properties for the topic metadata")
            Map<String, String> properties) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateNamespaceOperationAsync(namespaceName, NamespaceOperation.CREATE_TOPIC)
                .thenCompose(__ -> {
                    if (numInitialSegments < 1) {
                        throw new RestException(Response.Status.fromStatusCode(412),
                                "numInitialSegments must be >= 1");
                    }
                    Map<String, String> props = properties != null ? properties : Map.of();
                    ScalableTopicMetadata metadata = ScalableTopicController.createInitialMetadata(
                            numInitialSegments, props);
                    return resources().createScalableTopicAsync(tn, metadata)
                            .thenCompose(ignored -> createInitialSegmentTopicsAsync(tn, metadata));
                })
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

    /**
     * Create the backing persistent topic for each segment in the initial layout.
     *
     * <p>Segment topics are NEVER auto-created on client connect (see
     * {@code BrokerService.isAllowAutoTopicCreationAsync}); they only come into
     * existence through the controller's explicit-create path. So at scalable-topic
     * creation time we have to materialize the initial segment(s) up front, before
     * any producer or consumer arrives.
     *
     * <p>Routes via the internal admin client so each segment's create lands on
     * its bundle's owning broker (segment bundles can hash to a different broker
     * than the one handling this REST call).
     */
    private CompletableFuture<Void> createInitialSegmentTopicsAsync(
            TopicName parentTopic, ScalableTopicMetadata metadata) {
        try {
            var admin = pulsar().getAdminClient();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (SegmentInfo seg : metadata.getSegments().values()) {
                String segmentTopic = SegmentTopicName.fromParent(
                        parentTopic, seg.hashRange(), seg.segmentId()).toString();
                futures.add(admin.scalableTopics().createSegmentAsync(segmentTopic, List.of()));
            }
            return FutureUtil.waitForAll(futures);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // --- Migrate (PIP-475 regular-to-scalable) ---

    @POST
    @Path("/{tenant}/{namespace}/{topic}/migrate")
    @Operation(summary = "Migrate an existing regular (partitioned or non-partitioned) topic "
            + "to a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Topic migrated successfully"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have produce permission on the topic"),
            @ApiResponse(responseCode = "404", description = "Topic doesn't exist"),
            @ApiResponse(responseCode = "409", description = "Already a scalable topic, or legacy v4 clients are "
                    + "still connected and force was not set"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void migrateToScalable(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Migrate even if legacy v4 clients are still connected to the topic")
            @QueryParam("force") @DefaultValue("false") boolean force) {
        validateNamespaceName(tenant, namespace);
        // The scalable topic's canonical identity uses the topic:// domain; the migration
        // source is the same name in the persistent:// domain.
        TopicName scalableName = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);
        TopicName persistentBase =
                TopicName.get(TopicDomain.persistent.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(persistentBase, TopicOperation.MIGRATE_TO_SCALABLE)
                .thenCompose(__ -> doMigrateToScalableAsync(scalableName, persistentBase, force))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId()).attr("topic", scalableName)
                            .attr("force", force).log("Migrated topic to scalable");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    Throwable cause = FutureUtil.unwrapCompletionException(ex);
                    if (cause instanceof MetadataStoreException.AlreadyExistsException) {
                        asyncResponse.resume(new RestException(Response.Status.CONFLICT,
                                "Topic is already scalable: " + scalableName));
                    } else {
                        log.error().attr("clientAppId", clientAppId()).attr("topic", scalableName)
                                .exception(ex).log("Failed to migrate topic to scalable");
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                    }
                    return null;
                });
    }

    /**
     * Orchestrate a regular-to-scalable migration:
     * <ol>
     *   <li>reject if scalable metadata already exists;</li>
     *   <li>resolve the source topic's existence + partition count;</li>
     *   <li>unless {@code force}, reject if any legacy v4 client is still connected;</li>
     *   <li>build the migrated layout (sealed legacy parents + active children);</li>
     *   <li>create the new child segment topics;</li>
     *   <li>atomically write the scalable metadata (the commit point — connected V5 lookup
     *       sessions transition from the synthetic layout to the real DAG via the metadata
     *       watch);</li>
     *   <li>terminate the old topics so no further v4 writes can land — they become the
     *       drainable sealed parent segments.</li>
     * </ol>
     */
    private CompletableFuture<Void> doMigrateToScalableAsync(TopicName scalableName,
                                                             TopicName persistentBase, boolean force) {
        return resources().getScalableTopicMetadataAsync(scalableName).thenCompose(existing -> {
            if (existing.isPresent()) {
                throw new RestException(Response.Status.CONFLICT,
                        "Topic is already scalable: " + scalableName);
            }
            return pulsar().getNamespaceService().checkTopicExistsAsync(persistentBase);
        }).thenCompose(existsInfo -> {
            boolean exists = existsInfo.isExists();
            int partitions = existsInfo.getPartitions();
            existsInfo.recycle();
            if (!exists) {
                throw new RestException(Response.Status.NOT_FOUND,
                        "Topic does not exist: " + persistentBase);
            }
            CompletableFuture<Void> precheck = force
                    ? CompletableFuture.completedFuture(null)
                    : checkNoLegacyConnectionsAsync(persistentBase, partitions);
            return precheck.thenApply(__ -> partitions);
        }).thenCompose(partitions -> {
            ScalableTopicMetadata metadata =
                    ScalableTopicController.createMigratedMetadata(persistentBase, partitions);
            return createMigratedChildTopicsAsync(scalableName, metadata)
                    .thenCompose(__ -> resources().createScalableTopicAsync(scalableName, metadata))
                    .thenCompose(__ -> terminateLegacyTopicsAsync(persistentBase, partitions));
        });
    }

    /**
     * Reject the migration if any producer/consumer attached to the source topic is a legacy
     * v4 client — i.e. its metadata lacks the V5-managed marker. V5 clients (which attach to
     * the synthetic layout's legacy segments and transition transparently) are excluded.
     */
    private CompletableFuture<Void> checkNoLegacyConnectionsAsync(TopicName persistentBase,
                                                                  int partitions) {
        final PulsarAdmin admin;
        try {
            admin = pulsar().getAdminClient();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        // For a partitioned topic, inspect per-partition stats rather than the aggregate:
        // aggregation merges publishers by producer name into fresh stat objects that drop
        // per-connection metadata, which would hide the V5-managed marker and make every
        // V5 connection look like a legacy v4 one.
        final CompletableFuture<Long> legacyCount = partitions > 0
                ? admin.topics().getPartitionedStatsAsync(persistentBase.toString(), true)
                        .thenApply(stats -> {
                            long count = 0;
                            for (TopicStats partitionStats : stats.getPartitions().values()) {
                                count += countLegacyConnections(partitionStats);
                            }
                            return count;
                        })
                : admin.topics().getStatsAsync(persistentBase.toString())
                        .thenApply(ScalableTopics::countLegacyConnections);
        return legacyCount.thenAccept(legacy -> {
            if (legacy > 0) {
                throw new RestException(Response.Status.CONFLICT,
                        legacy + " legacy v4 client connection(s) still attached to " + persistentBase
                                + "; disconnect them (or all clients are V5) before migrating, "
                                + "or retry with force=true");
            }
        });
    }

    private static long countLegacyConnections(TopicStats stats) {
        long count = 0;
        for (var publisher : stats.getPublishers()) {
            if (!isV5Managed(publisher.getMetadata())) {
                count++;
            }
        }
        for (var subscription : stats.getSubscriptions().values()) {
            for (var consumer : subscription.getConsumers()) {
                if (!isV5Managed(consumer.getMetadata())) {
                    count++;
                }
            }
        }
        return count;
    }

    private static boolean isV5Managed(Map<String, String> metadata) {
        return metadata != null && ScalableTopicConstants.V5_MANAGED_METADATA_VALUE
                .equals(metadata.get(ScalableTopicConstants.V5_MANAGED_METADATA_KEY));
    }

    /**
     * Create the backing segment topic for each new <i>active child</i> in the migrated layout.
     * The sealed legacy parents are skipped — they wrap existing {@code persistent://} topics
     * that already have managed ledgers.
     */
    private CompletableFuture<Void> createMigratedChildTopicsAsync(
            TopicName scalableName, ScalableTopicMetadata metadata) {
        try {
            var admin = pulsar().getAdminClient();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (SegmentInfo seg : metadata.getSegments().values()) {
                if (seg.isLegacy()) {
                    continue;
                }
                String segmentTopic = SegmentTopicName.fromParent(
                        scalableName, seg.hashRange(), seg.segmentId()).toString();
                futures.add(admin.scalableTopics().createSegmentAsync(segmentTopic, List.of()));
            }
            return FutureUtil.waitForAll(futures);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Terminate the old topic(s) so no further v4 writes can land. The terminated topics
     * become the drainable sealed parent segments of the new scalable topic.
     */
    private CompletableFuture<Void> terminateLegacyTopicsAsync(TopicName persistentBase,
                                                               int partitions) {
        try {
            var admin = pulsar().getAdminClient();
            CompletableFuture<?> terminate = partitions > 0
                    ? admin.topics().terminatePartitionedTopicAsync(persistentBase.toString())
                    : admin.topics().terminateTopicAsync(persistentBase.toString());
            return terminate.thenAccept(ignored -> { });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // --- Get metadata ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(summary = "Get scalable topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get scalable topic metadata.",
                    content = @Content(schema = @Schema(implementation = ScalableTopicMetadata.class))),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void getScalableTopicMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.GET_METADATA)
                .thenCompose(__ -> resources().getScalableTopicMetadataAsync(tn))
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
    @Operation(summary = "Delete a scalable topic and all its segments.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Scalable topic deleted successfully"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void deleteScalableTopic(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Force deletion even if topic has active subscriptions")
            @QueryParam("force") @DefaultValue("false") boolean force) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateNamespaceOperationAsync(namespaceName, NamespaceOperation.DELETE_TOPIC)
                .thenCompose(__ -> resources().getScalableTopicMetadataAsync(tn))
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
    @Operation(summary = "Get aggregated stats for a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get aggregated stats for a scalable topic.",
                    content = @Content(schema = @Schema(
                            implementation = org.apache.pulsar.common.policies.data.ScalableTopicStats.class))),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void getStats(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.GET_STATS)
                .thenCompose(__ -> withScalableTopicService(svc -> svc.getStats(tn)))
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
    @Operation(summary = "Create a subscription on a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Subscription created successfully"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void createSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Subscription type: STREAM (controller-managed, ordered) "
                    + "or QUEUE (direct per-segment attach, no controller coordination)")
            @QueryParam("type") @DefaultValue("STREAM")
                    org.apache.pulsar.broker.resources.SubscriptionType type) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.SUBSCRIBE, subscription)
                .thenCompose(__ -> onControllerLeader(tn,
                        svc -> svc.createSubscription(tn, subscription, type)))
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
    @Operation(summary = "Delete a subscription from a scalable topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Subscription deleted successfully"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic or subscription doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void deleteSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.UNSUBSCRIBE, subscription)
                .thenCompose(__ -> onControllerLeader(tn,
                        svc -> svc.deleteSubscription(tn, subscription)))
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

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscriptions/{subscription}/seek")
    @Operation(summary = "Reset a subscription's cursor on every segment to the given"
            + " wall-clock timestamp. The controller uses each segment's recorded sealed-time"
            + " window to dispatch the cheapest per-segment op.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Cursor reset successfully on all segments"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic or subscription doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void seekSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription,
            @Parameter(description = "Wall-clock millis since the unix epoch", required = true)
            @QueryParam("timestamp") long timestampMs) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.RESET_CURSOR, subscription)
                .thenCompose(__ -> onControllerLeader(tn,
                        svc -> svc.seekSubscription(tn, subscription, timestampMs)))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .attr("timestampMs", timestampMs)
                            .log("Sought subscription on scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .exception(ex).log("Failed to seek subscription");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscriptions/{subscription}/skip-all")
    @Operation(summary = "Skip every undelivered message on the subscription, across every"
            + " segment in the DAG (advance each per-segment cursor to the end).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Backlog cleared successfully on all segments"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission on the namespace"),
            @ApiResponse(responseCode = "404", description = "Scalable topic or subscription doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void clearBacklog(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Subscription name", required = true)
            @PathParam("subscription") String subscription) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateTopicOperationAsync(tn, TopicOperation.SKIP, subscription)
                .thenCompose(__ -> onControllerLeader(tn,
                        svc -> svc.clearBacklog(tn, subscription)))
                .thenAccept(__ -> {
                    log.info().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .log("Cleared backlog on scalable topic");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    log.error().attr("clientAppId", clientAppId())
                            .attr("subscription", subscription).attr("topic", tn)
                            .exception(ex).log("Failed to clear subscription backlog");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    // --- Segment operations ---

    @POST
    @Path("/{tenant}/{namespace}/{topic}/split/{segmentId}")
    @Operation(summary = "Split a segment into two halves.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Segment split successfully"),
            @ApiResponse(responseCode = "404", description = "Scalable topic or segment doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Segment is not active or cannot be split"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void splitSegment(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Segment ID to split", required = true)
            @PathParam("segmentId") long segmentId) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> onControllerLeader(tn, svc -> svc.splitSegment(tn, segmentId)))
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
    @Operation(summary = "Merge two adjacent segments into one.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Segments merged successfully"),
            @ApiResponse(responseCode = "404", description = "Scalable topic or segment doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Segments are not active or not adjacent"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void mergeSegments(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "First segment ID to merge", required = true)
            @PathParam("segmentId1") long segmentId1,
            @Parameter(description = "Second segment ID to merge", required = true)
            @PathParam("segmentId2") long segmentId2) {
        validateNamespaceName(tenant, namespace);
        TopicName tn = TopicName.get(TopicDomain.topic.value(), namespaceName, encodedTopic);

        validateSuperUserAccessAsync()
                .thenCompose(__ -> onControllerLeader(tn,
                        svc -> svc.mergeSegments(tn, segmentId1, segmentId2)))
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
     * Resolve the local scalable-topic service and pass it to {@code op}. Surfaces a 503
     * RestException if the service is not available on this broker.
     */
    private <T> CompletableFuture<T> withScalableTopicService(
            Function<ScalableTopicService, CompletableFuture<T>> op) {
        ScalableTopicService svc = pulsar().getBrokerService().getScalableTopicService();
        if (svc == null) {
            return FutureUtil.failedFuture(new RestException(Response.Status.SERVICE_UNAVAILABLE,
                    "Scalable topic service not available"));
        }
        return op.apply(svc);
    }

    /**
     * Invoke {@code op} on the controller leader for {@code tn}. Combines the
     * service-availability check with {@link #redirectToControllerLeaderIfNeeded(TopicName)}
     * so that endpoints requiring the elected controller leader can express the operation
     * as a single chained step.
     */
    private <T> CompletableFuture<T> onControllerLeader(TopicName tn,
            Function<ScalableTopicService, CompletableFuture<T>> op) {
        return withScalableTopicService(svc -> redirectToControllerLeaderIfNeeded(tn)
                .thenCompose(__ -> op.apply(svc)));
    }

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
     * Best-effort delete the underlying topic for every segment in the DAG. Uses the
     * segment-aware admin endpoint, which routes to the segment-owning broker via the
     * standard bundle-ownership lookup.
     */
    private CompletableFuture<Void> deleteSegmentTopics(TopicName parentTopic,
                                                         ScalableTopicMetadata metadata,
                                                         boolean force) {
        try {
            var admin = pulsar().getAdminClient();
            CompletableFuture<?>[] futures = metadata.getSegments().values().stream()
                    .map(seg -> {
                        String segmentTopicName = SegmentTopicName.fromParent(
                                parentTopic, seg.hashRange(), seg.segmentId()).toString();
                        return admin.scalableTopics().deleteSegmentAsync(segmentTopicName, force)
                                .exceptionally(ex -> {
                                    log.warn().attr("segment", segmentTopicName).exceptionMessage(ex)
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
}
