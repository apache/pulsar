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
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 */
@Path("/non-persistent")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "non-persistent topic", description = "Non-Persistent topic admin apis")
@SuppressWarnings("deprecation")
public class NonPersistentTopics extends PersistentTopics {
    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @Operation(summary = "Get partitioned topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get partitioned topic metadata.",
                    content = @Content(schema = @Schema(implementation = PartitionedTopicMetadata.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "The tenant/namespace/topic does not exist"),
            @ApiResponse(responseCode = "412", description = "Topic name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate cluster configuration")
    })
    public void getPartitionedMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Parameter(description = "Is check configuration required to automatically create topic")
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOwnershipAsync(topicName, authoritative).whenComplete((__, ex) -> {
            if (ex != null) {
                Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                if (isNot307And404Exception(actEx)) {
                    log.error()
                            .attr("topic", topicName)
                            .exception(ex)
                            .log("Failed to get internal stats for topic");
                }
                resumeAsyncResponseExceptionally(asyncResponse, actEx);
            } else {
                // "super.getPartitionedMetadata" will handle error itself.
                super.getPartitionedMetadata(asyncResponse, tenant, namespace, encodedTopic, authoritative,
                        checkAllowAutoCreation);
            }
        });
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @Operation(summary = "Get the internal stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the internal stats for the topic.",
                    content = @Content(schema = @Schema(implementation = PersistentTopicInternalStats.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "The tenant/namespace/topic does not exist"),
            @ApiResponse(responseCode = "412", description = "Topic name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
    })
    public void getInternalStats(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.GET_STATS))
                .thenCompose(__ -> {
                    Topic topic = getTopicReference(topicName);
                    boolean includeMetadata = metadata && hasSuperUserAccess();
                    return topic.getInternalStats(includeMetadata);
                })
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    if (isNot307And404Exception(ex)) {
                        log.error()
                                .attr("topic", topicName)
                                .exception(ex)
                                .log("Failed to get internal stats for topic");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @Operation(summary = "Create a partitioned topic.",
            description = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "The tenant/namespace does not exist"),
            @ApiResponse(responseCode = "406", description = "The number of partitions should be more than 0 and less"
                    + " than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(responseCode = "409", description = "Partitioned topic already exists"),
            @ApiResponse(responseCode = "412", description = "Failed Reason : Name is invalid or "
                    + "Namespace does not have any clusters configured"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate global cluster configuration"),
    })
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @RequestBody(description = "The number of partitions for the topic, or the partitioned topic metadata"
                    + " (partitions and properties) when the request is sent with the '"
                    + PartitionedTopicMetadata.MEDIA_TYPE + "' content type",
                    required = true, content = {
                            @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(type = "integer", defaultValue = "0")),
                            @Content(mediaType = PartitionedTopicMetadata.MEDIA_TYPE,
                                    schema = @Schema(implementation = PartitionedTopicMetadata.class))})
                    int numPartitions,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        validateAndCreatePartitionedTopic(asyncResponse, tenant, namespace, encodedTopic, numPartitions,
                createLocalTopicOnly, null);
    }

    // Also handles the inherited 'application/vnd.partitioned-topic-metadata+json' createPartitionedTopic
    // overload: non-persistent topics validate the topic name only.
    @Override
    protected void validateAndCreatePartitionedTopic(AsyncResponse asyncResponse, String tenant, String namespace,
            String encodedTopic, int numPartitions, boolean createLocalTopicOnly, Map<String, String> properties) {
        try {
            validateNamespaceName(tenant, namespace);
            validateGlobalNamespaceOwnership();
            validateTopicName(tenant, namespace, encodedTopic);
            internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly, properties);
        } catch (Exception e) {
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to create partitioned topic");
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-stats")
    @Operation(
            summary = "Get the stats for the partitioned topic."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the stats for the partitioned topic.",
                    content = @Content(schema =
                            @Schema(implementation = NonPersistentPartitionedTopicStatsImpl.class))),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic does not exist"),
            @ApiResponse(responseCode = "412", description = "Partitioned topic name is invalid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate global cluster configuration")
    })
    public void getPartitionedStats(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Get per partition stats")
            @QueryParam("perPartition") @DefaultValue("true") boolean perPartition,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Parameter(description = "If return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog,
            @Parameter(description = "If return backlog size for each subscription, require locking on ledger so be "
                    + "careful not to use when there's heavy traffic.")
            @QueryParam("subscriptionBacklogSize") @DefaultValue("false") boolean subscriptionBacklogSize,
            @Parameter(description = "If return the earliest time in backlog")
            @QueryParam("getEarliestTimeInBacklog") @DefaultValue("false") boolean getEarliestTimeInBacklog,
            @Parameter(description = "If exclude the publishers")
            @QueryParam("excludePublishers") @DefaultValue("false") boolean excludePublishers,
            @Parameter(description = "If exclude the consumers")
            @QueryParam("excludeConsumers") @DefaultValue("false") boolean excludeConsumers) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            if (topicName.isPartitioned()) {
                throw new RestException(Response.Status.PRECONDITION_FAILED,
                        "Partitioned Topic Name should not contain '-partition-'");
            }
            try {
                validateGlobalNamespaceOwnership(namespaceName);
            } catch (Exception e) {
                log.error()
                        .attr("topic", topicName)
                        .exception(e)
                        .log("Failed to get partitioned stats");
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
            getPartitionedTopicMetadataAsync(topicName,
                    authoritative, false).thenAccept(partitionMetadata -> {
                if (partitionMetadata.partitions == 0) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                            String.format("Partitioned topic not found %s", topicName.toString())));
                    return;
                }
                NonPersistentPartitionedTopicStatsImpl stats =
                        new NonPersistentPartitionedTopicStatsImpl(partitionMetadata);
                List<CompletableFuture<TopicStats>> topicStatsFutureList = new ArrayList<>();
                org.apache.pulsar.client.admin.GetStatsOptions statsOptions =
                        new org.apache.pulsar.client.admin.GetStatsOptions(
                                getPreciseBacklog,
                                subscriptionBacklogSize,
                                getEarliestTimeInBacklog,
                                excludePublishers,
                                excludeConsumers
                        );
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    try {
                        topicStatsFutureList
                                .add(pulsar().getAdminClient().topics().getStatsAsync(
                                        (topicName.getPartition(i).toString()), statsOptions));
                    } catch (PulsarServerException e) {
                        asyncResponse.resume(new RestException(e));
                        return;
                    }
                }

                FutureUtil.waitForAll(topicStatsFutureList).handle((result, exception) -> {
                    CompletableFuture<TopicStats> statFuture = null;
                    for (int i = 0; i < topicStatsFutureList.size(); i++) {
                        statFuture = topicStatsFutureList.get(i);
                        if (statFuture.isDone() && !statFuture.isCompletedExceptionally()) {
                            try {
                                stats.add((NonPersistentTopicStatsImpl) statFuture.get());
                                if (perPartition) {
                                    stats.getPartitions().put(topicName.getPartition(i).toString(),
                                            (NonPersistentTopicStatsImpl) statFuture.get());
                                }
                            } catch (Exception e) {
                                asyncResponse.resume(new RestException(e));
                                return null;
                            }
                        }
                    }
                    if (perPartition && stats.partitions.isEmpty()) {
                        try {
                            boolean topicExists = namespaceResources().getPartitionedTopicResources()
                                    .partitionedTopicExists(topicName);
                            if (topicExists) {
                                stats.getPartitions().put(topicName.toString(), new NonPersistentTopicStatsImpl());
                            } else {
                                asyncResponse.resume(
                                        new RestException(Status.NOT_FOUND,
                                                "Internal topics have not been generated yet"));
                                return null;
                            }
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                    asyncResponse.resume(stats);
                    return null;
                });
            }).exceptionally(ex -> {
                log.error()
                        .attr("topic", topicName)
                        .exception(ex)
                        .log("Failed to get partitioned stats");
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/unload")
    @Operation(summary = "Unload a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "307",
                    description = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(responseCode = "401", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "The tenant/namespace/topic does not exist"),
            @ApiResponse(responseCode = "412", description = "Topic name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate global cluster configuration"),
    })
    public void unloadTopic(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalUnloadTopic(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}")
    @Operation(summary = "Get the list of non-persistent topics under a namespace.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the list of non-persistent topics under a namespace.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "The tenant/namespace does not exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate global cluster configuration"),
    })
    @SuppressWarnings("deprecation")
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify the bundle name", required = false)
            @QueryParam("bundle") String nsBundle,
            @Parameter(description = "Include system topic")
            @QueryParam("includeSystemTopic") boolean includeSystemTopic,
            @QueryParam("properties") String propertiesStr) {
        Policies policies = null;
        try {
            validateNamespaceName(tenant, namespace);
                log.debug()
                        .attr("namespace", namespaceName)
                        .log("list of topics on namespace");
                        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);
            policies = getNamespacePolicies(namespaceName);

            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        final List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        final List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            final String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            if (StringUtils.isNotBlank(nsBundle) && !nsBundle.equals(bundle)) {
                continue;
            }
            try {
                futures.add(pulsar().getAdminClient().topics().getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
                log.error()
                        .attr("namespace", namespaceName)
                        .attr("bundle", bundle)
                        .exception(e)
                        .log("Failed to get list of topics under namespace");
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        FutureUtil.waitForAll(futures).whenComplete((result, ex) -> {
            if (ex != null) {
                resumeAsyncResponseExceptionally(asyncResponse, ex);
            } else {
                final List<String> topics = new ArrayList<>();
                for (int i = 0; i < futures.size(); i++) {
                    List<String> topicList = futures.get(i).join();
                    if (topicList != null) {
                        topics.addAll(topicList);
                    }
                }
                final List<String> nonPersistentTopics =
                        topics.stream()
                                .filter(name -> !TopicName.get(name).isPersistent())
                                .collect(Collectors.toList());
                asyncResponse.resume(filterSystemTopic(nonPersistentTopics, includeSystemTopic));
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @Operation(summary = "Get the list of non-persistent topics under a namespace bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the list of non-persistent topics under a namespace bundle.",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "401",
                    description = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace doesn't exist"),
            @ApiResponse(responseCode = "412", description = "Namespace name is not valid"),
            @ApiResponse(responseCode = "500", description = "Internal server error"),
            @ApiResponse(responseCode = "503", description = "Failed to validate global cluster configuration"),
    })
    @SuppressWarnings("deprecation")
    public void getListFromBundle(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Bundle range of a topic", required = true)
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
            log.debug()
                    .attr("namespace", namespaceName)
                    .attr("bundleRange", bundleRange)
                    .log("list of topics on namespace bundle");
                validateNamespaceOperation(namespaceName, NamespaceOperation.GET_BUNDLE);

        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
        validateGlobalNamespaceOwnership(namespaceName);

        isBundleOwnedByAnyBroker(namespaceName, bundleRange).thenAccept(flag -> {
            if (!flag) {
                log.info()
                        .attr("namespace", namespaceName)
                        .attr("bundleRange", bundleRange)
                        .log("Namespace bundle is not owned by any broker");
                asyncResponse.resume(Response.noContent().build());
            } else {
                validateNamespaceBundleOwnershipAsync(namespaceName, bundleRange, true, true)
                        .thenAccept(nsBundle -> {
                            final var bundleTopics = pulsar().getBrokerService().getMultiLayerTopicsMap()
                                    .get(namespaceName.toString());
                            if (bundleTopics == null || bundleTopics.isEmpty()) {
                                asyncResponse.resume(Collections.emptyList());
                                return;
                            }
                            final List<String> topicList = new ArrayList<>();
                            String bundleKey = namespaceName.toString() + "/" + nsBundle.getBundleRange();
                            final var topicMap = bundleTopics.get(bundleKey);
                            if (topicMap != null) {
                                topicList.addAll(topicMap.keySet().stream()
                                        .filter(name -> !TopicName.get(name).isPersistent())
                                        .collect(Collectors.toList()));
                            }
                            asyncResponse.resume(topicList);
                        }).exceptionally(ex -> {
                            if (isNot307And404Exception(ex)) {
                                log.error()
                                        .attr("namespace", namespaceName)
                                        .attr("bundleRange", bundleRange)
                                        .exception(ex)
                                        .log("Failed to list topics on namespace bundle");
                            }
                            resumeAsyncResponseExceptionally(asyncResponse, ex);
                            return null;
                        });
            }
        }).exceptionally(ex -> {
            if (isNot307And404Exception(ex)) {
                log.error()
                        .attr("namespace", namespaceName)
                        .attr("bundleRange", bundleRange)
                        .exception(ex)
                        .log("Failed to list topics on namespace bundle");
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/truncate")
    @Operation(summary = "Truncate a topic.",
            description = "NonPersistentTopic does not support truncate.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "412", description = "NonPersistentTopic does not support truncate.")
    })
    public void truncateTopic(
            @Suspended final AsyncResponse asyncResponse,
            @Parameter(description = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @Parameter(description = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @Parameter(description = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative){
        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED.getStatusCode(),
                "unsupport truncate"));
    }

    protected void validateAdminOperationOnTopic(TopicName topicName, boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @Operation(summary = "Get entry filters for a topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get entry filters for a topic.",
                    content = @Content(schema = @Schema(implementation = EntryFilters.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenants or Namespace doesn't exist") })
    public void getEntryFilters(@Suspended AsyncResponse asyncResponse,
                                @Parameter(description = "Specify the tenant", required = true)
                                @PathParam("tenant") String tenant,
                                @Parameter(description = "Specify the namespace", required = true)
                                @PathParam("namespace") String namespace,
                                @Parameter(description = "Specify topic name", required = true)
                                @PathParam("topic") @Encoded String encodedTopic,
                                @QueryParam("applied") @DefaultValue("false") boolean applied,
                                @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                @Parameter(description = "Whether leader broker redirected this call to this "
                                        + "broker. For internal use.")
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalGetEntryFilters(applied, isGlobal))
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    handleTopicPolicyException("getEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @Operation(summary = "Set entry filters for specified topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void setEntryFilters(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                @Parameter(description = "Whether leader broker redirected this "
                                        + "call to this broker. For internal use.")
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                @RequestBody(description = "Entry filters for the specified topic")
                                        EntryFilters entryFilters) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalSetEntryFilters(entryFilters, isGlobal))
                .thenAccept(__ -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    handleTopicPolicyException("setEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/entryFilters")
    @Operation(summary = "Remove entry filters for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeEntryFilters(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic,
                                   @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
                                   @Parameter(description = "Whether leader broker redirected this "
                                           + "call to this broker. For internal use.")
                                   @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        preValidation(authoritative)
                .thenCompose(__ -> internalRemoveEntryFilters(isGlobal))
                .thenRun(() -> {
                    log.info()
                            .attr("tenant", tenant)
                            .attr("namespace", namespace)
                            .attr("topic", topicName.getLocalName())
                            .attr("isGlobal", isGlobal)
                            .log("Successfully remove entry filters");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    handleTopicPolicyException("removeEntryFilters", ex, asyncResponse);
                    return null;
                });
    }

    private Topic getTopicReference(TopicName topicName) {
        try {
            return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Topic not found"));
        } catch (ExecutionException e) {
            throw new RestException(e.getCause());
        } catch (InterruptedException | TimeoutException e) {
            throw new RestException(e);
        }
    }
}
