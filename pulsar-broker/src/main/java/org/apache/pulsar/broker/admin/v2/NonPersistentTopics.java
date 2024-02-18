/**
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

import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
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
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/non-persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/non-persistent", description = "Non-Persistent topic admin apis", tags = "non-persistent topic")
public class NonPersistentTopics extends PersistentTopics {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopics.class);

    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate cluster configuration")
    })
    public void getPartitionedMetadata(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is check configuration required to automatically create topic")
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        super.getPartitionedMetadata(asyncResponse, tenant, namespace, encodedTopic, authoritative,
                checkAllowAutoCreation);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
    })
    public NonPersistentTopicStats getStats(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "If return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog,
            @ApiParam(value = "If return backlog size for each subscription, require locking on ledger so be careful "
                    + "not to use when there's heavy traffic.")
            @QueryParam("subscriptionBacklogSize") @DefaultValue("false") boolean subscriptionBacklogSize,
            @ApiParam(value = "If return time of the earliest message in backlog")
            @QueryParam("getEarliestTimeInBacklog") @DefaultValue("false") boolean getEarliestTimeInBacklog) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);

        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats(getPreciseBacklog, subscriptionBacklogSize,
                getEarliestTimeInBacklog);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
    })
    public PersistentTopicInternalStats getInternalStats(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("metadata") @DefaultValue("false") boolean metadata) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);
        Topic topic = getTopicReference(topicName);
        try {
            boolean includeMetadata = metadata && hasSuperUserAccess();
            return topic.getInternalStats(includeMetadata).get();
        } catch (Exception e) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR,
                    (e instanceof ExecutionException) ? e.getCause().getMessage() : e.getMessage());
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.",
            notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace does not exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than"
                    + " or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exists"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or "
                    + "Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The number of partitions for the topic",
                    required = true, type = "int", defaultValue = "0")
                    int numPartitions,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        try {
            validateNamespaceName(tenant, namespace);
            validateGlobalNamespaceOwnership();
            validateTopicName(tenant, namespace, encodedTopic);
            internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }


    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-stats")
    @ApiOperation(value = "Get the stats for the partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void getPartitionedStats(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Get per partition stats")
            @QueryParam("perPartition") @DefaultValue("true") boolean perPartition,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "If return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog,
            @ApiParam(value = "If return backlog size for each subscription, require locking on ledger so be careful "
                    + "not to use when there's heavy traffic.")
            @QueryParam("subscriptionBacklogSize") @DefaultValue("false") boolean subscriptionBacklogSize,
            @ApiParam(value = "If return the earliest time in backlog")
            @QueryParam("getEarliestTimeInBacklog") @DefaultValue("false") boolean getEarliestTimeInBacklog) {
        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            if (topicName.isGlobal()) {
                try {
                    validateGlobalNamespaceOwnership(namespaceName);
                } catch (Exception e) {
                    log.error("[{}] Failed to get partitioned stats for {}", clientAppId(), topicName, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return;
                }
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
                List<CompletableFuture<TopicStats>> topicStatsFutureList = Lists.newArrayList();
                for (int i = 0; i < partitionMetadata.partitions; i++) {
                    try {
                        topicStatsFutureList
                                .add(pulsar().getAdminClient().topics().getStatsAsync(
                                        (topicName.getPartition(i).toString()), getPreciseBacklog,
                                        subscriptionBacklogSize, getEarliestTimeInBacklog));
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
                log.error("[{}] Failed to get partitioned stats for {}", clientAppId(), topicName, ex);
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
    @ApiOperation(value = "Unload a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace/topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void unloadTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
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
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace does not exist"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify the bundle name", required = false)
            @QueryParam("bundle") String nsBundle) {
        Policies policies = null;
        try {
            validateNamespaceName(tenant, namespace);
            if (log.isDebugEnabled()) {
                log.debug("[{}] list of topics on namespace {}", clientAppId(), namespaceName);
            }
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

        final List<CompletableFuture<List<String>>> futures = Lists.newArrayList();
        final List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            final String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            if (StringUtils.isNotBlank(nsBundle) && !nsBundle.equals(bundle)) {
                continue;
            }
            try {
                futures.add(pulsar().getAdminClient().topics().getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
                log.error("[{}] Failed to get list of topics under namespace {}/{}", clientAppId(), namespaceName,
                        bundle, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        FutureUtil.waitForAll(futures).whenComplete((result, ex) -> {
            if (ex != null) {
                resumeAsyncResponseExceptionally(asyncResponse, ex);
            } else {
                final List<String> topics = Lists.newArrayList();
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
                asyncResponse.resume(nonPersistentTopics);
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace bundle.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void getListFromBundle(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Bundle range of a topic", required = true)
            @PathParam("bundle") String bundleRange) {
        validateNamespaceName(tenant, namespace);
        if (log.isDebugEnabled()) {
            log.debug("[{}] list of topics on namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);
        }

        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_BUNDLE);
        Policies policies = getNamespacePolicies(namespaceName);

        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
        validateGlobalNamespaceOwnership(namespaceName);

        isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange).thenAccept(flag -> {
            if (!flag) {
                log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(), namespaceName,
                        bundleRange);
                asyncResponse.resume(Response.noContent().build());
            } else {
                NamespaceBundle nsBundle;
                try {
                    nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles,
                        bundleRange, true, true);
                } catch (WebApplicationException wae) {
                    asyncResponse.resume(wae);
                    return;
                }
                try {
                    ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>> bundleTopics =
                            pulsar().getBrokerService().getMultiLayerTopicsMap().get(namespaceName.toString());
                    if (bundleTopics == null || bundleTopics.isEmpty()) {
                        asyncResponse.resume(Collections.emptyList());
                        return;
                    }
                    final List<String> topicList = Lists.newArrayList();
                    String bundleKey = namespaceName.toString() + "/" + nsBundle.getBundleRange();
                    ConcurrentOpenHashMap<String, Topic> topicMap = bundleTopics.get(bundleKey);
                    if (topicMap != null) {
                        topicList.addAll(topicMap.keys().stream()
                                .filter(name -> !TopicName.get(name).isPersistent())
                                .collect(Collectors.toList()));
                    }
                    asyncResponse.resume(topicList);
                } catch (Exception e) {
                    if (!isNot307And404Exception(e)) {
                        log.error("[{}] Failed to list topics on namespace bundle {}/{}", clientAppId(),
                                namespaceName, bundleRange, e);
                    }
                    asyncResponse.resume(new RestException(e));
                }
            }
        }).exceptionally(ex -> {
            if (!isNot307And404Exception(ex)) {
                log.error("[{}] Failed to list topics on namespace bundle {}/{}", clientAppId(),
                        namespaceName, bundleRange, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/truncate")
    @ApiOperation(value = "Truncate a topic.",
            notes = "NonPersistentTopic does not support truncate.")
    @ApiResponses(value = {
            @ApiResponse(code = 412, message = "NonPersistentTopic does not support truncate.")
    })
    public void truncateTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative){
        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED.getStatusCode(),
                "unsupport truncate"));
    }

    protected void validateAdminOperationOnTopic(TopicName topicName, boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
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
