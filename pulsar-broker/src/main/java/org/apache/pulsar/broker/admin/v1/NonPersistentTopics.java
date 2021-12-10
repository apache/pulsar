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

package org.apache.pulsar.broker.admin.v1;

import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/non-persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/non-persistent",
        description = "Non-Persistent topic admin apis", tags = "non-persistent topic", hidden = true)
@SuppressWarnings("deprecation")
public class NonPersistentTopics extends PersistentTopics {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopics.class);

    @GET
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Get partitioned topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission")})
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
                                                           @PathParam("cluster") String cluster,
                                                           @PathParam("namespace") String namespace,
                                                           @PathParam("topic") @Encoded String encodedTopic,
                                                           @QueryParam("authoritative") @DefaultValue("false")
                                                                       boolean authoritative,
                                                           @QueryParam("checkAllowAutoCreation") @DefaultValue("false")
                                                                       boolean checkAllowAutoCreation) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative, checkAllowAutoCreation);
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/stats")
    @ApiOperation(hidden = true, value = "Get the stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist")})
    public NonPersistentTopicStats getStats(@PathParam("property") String property,
                                            @PathParam("cluster") String cluster,
                                            @PathParam("namespace") String namespace,
                                            @PathParam("topic") @Encoded String encodedTopic,
                                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                                            @QueryParam("getPreciseBacklog") @DefaultValue("false")
                                                                       boolean getPreciseBacklog) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        validateTopicOwnership(topicName, authoritative);
        validateTopicOperation(topicName, TopicOperation.GET_STATS);
        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats(getPreciseBacklog, false, false);
    }

    @GET
    @Path("{property}/{cluster}/{namespace}/{topic}/internalStats")
    @ApiOperation(hidden = true, value = "Get the internal stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist")})
    public PersistentTopicInternalStats getInternalStats(@PathParam("property") String property,
                                                         @PathParam("cluster") String cluster, @PathParam("namespace")
                                                                     String namespace,
                                                         @PathParam("topic") @Encoded String encodedTopic,
                                                         @QueryParam("authoritative") @DefaultValue("false")
                                                                     boolean authoritative,
                                                         @QueryParam("metadata") @DefaultValue("false")
                                                                     boolean metadata) {
        validateTopicName(property, cluster, namespace, encodedTopic);
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
    @Path("/{property}/{cluster}/{namespace}/{topic}/partitions")
    @ApiOperation(hidden = true, value = "Create a partitioned topic.",
            notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than or equal"
                    + " to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist")})
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("property") String property,
            @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            int numPartitions,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @PUT
    @Path("/{property}/{cluster}/{namespace}/{topic}/unload")
    @ApiOperation(hidden = true, value = "Unload a topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist")})
    public void unloadTopic(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
                            @PathParam("cluster") String cluster, @PathParam("namespace") String namespace,
                            @PathParam("topic") @Encoded String encodedTopic,
                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(property, cluster, namespace, encodedTopic);
            internalUnloadTopic(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist")})
    public void getList(@Suspended final AsyncResponse asyncResponse, @PathParam("property") String property,
                        @PathParam("cluster") String cluster, @PathParam("namespace") String namespace) {
        log.info("[{}] list of topics on namespace {}/{}/{}", clientAppId(), property, cluster, namespace);

        Policies policies = null;
        NamespaceName nsName = null;
        try {
            validateNamespaceOperation(namespaceName, NamespaceOperation.GET_TOPICS);
            policies = getNamespacePolicies(property, cluster, namespace);
            nsName = NamespaceName.get(property, cluster, namespace);

            if (!cluster.equals(Constants.GLOBAL_CLUSTER)) {
                validateClusterOwnership(cluster);
                validateClusterForTenant(property, cluster);
            } else {
                // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                validateGlobalNamespaceOwnership(nsName);
            }
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
            try {
                futures.add(pulsar().getAdminClient().nonPersistentTopics().getListInBundleAsync(nsName.toString(),
                        bundle));
            } catch (PulsarServerException e) {
                log.error("[{}] Failed to get list of topics under namespace {}/{}/{}/{}", clientAppId(), property,
                        cluster, namespace, bundle, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        final List<String> topics = Lists.newArrayList();
        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            for (int i = 0; i < futures.size(); i++) {
                try {
                    if (futures.get(i).isDone() && futures.get(i).get() != null) {
                        topics.addAll(futures.get(i).get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("[{}] Failed to get list of topics under namespace {}/{}/{}", clientAppId(), property,
                            cluster, namespace, e);
                    asyncResponse.resume(new RestException(e instanceof ExecutionException ? e.getCause() : e));
                    return null;
                }
            }
            asyncResponse.resume(topics);
            return null;
        });
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{bundle}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace bundle.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist")})
    public List<String> getListFromBundle(@PathParam("property") String property, @PathParam("cluster") String cluster,
                                          @PathParam("namespace") String namespace,
                                          @PathParam("bundle") String bundleRange) {
        log.info("[{}] list of topics on namespace bundle {}/{}/{}/{}", clientAppId(), property, cluster, namespace,
                bundleRange);
        validateNamespaceOperation(namespaceName, NamespaceOperation.GET_BUNDLE);
        Policies policies = getNamespacePolicies(property, cluster, namespace);
        if (!cluster.equals(Constants.GLOBAL_CLUSTER)) {
            validateClusterOwnership(cluster);
            validateClusterForTenant(property, cluster);
        } else {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(NamespaceName.get(property, cluster, namespace));
        }
        NamespaceName fqnn = NamespaceName.get(property, cluster, namespace);
        try {
            if (!isBundleOwnedByAnyBroker(fqnn, policies.bundles, bundleRange)
                .get(pulsar().getConfig().getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.info("[{}] Namespace bundle is not owned by any broker {}/{}/{}/{}", clientAppId(), property,
                    cluster, namespace, bundleRange);
                return null;
            }
            NamespaceBundle nsBundle = validateNamespaceBundleOwnership(fqnn, policies.bundles, bundleRange,
                true, true);
            final List<String> topicList = Lists.newArrayList();
            pulsar().getBrokerService().forEachTopic(topic -> {
                TopicName topicName = TopicName.get(topic.getName());
                if (nsBundle.includes(topicName)) {
                    topicList.add(topic.getName());
                }
            });
            return topicList;
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) {
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), fqnn.toString(), bundleRange, e);
            throw new RestException(e);
        }
    }

    private Topic getTopicReference(TopicName topicName) {
        try {
            return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                    .get(config().getZooKeeperOperationTimeoutSeconds(), TimeUnit.SECONDS)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Topic not found"));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } catch (InterruptedException | TimeoutException e) {
            throw new RestException(e);
        }
    }
}
