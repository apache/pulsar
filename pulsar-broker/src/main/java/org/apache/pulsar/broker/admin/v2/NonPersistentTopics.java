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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.zookeeper.KeeperException;
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
    public PartitionedTopicMetadata getPartitionedMetadata(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is check configuration required to automatically create topic")
            @QueryParam("checkAllowAutoCreation") @DefaultValue("false") boolean checkAllowAutoCreation) {
        validateTopicName(tenant, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative, checkAllowAutoCreation);
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
            @ApiParam(value = "Is return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats(getPreciseBacklog);
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
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getInternalStats();
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "The tenant/namespace does not exist"),
            @ApiResponse(code = 409, message = "Partitioned topic already exists"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
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
            @ApiParam(value = "The number of partitions for the topic", required = true, type = "int", defaultValue = "0")
            int numPartitions) {

        try {
            validateGlobalNamespaceOwnership(tenant,namespace);
            validateTopicName(tenant, namespace, encodedTopic);
            validateAdminAccessForTenant(topicName.getTenant());
            internalCreatePartitionedTopic(asyncResponse, numPartitions);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
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
            @ApiParam(value = "Is authentication required to perform this operation")
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
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace.", response = String.class, responseContainer = "List")
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
            @PathParam("namespace") String namespace) {
        Policies policies = null;
        try {
            validateNamespaceName(tenant, namespace);
            if (log.isDebugEnabled()) {
                log.debug("[{}] list of topics on namespace {}", clientAppId(), namespaceName);
            }
            validateAdminAccessForTenant(tenant);
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
            try {
                futures.add(pulsar().getAdminClient().topics().getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
                log.error("[{}] Failed to get list of topics under namespace {}/{}", clientAppId(), namespaceName,
                        bundle, e);
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
                    log.error("[{}] Failed to get list of topics under namespace {}", clientAppId(), namespaceName, e);
                    asyncResponse.resume(new RestException(e instanceof ExecutionException ? e.getCause() : e));
                    return null;
                }
            }

            final List<String> nonPersistentTopics =
                topics.stream()
                      .filter(name -> !TopicName.get(name).isPersistent())
                      .collect(Collectors.toList());
            asyncResponse.resume(nonPersistentTopics);
            return null;
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{bundle}")
    @ApiOperation(value = "Get the list of non-persistent topics under a namespace bundle.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to manage resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Namespace doesn't exist"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public List<String> getListFromBundle(
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

        validateAdminAccessForTenant(tenant);
        Policies policies = getNamespacePolicies(namespaceName);

        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
        validateGlobalNamespaceOwnership(namespaceName);

        if (!isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange)) {
            log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(), namespaceName,
                    bundleRange);
            return null;
        }

        NamespaceBundle nsBundle = validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange, true, true);
        try {
            final List<String> topicList = Lists.newArrayList();
            pulsar().getBrokerService().forEachTopic(topic -> {
                TopicName topicName = TopicName.get(topic.getName());
                if (nsBundle.includes(topicName)) {
                    topicList.add(topic.getName());
                }
            });
            return topicList;
        } catch (Exception e) {
            log.error("[{}] Failed to unload namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange, e);
            throw new RestException(e);
        }
    }


    protected void validateAdminOperationOnTopic(TopicName topicName, boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    private Topic getTopicReference(TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString()).join()
                .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Topic not found"));
    }
}
