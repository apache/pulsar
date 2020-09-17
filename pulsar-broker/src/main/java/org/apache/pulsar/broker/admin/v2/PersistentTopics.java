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

import static org.apache.pulsar.common.util.Codec.decode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic")
public class PersistentTopics extends PersistentTopicsBase {

    @GET
    @Path("/{tenant}/{namespace}")
    @ApiOperation(value = "Get the list of topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void getList(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace) {
        try {
            validateNamespaceName(tenant, namespace);
            asyncResponse.resume(internalGetList());
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/partitioned")
    @ApiOperation(value = "Get the list of partitioned topics under a namespace.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public List<String> getPartitionedTopicList(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace) {
        validateNamespaceName(tenant, namespace);
        return internalGetPartitionedTopicList();
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/permissions")
    @ApiOperation(value = "Get permissions on a topic.", notes = "Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the"
            + "namespace level combined (union) with any eventual specific permission set on the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public Map<String, Set<AuthAction>> getPermissionsOnTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetPermissionsOnTopic();
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Grant a new permission to a role on a single topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void grantPermissionsOnTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Client role to which grant permissions", required = true)
            @PathParam("role") String role,
            @ApiParam(value = "Actions to be granted (produce,functions,consume)", allowableValues = "produce,functions,consume")
            Set<AuthAction> actions) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGrantPermissionsOnTopic(role, actions);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/permissions/{role}")
    @ApiOperation(value = "Revoke permissions on a topic.", notes = "Revoke permissions to a role on a single topic. If the permission was not set at the topic"
            + "level, but rather at the namespace level, this operation will return an error (HTTP status code 412).")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Permissions are not set at the topic level"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void revokePermissionsOnTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Client role to which grant permissions", required = true)
            @PathParam("role") String role) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRevokePermissionsOnTopic(role);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
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
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            validateAdminAccessForTenant(topicName.getTenant());
            internalCreatePartitionedTopic(asyncResponse, numPartitions);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value="Create a non-partitioned topic.", notes = "This is the only REST endpoint from which non-partitioned topics could be created.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist"),
            @ApiResponse(code = 412, message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void createNonPartitionedTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateGlobalNamespaceOwnership(tenant,namespace);
        validateTopicName(tenant, namespace, encodedTopic);
        internalCreateNonPartitionedTopic(authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Get offload policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"),})
    public void getOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isOffloadPoliciesSet()) {
            asyncResponse.resume(topicPolicies.getOffloadPolicies());
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Set offload policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void setOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic,
                                                    @ApiParam(value = "Offload policies for the specified topic")
                                                            OffloadPolicies offloadPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenant(tenant);
        validatePoliciesReadOnlyAccess();
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        internalSetOffloadPolicies(offloadPolicies).whenComplete((res, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed set offloadPolicies", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed set offloadPolicies", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @ApiOperation(value = "Delete offload policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void removeOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
                                      @PathParam("tenant") String tenant,
                                      @PathParam("namespace") String namespace,
                                      @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setOffloadPolicies(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Get max unacked messages per consumer config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"),})
    public void getMaxUnackedMessagesOnConsumer(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isMaxUnackedMessagesOnConsumerSet()) {
            asyncResponse.resume(topicPolicies.getMaxUnackedMessagesOnConsumer());
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Set max unacked messages per consumer config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void setMaxUnackedMessagesOnConsumer(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic,
                                                    @ApiParam(value = "Max unacked messages on consumer policies for the specified topic")
                                                            Integer maxUnackedNum) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenant(tenant);
        validatePoliciesReadOnlyAccess();
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        internalSetMaxUnackedMessagesOnConsumer(maxUnackedNum).whenComplete((res, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed set MaxUnackedMessagesOnConsumer", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed set MaxUnackedMessagesOnConsumer", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnConsumer")
    @ApiOperation(value = "Delete max unacked messages per consumer config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void deleteMaxUnackedMessagesOnConsumer(@Suspended final AsyncResponse asyncResponse,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace,
                                                       @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setMaxUnackedMessagesOnConsumer(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Get inactive topic policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"),})
    public void getInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
                                         @PathParam("tenant") String tenant,
                                         @PathParam("namespace") String namespace,
                                         @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isInactiveTopicPoliciesSet()) {
            asyncResponse.resume(topicPolicies.getInactiveTopicPolicies());
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Set inactive topic policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void setInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
                                                @PathParam("tenant") String tenant,
                                                @PathParam("namespace") String namespace,
                                                @PathParam("topic") @Encoded String encodedTopic,
                                                @ApiParam(value = "inactive topic policies for the specified topic")
                                                        InactiveTopicPolicies inactiveTopicPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenant(tenant);
        validatePoliciesReadOnlyAccess();
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        internalSetInactiveTopicPolicies(inactiveTopicPolicies).whenComplete((res, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed set InactiveTopicPolicies", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed set InactiveTopicPolicies", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/inactiveTopicPolicies")
    @ApiOperation(value = "Delete inactive topic policies on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void deleteInactiveTopicPolicies(@Suspended final AsyncResponse asyncResponse,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace,
                                                       @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setInactiveTopicPolicies(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Get max unacked messages per subscription config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"),})
    public void getMaxUnackedMessagesOnSubscription(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isMaxUnackedMessagesOnSubscriptionSet()) {
            asyncResponse.resume(topicPolicies.getMaxUnackedMessagesOnSubscription());
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Set max unacked messages per subscription config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void setMaxUnackedMessagesOnSubscription(@Suspended final AsyncResponse asyncResponse,
                                                    @PathParam("tenant") String tenant,
                                                    @PathParam("namespace") String namespace,
                                                    @PathParam("topic") @Encoded String encodedTopic,
                                                    @ApiParam(value = "Max unacked messages on subscription policies for the specified topic")
                                                            Integer maxUnackedNum) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenant(tenant);
        validatePoliciesReadOnlyAccess();
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        internalSetMaxUnackedMessagesOnSubscription(maxUnackedNum).whenComplete((res, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed set MaxUnackedMessagesOnSubscription", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed set MaxUnackedMessagesOnSubscription", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }



    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxUnackedMessagesOnSubscription")
    @ApiOperation(value = "Delete max unacked messages per subscription config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void deleteMaxUnackedMessagesOnSubscription(@Suspended final AsyncResponse asyncResponse,
                                                       @PathParam("tenant") String tenant,
                                                       @PathParam("namespace") String namespace,
                                                       @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setMaxUnackedMessagesOnSubscription(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Get delayed delivery messages config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 500, message = "Internal server error"),})
    public void getDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isDelayedDeliveryEnabledSet() && topicPolicies.isDelayedDeliveryTickTimeMillisSet()) {
            asyncResponse.resume(new DelayedDeliveryPolicies(topicPolicies.getDelayedDeliveryTickTimeMillis()
                    , topicPolicies.getDelayedDeliveryEnabled()));
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void setDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("topic") @Encoded String encodedTopic,
                                           @ApiParam(value = "Delayed delivery policies for the specified topic") DelayedDeliveryPolicies deliveryPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenant(tenant);
        validatePoliciesReadOnlyAccess();
        checkTopicLevelPolicyEnable();
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        internalSetDelayedDeliveryPolicies(asyncResponse, deliveryPolicies);
    }



    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @ApiOperation(value = "Set delayed delivery messages config on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),})
    public void deleteDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
                                              @PathParam("tenant") String tenant,
                                              @PathParam("namespace") String namespace,
                                              @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setDelayedDeliveryPolicies(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    /**
     * It updates number of partitions of an existing non-global partitioned topic. It requires partitioned-topic to be
     * already exist and number of new partitions must be greater than existing number of partitions. Decrementing
     * number of partitions requires deletion of topic which is not supported.
     *
     * Already created partitioned producers and consumers can't see newly created partitions and it requires to
     * recreate them at application so, newly created producers and consumers can connect to newly added partitions as
     * well. Therefore, it can violate partition ordering at producers until all producers are restarted at application.
     *
     * @param tenant
     * @param namespace
     * @param encodedTopic
     * @param numPartitions
     */
    @POST
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Increment partitions of an existing partitioned topic.", notes = "It only increments partitions of existing non-global partitioned-topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to adminisActions to be grantedtrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void updatePartitionedTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("updateLocalTopicOnly") @DefaultValue("false") boolean updateLocalTopicOnly,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "The number of partitions for the topic", required = true, type = "int", defaultValue = "0")
            int numPartitions) {
        validatePartitionedTopicName(tenant, namespace, encodedTopic);
        validatePartitionedTopicMetadata(tenant, namespace, encodedTopic);
        internalUpdatePartitionedTopic(numPartitions, updateLocalTopicOnly, authoritative);
    }


    @POST
    @Path("/{tenant}/{namespace}/{topic}/createMissedPartitions")
    @ApiOperation(value = "Create missed partitions of an existing partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to adminisActions to be grantedtrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void createMissedPartitions(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic) {

        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            internalCreateMissedPartitions(asyncResponse);
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
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
        return internalGetPartitionedMetadata(authoritative, checkAllowAutoCreation);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Delete a partitioned topic.", notes = "It will also delete all the partitions of the topic if it exists.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Partitioned topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Partitioned topic name is invalid"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void deletePartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Stop all producer/consumer/replicator and delete topic forcefully", defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            internalDeletePartitionedTopic(asyncResponse, authoritative, force);
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
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Topic name is not valid or can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
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

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Delete a topic.", notes = "The topic cannot be deleted if delete is not forcefully and there's any active "
            + "subscription or producer connected to the it. Force delete ignores connected clients and deletes topic by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic has active producers/subscriptions"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void deleteTopic(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Stop all producer/consumer/replicator and delete topic forcefully", defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalDeleteTopic(authoritative, force);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscriptions")
    @ApiOperation(value = "Get the list of persistent subscriptions for a given topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration"),
    })
    public void getSubscriptions(
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
            internalGetSubscriptions(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public TopicStats getStats(
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
        return internalGetStats(authoritative, getPreciseBacklog);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
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
        return internalGetInternalStats(authoritative);
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/internal-info")
    @ApiOperation(value = "Get the stored topic metadata.")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getManagedLedgerInfo(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic")
            @Encoded String encodedTopic,@Suspended AsyncResponse asyncResponse) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalGetManagedLedgerInfo(asyncResponse, authoritative);
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
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(value = "Is return precise backlog or imprecise backlog")
            @QueryParam("getPreciseBacklog") @DefaultValue("false") boolean getPreciseBacklog) {
        try {
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            internalGetPartitionedStats(asyncResponse, authoritative, perPartition, getPreciseBacklog);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/partitioned-internalStats")
    @ApiOperation(hidden = true, value = "Get the stats-internal for the partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getPartitionedStatsInternal(
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
            internalGetPartitionedStatsInternal(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}")
    @ApiOperation(value = "Delete a subscription.", notes = "The subscription cannot be deleted if delete is not forcefully and there are any active consumers attached to it. "
            + "Force delete ignores connected consumers and deletes subscription by explicitly closing them.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 412, message = "Subscription has active consumers"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void deleteSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to be deleted")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Disconnect and close all consumers and delete subscription forcefully", defaultValue = "false", type = "boolean")
            @QueryParam("force") @DefaultValue("false") boolean force,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalDeleteSubscription(asyncResponse, decode(encodedSubName), authoritative, force);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip_all")
    @ApiOperation(value = "Skip all messages on a topic subscription.", notes = "Completely clears the backlog on the subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Operation not allowed on non-persistent topic"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void skipAllMessages(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalSkipAllMessages(asyncResponse, decode(encodedSubName), authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/skip/{numMessages}")
    @ApiOperation(value = "Skipping messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a partitioned topic is not allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void skipMessages(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Name of subscription")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "The number of messages to skip", defaultValue = "0")
            @PathParam("numMessages") int numMessages,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSkipMessages(decode(encodedSubName), numMessages, authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expiry messages on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Expiry messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void expireTopicMessages(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to be Expiry messages on")
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Expires beyond the specified number of seconds", defaultValue = "0")
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalExpireMessages(asyncResponse, decode(encodedSubName), expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/all_subscription/expireMessages/{expireTimeInSeconds}")
    @ApiOperation(value = "Expiry messages on all subscriptions of topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic or subscription does not exist"),
            @ApiResponse(code = 405, message = "Expiry messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Can't find owner for topic"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void expireMessagesForAllSubscriptions(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Expires beyond the specified number of seconds", defaultValue = "0")
            @PathParam("expireTimeInSeconds") int expireTimeInSeconds,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalExpireMessagesForAllSubscriptions(asyncResponse, expireTimeInSeconds, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subscriptionName}")
    @ApiOperation(value = "Create a subscription on the topic.", notes = "Creates a subscription on the topic at the specified message id")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 400, message = "Create subscription on non persistent topic is not supported"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void createSubscription(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String topic,
            @ApiParam(value = "Subscription to create position on", required = true)
            @PathParam("subscriptionName") String encodedSubName,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(name = "messageId", value = "messageId where to create the subscription. " +
                    "It can be 'latest', 'earliest' or (ledgerId:entryId)",
                    defaultValue = "latest",
                    allowableValues = "latest,earliest,ledgerId:entryId"
            )
            MessageIdImpl messageId,
            @ApiParam(value = "Is replicated required to perform this operation")
            @QueryParam("replicated") boolean replicated
            ) {
        try {
            validateTopicName(tenant, namespace, topic);
            if (!topicName.isPersistent()) {
                throw new RestException(Response.Status.BAD_REQUEST, "Create subscription on non-persistent topic" +
                        "can only be done through client");
            }
            internalCreateSubscription(asyncResponse, decode(encodedSubName), messageId, authoritative, replicated);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor/{timestamp}")
    @ApiOperation(value = "Reset subscription to message position closest to absolute timestamp (in ms).", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Method Not Allowed"),
            @ApiResponse(code = 412, message = "Failed to reset cursor on subscription or " +
                    "Unable to find position for timestamp specified"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
            })
    public void resetCursor(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Subscription to reset position on", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "time in minutes to reset back to (or minutes, hours,days,weeks eg:100m, 3h, 2d, 5w)")
            @PathParam("timestamp") long timestamp,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalResetCursor(asyncResponse, decode(encodedSubName), timestamp, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/resetcursor")
    @ApiOperation(value = "Reset subscription to message position closest to given position.", notes = "It fence cursor and disconnects all active consumers before reseting cursor.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic/Subscription does not exist"),
            @ApiResponse(code = 405, message = "Not supported for partitioned topics"),
            @ApiResponse(code = 412, message = "Unable to find position for position specified"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void resetCursorOnPosition(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(name = "subName", value = "Subscription to reset position on", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @ApiParam(name = "messageId", value = "messageId to reset back to (ledgerId:entryId)")
            MessageIdImpl messageId) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalResetCursorOnPosition(asyncResponse, decode(encodedSubName), authoritative, messageId);
        } catch (Exception e) {
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/subscription/{subName}/position/{messagePosition}")
    @ApiOperation(value = "Peek nth message on a topic subscription.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic, subscription or the message position does not exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public Response peekNthMessage(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(name = "subName", value = "Subscribed message expired", required = true)
            @PathParam("subName") String encodedSubName,
            @ApiParam(value = "The number of messages (default 1)", defaultValue = "1")
            @PathParam("messagePosition") int messagePosition,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalPeekNthMessage(decode(encodedSubName), messagePosition, authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/ledger/{ledgerId}/entry/{entryId}")
    @ApiOperation(value = "Get message by its messageId.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic, subscription or the message position does not exist"),
            @ApiResponse(code = 405, message = "Skipping messages on a non-persistent topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")})
    public void getMessageById(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The ledger id", required = true)
            @PathParam("ledgerId") long ledgerId,
            @ApiParam(value = "The entry id", required = true)
            @PathParam("entryId") long entryId,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        try {
            validateTopicName(tenant, namespace, encodedTopic);
            internalGetMessageById(asyncResponse, ledgerId, entryId, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("{tenant}/{namespace}/{topic}/backlog")
    @ApiOperation(value = "Get estimated backlog for offline topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Namespace does not exist"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public PersistentOfflineTopicStats getBacklog(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalGetBacklog(authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/backlogQuotaMap")
    @ApiOperation(value = "Get backlog quota map on a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic policy does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        return getTopicPolicies(topicName)
                .map(TopicPolicies::getBackLogQuotaMap)
                .map(map -> {
                    HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> hashMap = Maps.newHashMap();
                    map.forEach((key,value) -> {
                        hashMap.put(BacklogQuota.BacklogQuotaType.valueOf(key),value);
                    });
                    return hashMap;
                })
                .orElse(Maps.newHashMap());
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/backlogQuota")
    @ApiOperation(value = "Set a backlog quota for a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Specified backlog quota exceeds retention quota. Increase retention quota and retry request") })
    public void setBacklogQuota(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("backlogQuotaType") BacklogQuota.BacklogQuotaType backlogQuotaType, BacklogQuota backlogQuota) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetBacklogQuota(asyncResponse, backlogQuotaType, backlogQuota);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/backlogQuota")
    @ApiOperation(value = "Remove a backlog quota policy from a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeBacklogQuota(@Suspended final AsyncResponse asyncResponse, @PathParam("tenant") String tenant, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("backlogQuotaType") BacklogQuota.BacklogQuotaType backlogQuotaType) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveBacklogQuota(asyncResponse, backlogQuotaType);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Get message TTL in seconds for a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
                            @ApiResponse(code = 404, message = "Topic does not exist"),
                            @ApiResponse(code = 405, message = "Topic level policy is disabled, enable the topic level policy and retry")})
    public int getMessageTTL(@PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace,
                             @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        return getTopicPolicies(topicName)
                .map(TopicPolicies::getMessageTTLInSeconds)
                .orElse(0);  //same as default ttl at namespace level
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Set message TTL in seconds for a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Not authenticate to perform the request or policy is read only"),
                            @ApiResponse(code = 404, message = "Topic does not exist"),
                            @ApiResponse(code = 405, message = "Topic level policy is disabled, enable the topic level policy and retry"),
                            @ApiResponse(code = 412, message = "Invalid message TTL value") })
    public void setMessageTTL(@Suspended final AsyncResponse asyncResponse,
                              @PathParam("tenant") String tenant,
                              @PathParam("namespace") String namespace,
                              @PathParam("topic") @Encoded String encodedTopic,
                              @ApiParam(value = "TTL in seconds for the specified namespace", required = true)
                              @QueryParam("messageTTL") int messageTTL) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetMessageTTL(asyncResponse, messageTTL);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @ApiOperation(value = "Remove message TTL in seconds for a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Not authenticate to perform the request or policy is read only"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(code = 412, message = "Invalid message TTL value") })
    public void removeMessageTTL(@Suspended final AsyncResponse asyncResponse,
                              @PathParam("tenant") String tenant,
                              @PathParam("namespace") String namespace,
                              @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetMessageTTL(asyncResponse, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Get deduplication configuration of a topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public void getDeduplicationEnabled(@Suspended final AsyncResponse asyncResponse,
                             @PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace,
                             @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        TopicPolicies topicPolicies = getTopicPolicies(topicName).orElse(new TopicPolicies());
        if (topicPolicies.isDeduplicationSet()) {
            asyncResponse.resume(topicPolicies.getDeduplicationEnabled());
        } else {
            asyncResponse.resume(Response.noContent().build());
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Set deduplication enabled on a topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry")})
    public void setDeduplicationEnabled(@Suspended final AsyncResponse asyncResponse,
                             @PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace,
                             @PathParam("topic") @Encoded String encodedTopic,
                             @ApiParam(value = "DeduplicationEnabled policies for the specified topic") Boolean enabled) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetDeduplicationEnabled(enabled).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed updated deduplication", ex);
                asyncResponse.resume(ex);
            }else if (ex != null) {
                log.error("Failed updated deduplication", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/deduplicationEnabled")
    @ApiOperation(value = "Remove deduplication configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void removeDeduplicationEnabled(@Suspended final AsyncResponse asyncResponse,
                                           @PathParam("tenant") String tenant,
                                           @PathParam("namespace") String namespace,
                                           @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        setDeduplicationEnabled(asyncResponse, tenant, namespace, encodedTopic, null);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Get retention configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification") })
    public void getRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            internalGetRetention(asyncResponse);
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Set retention configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void setRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Retention policies for the specified namespace") RetentionPolicies retention) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetRetention(retention).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed updated retention", ex);
                asyncResponse.resume(ex);
            }else if (ex != null) {
                log.error("Failed updated retention", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully updated retention: namespace={}, topic={}, retention={}",
                            clientAppId(),
                            namespaceName,
                            topicName.getLocalName(),
                            jsonMapper().writeValueAsString(retention));
                } catch (JsonProcessingException ignore) { }
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @ApiOperation(value = "Remove retention configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Retention Quota must exceed backlog quota") })
    public void removeRetention(@Suspended final AsyncResponse asyncResponse,
                             @PathParam("tenant") String tenant,
                             @PathParam("namespace") String namespace,
                             @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveRetention().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed updated retention", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove retention: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(value = "Get configuration of persistence policies for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPersistence(@Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenant") String tenant,
                               @PathParam("namespace") String namespace,
                               @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<PersistencePolicies> persistencePolicies = internalGetPersistence();
            if (!persistencePolicies.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(persistencePolicies.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(value = "Set configuration of persistence policies for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 400, message = "Invalid persistence policies")})
    public void setPersistence(@Suspended final AsyncResponse asyncResponse,
                               @PathParam("tenant") String tenant,
                               @PathParam("namespace") String namespace,
                               @PathParam("topic") @Encoded String encodedTopic,
                               @ApiParam(value = "Bookkeeper persistence policies for specified topic") PersistencePolicies persistencePolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetPersistence(persistencePolicies).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully updated persistence policies: namespace={}, topic={}, persistencePolicies={}",
                            clientAppId(),
                            namespaceName,
                            topicName.getLocalName(),
                            jsonMapper().writeValueAsString(persistencePolicies));
                } catch (JsonProcessingException ignore) {
                }
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @ApiOperation(value = "Remove configuration of persistence policies for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removePersistence(@Suspended final AsyncResponse asyncResponse,
                                  @PathParam("tenant") String tenant,
                                  @PathParam("namespace") String namespace,
                                  @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemovePersistence().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed updated retention", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove persistence policies: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Get maxProducers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxProducers(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<Integer> maxProducers = internalGetMaxProducers();
            if (!maxProducers.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(maxProducers.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Set maxProducers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxProducers")})
    public void setMaxProducers(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @ApiParam(value = "The max producers of the topic") int maxProducers) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetMaxProducers(maxProducers).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully updated max producers: namespace={}, topic={}, maxProducers={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName(),
                        maxProducers);
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxProducers")
    @ApiOperation(value = "Remove maxProducers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxProducers(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveMaxProducers().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove maxProducers", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove max producers: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Get maxConsumers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxConsumers(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<Integer> maxConsumers = internalGetMaxConsumers();
            if (!maxConsumers.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(maxConsumers.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Set maxConsumers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification"),
            @ApiResponse(code = 412, message = "Invalid value of maxConsumers")})
    public void setMaxConsumers(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @ApiParam(value = "The max consumers of the topic") int maxConsumers) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetMaxConsumers(maxConsumers).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed updated persistence policies", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully updated max consumers: namespace={}, topic={}, maxConsumers={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName(),
                        maxConsumers);
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxConsumers")
    @ApiOperation(value = "Remove maxConsumers config for specified topic.")
    @ApiResponses(value = {@ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxConsumers(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveMaxConsumers().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove maxConsumers", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove max consumers: namespace={}, topic={}",
                        clientAppId(),
                        namespaceName,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }


    @POST
    @Path("/{tenant}/{namespace}/{topic}/terminate")
    @ApiOperation(value = "Terminate a topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Termination of a partitioned topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public MessageId terminate(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalTerminate(authoritative);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/terminate/partitions")
    @ApiOperation(value = "Terminate all partitioned topic. A topic that is terminated will not accept any more "
            + "messages to be published and will let consumer to drain existing messages in backlog")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Termination of a partitioned topic is not allowed"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void terminatePartitionedTopic( @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalTerminatePartitionedTopic(asyncResponse, authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Trigger a compaction operation on a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 409, message = "Compaction already running"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void compact(
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
            internalTriggerCompaction(asyncResponse, authoritative);
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/compaction")
    @ApiOperation(value = "Get the status of a compaction operation for a topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist, or compaction hasn't run"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public LongRunningProcessStatus compactionStatus(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalCompactionStatus(authoritative);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 409, message = "Offload already running"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void triggerOffload(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               MessageIdImpl messageId) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalTriggerOffload(authoritative, messageId);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offload")
    @ApiOperation(value = "Offload a prefix of a topic to long term storage")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public OffloadProcessStatus offloadStatus(
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "Is authentication required to perform this operation")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        return internalOffloadStatus(authoritative);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/lastMessageId")
    @ApiOperation(value = "Return the last commit message id of topic")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant or" +
                    "subscriber is not authorized to access this operation"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Operation is not allowed on the persistent topic"),
            @ApiResponse(code = 412, message = "Topic name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration") })
    public void getLastMessageId(
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
            internalGetLastMessageId(asyncResponse, authoritative);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Get dispatch rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getDispatchRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<DispatchRate> dispatchRate = internalGetDispatchRate();
            if (!dispatchRate.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(dispatchRate.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Set message dispatch rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setDispatchRate(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @ApiParam(value = "Dispatch rate for the specified topic") DispatchRate dispatchRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetDispatchRate(dispatchRate).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed to set topic dispatch rate", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed to set topic dispatch rate");
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully set topic dispatch rate: tenant={}, namespace={}, topic={}, dispatchRate={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        jsonMapper().writeValueAsString(dispatchRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/dispatchRate")
    @ApiOperation(value = "Remove message dispatch rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
        @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeDispatchRate(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveDispatchRate().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove topic dispatch rate", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove topic dispatch rate: tenant={}, namespace={}, topic={}",
                    clientAppId(),
                    tenant,
                    namespace,
                    topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Get compaction threshold configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
        @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
                                       @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<Long> compactionThreshold = internalGetCompactionThreshold();
            if (!compactionThreshold.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(compactionThreshold.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Set compaction threshold configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
        @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @ApiParam(value = "Dispatch rate for the specified topic") long compactionThreshold) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetCompactionThreshold(compactionThreshold).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed to set topic dispatch rate", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed to set topic dispatch rate");
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully set topic compaction threshold: tenant={}, namespace={}, topic={}, compactionThreshold={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        jsonMapper().writeValueAsString(compactionThreshold));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/compactionThreshold")
    @ApiOperation(value = "Remove compaction threshold configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
        @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeCompactionThreshold(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveCompactionThreshold().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove topic dispatch rate", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove topic compaction threshold: tenant={}, namespace={}, topic={}",
                    clientAppId(),
                    tenant,
                    namespace,
                    topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(value = "Get max consumers per subscription configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getMaxConsumersPerSubscription(@Suspended final AsyncResponse asyncResponse,
                                       @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<Integer> maxConsumersPerSubscription = internalGetMaxConsumersPerSubscription();
            if (!maxConsumersPerSubscription.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(maxConsumersPerSubscription.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(value = "Set max consumers per subscription configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setMaxConsumersPerSubscription(@Suspended final AsyncResponse asyncResponse,
                                       @PathParam("tenant") String tenant,
                                       @PathParam("namespace") String namespace,
                                       @PathParam("topic") @Encoded String encodedTopic,
                                       @ApiParam(value = "Dispatch rate for the specified topic") int maxConsumersPerSubscription) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetMaxConsumersPerSubscription(maxConsumersPerSubscription).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed to set topic {} max consumers per subscription ", topicName.getLocalName(), ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed to set topic max consumers per subscription");
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully set topic max consumers per subscription: tenant={}, namespace={}, topic={}, maxConsumersPerSubscription={}",
                            clientAppId(),
                            tenant,
                            namespace,
                            topicName.getLocalName(),
                            jsonMapper().writeValueAsString(maxConsumersPerSubscription));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxConsumersPerSubscription")
    @ApiOperation(value = "Remove max consumers per subscription configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removeMaxConsumersPerSubscription(@Suspended final AsyncResponse asyncResponse,
                                          @PathParam("tenant") String tenant,
                                          @PathParam("namespace") String namespace,
                                          @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemoveMaxConsumersPerSubscription().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove topic {} max consuners per subscription", topicName.getLocalName(), ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove topic max consumers per subscription: tenant={}, namespace={}, topic={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Get publish rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void getPublishRate(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        try {
            Optional<PublishRate> publishRate = internalGetPublishRate();
            if (!publishRate.isPresent()) {
                asyncResponse.resume(Response.noContent().build());
            } else {
                asyncResponse.resume(publishRate.get());
            }
        } catch (RestException e) {
            asyncResponse.resume(e);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
        }
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Set message publish rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
            @ApiResponse(code = 404, message = "Topic does not exist"),
            @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
            @ApiResponse(code = 409, message = "Concurrent modification")})
    public void setPublishRate(@Suspended final AsyncResponse asyncResponse,
                                @PathParam("tenant") String tenant,
                                @PathParam("namespace") String namespace,
                                @PathParam("topic") @Encoded String encodedTopic,
                                @ApiParam(value = "Dispatch rate for the specified topic") PublishRate publishRate) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalSetPublishRate(publishRate).whenComplete((r, ex) -> {
            if (ex instanceof RestException) {
                log.error("Failed to set topic dispatch rate", ex);
                asyncResponse.resume(ex);
            } else if (ex != null) {
                log.error("Failed to set topic dispatch rate");
                asyncResponse.resume(new RestException(ex));
            } else {
                try {
                    log.info("[{}] Successfully set topic publish rate: tenant={}, namespace={}, topic={}, publishRate={}",
                        clientAppId(),
                        tenant,
                        namespace,
                        topicName.getLocalName(),
                        jsonMapper().writeValueAsString(publishRate));
                } catch (JsonProcessingException ignore) {}
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/publishRate")
    @ApiOperation(value = "Remove message publish rate configuration for specified topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Topic does not exist"),
        @ApiResponse(code = 404, message = "Topic does not exist"),
        @ApiResponse(code = 405, message = "Topic level policy is disabled, please enable the topic level policy and retry"),
        @ApiResponse(code = 409, message = "Concurrent modification")})
    public void removePublishRate(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("tenant") String tenant,
                                   @PathParam("namespace") String namespace,
                                   @PathParam("topic") @Encoded String encodedTopic) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalRemovePublishRate().whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("Failed to remove topic publish rate", ex);
                asyncResponse.resume(new RestException(ex));
            } else {
                log.info("[{}] Successfully remove topic publish rate: tenant={}, namespace={}, topic={}",
                    clientAppId(),
                    tenant,
                    namespace,
                    topicName.getLocalName());
                asyncResponse.resume(Response.noContent().build());
            }
        });
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopics.class);
}
