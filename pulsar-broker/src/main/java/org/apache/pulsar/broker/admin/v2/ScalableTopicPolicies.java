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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
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
import jakarta.ws.rs.HttpMethod;
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
import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Topic-level policies of scalable topics ({@code topic://} domain).
 *
 * <p>A policy set on a scalable topic is applied by every one of its segments, so only the policies that act per
 * message or per ledger are supported: {@link #SUPPORTED_POLICIES}. Their per-segment meaning equals their
 * whole-topic meaning, except for the retention size, which each segment counts on its own: a topic with N
 * segments may retain up to N times the size. Retention is a floor, so this only keeps more data. Every other
 * topic policy, and every other topic operation, under this path is rejected with 405. The endpoints mirror
 * those of {@link PersistentTopics} and delegate to the same {@link PersistentTopicsBase} implementations.
 */
@Path("/topic")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "scalable topic policies", description = "Topic-level policies of scalable topics")
public class ScalableTopicPolicies extends PersistentTopicsBase {

    static final List<String> SUPPORTED_POLICIES = List.of("retention", "messageTTL", "persistence",
            "maxMessageSize", "delayedDelivery", "offloadPolicies");

    @Override
    protected String domain() {
        return TopicDomain.topic.value();
    }

    /**
     * Checks, in the order {@link PersistentTopicsBase} does for a regular topic, that topic-level policies are
     * enabled, that the name is not a partition name (the policies service keys policies by the partitioned topic
     * name), the global namespace ownership, and that the scalable topic exists and is not being deleted. Then
     * redirects to the broker owning the bundle of the {@code topic://} name, like {@link PersistentTopicsBase}
     * does for the name of a regular topic.
     */
    @Override
    protected CompletableFuture<Void> preValidation(boolean authoritative) {
        if (!config().isTopicLevelPoliciesEnabled()) {
            return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Topic level policies is disabled, to enable the topic level policy and retry."));
        }
        if (topicName.isPartitioned()) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Not allowed to set/get topic policy for a partition"));
        }
        return validateGlobalNamespaceOwnershipAsync(namespaceName)
                .thenCompose(__ -> getPulsarResources().getScalableTopicResources()
                        .getScalableTopicMetadataAsync(topicName))
                .thenCompose(metadata -> {
                    if (metadata.isEmpty() || metadata.get().isDeleted()) {
                        throw new RestException(Status.NOT_FOUND, "Scalable topic not found: " + topicName);
                    }
                    return validateTopicOwnershipAsync(topicName, authoritative);
                });
    }

    // --- Retention ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @Operation(summary = "Get retention configuration for specified topic.")
    @ApiResponses(value = {@ApiResponse(
            responseCode = "200",
            description = "Get retention configuration for specified topic.",
            content = @Content(schema = @Schema(implementation = RetentionPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void getRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetRetention(applied, isGlobal))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getRetention", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @Operation(summary = "Set retention configuration for specified topic.",
            description = "On a scalable topic, retentionSizeInMB applies to each segment, so the total retained"
                    + " size can reach N times the value for a topic with N segments. The retention time also"
                    + " drives the garbage collection of sealed segments.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405", description =
                    "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "Retention Quota must exceed backlog quota")})
    public void setRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @RequestBody(description = "Retention policies for the specified topic") RetentionPolicies retention) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetRetention(retention, isGlobal))
            .thenRun(() -> {
                try {
                    log.info()
                            .attr("namespace", namespaceName)
                            .attr("topic", topicName.getLocalName())
                            .attr("retention", objectWriter().writeValueAsString(retention))
                            .log("Successfully updated retention");
                } catch (JsonProcessingException ignore) {
                }
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setRetention", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/retention")
    @Operation(summary = "Remove retention configuration for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "Retention Quota must exceed backlog quota")})
    public void removeRetention(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.RETENTION, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemoveRetention(isGlobal))
            .thenRun(() -> {
                log.info()
                        .attr("namespace", namespaceName)
                        .attr("topic", topicName.getLocalName())
                        .log("Successfully remove retention");
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeRetention", ex, asyncResponse);
                return null;
            });
    }

    // --- Message TTL ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @Operation(summary = "Get message TTL in seconds for a topic")
    @ApiResponses(value = {@ApiResponse(
            responseCode = "200",
            description = "Get message TTL in seconds for a topic",
            content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405", description =
                    "Topic level policy is disabled, enable the topic level policy and retry")})
    public void getMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMessageTTL(applied, isGlobal))
            .thenAccept(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @Operation(summary = "Set message TTL in seconds for a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description =
            "Not authenticate to perform the request or policy is read only"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405", description =
                    "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(responseCode = "412", description = "Invalid message TTL value")})
    public void setMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "TTL in seconds for the specified topic", required = true)
            @QueryParam("messageTTL") Integer messageTTL,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMessageTTL(messageTTL, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/messageTTL")
    @Operation(summary = "Remove message TTL in seconds for a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403",
                    description = "Not authenticate to perform the request or policy is read only"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, enable the topic level policy and retry"),
            @ApiResponse(responseCode = "412", description = "Invalid message TTL value")})
    public void removeMessageTTL(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.TTL, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMessageTTL(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMessageTTL", ex, asyncResponse);
                return null;
            });
    }

    // --- Persistence ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @Operation(
            summary = "Get configuration of persistence policies for specified topic.")
    @ApiResponses(value = {@ApiResponse(
            responseCode = "200",
            description = "Get configuration of persistence policies for specified topic.",
            content = @Content(schema = @Schema(implementation = PersistencePolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void getPersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetPersistence(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getPersistence", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @Operation(summary = "Set configuration of persistence policies for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "400", description = "Invalid persistence policies")})
    public void setPersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @RequestBody(description = "Bookkeeper persistence policies for specified topic")
                                           PersistencePolicies persistencePolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetPersistence(persistencePolicies, isGlobal))
            .thenRun(() -> {
                try {
                    log.info()
                            .attr("namespace", namespaceName)
                            .attr("topic", topicName.getLocalName())
                            .attr("persistencePolicies", objectWriter().writeValueAsString(persistencePolicies))
                            .log("Successfully updated persistence policies");
                } catch (JsonProcessingException ignore) {
                }
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setPersistence", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/persistence")
    @Operation(summary = "Remove configuration of persistence policies for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removePersistence(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalRemovePersistence(isGlobal))
            .thenRun(() -> {
                log.info()
                        .attr("namespace", namespaceName)
                        .attr("topic", topicName.getLocalName())
                        .log("Successfully remove persistence policies");
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removePersistence", ex, asyncResponse);
                return null;
            });
    }

    // --- Max message size ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @Operation(summary = "Get maxMessageSize config for specified topic.")
    @ApiResponses(value = {@ApiResponse(
            responseCode = "200",
            description = "Get maxMessageSize config for specified topic.",
            content = @Content(schema = @Schema(implementation = Integer.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void getMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetMaxMessageSize(isGlobal))
            .thenAccept(policies -> {
                asyncResponse.resume(policies.isPresent() ? policies.get() : Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("getMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @Operation(summary = "Set maxMessageSize config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification"),
            @ApiResponse(responseCode = "412", description = "Invalid value of maxMessageSize")})
    public void setMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @RequestBody(description = "The max message size of the topic") int maxMessageSize) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxMessageSize(maxMessageSize, isGlobal))
            .thenRun(() -> {
                log.info()
                        .attr("namespace", namespaceName)
                        .attr("topic", topicName.getLocalName())
                        .attr("maxMessageSize", maxMessageSize)
                        .attr("isGlobal", isGlobal)
                        .log("Successfully set max message size");
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("setMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/maxMessageSize")
    @Operation(summary = "Remove maxMessageSize config for specified topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "405",
                    description = "Topic level policy is disabled, to enable the topic level policy and retry"),
            @ApiResponse(responseCode = "409", description = "Concurrent modification")})
    public void removeMaxMessageSize(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetMaxMessageSize(null, isGlobal))
            .thenRun(() -> {
                log.info()
                        .attr("namespace", namespaceName)
                        .attr("topic", topicName.getLocalName())
                        .log("Successfully remove max message size");
                asyncResponse.resume(Response.noContent().build());
            })
            .exceptionally(ex -> {
                handleTopicPolicyException("removeMaxMessageSize", ex, asyncResponse);
                return null;
            });
    }

    // --- Delayed delivery ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @Operation(summary = "Get delayed delivery messages config on a topic.")
    @ApiResponses(value = { @ApiResponse(
            responseCode = "200",
            description = "Get delayed delivery messages config on a topic.",
            content = @Content(schema = @Schema(implementation = DelayedDeliveryPolicies.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"), })
    public void getDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.DELAYED_DELIVERY, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetDelayedDeliveryPolicies(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @Operation(summary = "Set delayed delivery messages config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setDelayedDeliveryPolicies(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @RequestBody(description = "Delayed delivery policies for the specified topic")
                    DelayedDeliveryPolicies deliveryPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validatePoliciesReadOnlyAccessAsync()
            .thenCompose(__ -> validateTopicPolicyOperationAsync(topicName, PolicyName.DELAYED_DELIVERY,
                    PolicyOperation.WRITE))
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDelayedDeliveryPolicies(deliveryPolicies, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/delayedDelivery")
    @Operation(summary = "Remove delayed delivery messages config on a topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void deleteDelayedDeliveryPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validatePoliciesReadOnlyAccessAsync()
            .thenCompose(__ -> validateTopicPolicyOperationAsync(topicName, PolicyName.DELAYED_DELIVERY,
                    PolicyOperation.WRITE))
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetDelayedDeliveryPolicies(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("deleteDelayedDeliveryPolicies", ex, asyncResponse);
                return null;
            });
    }

    // --- Offload policies ---

    @GET
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @Operation(summary = "Get offload policies on a topic.")
    @ApiResponses(value = { @ApiResponse(
            responseCode = "200",
            description = "Get offload policies on a topic.",
            content = @Content(schema = @Schema(implementation = OffloadPoliciesImpl.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Tenant or cluster or namespace or topic doesn't exist"),
            @ApiResponse(responseCode = "500", description = "Internal server error"), })
    public void getOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("applied") @DefaultValue("false") boolean applied,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.READ)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalGetOffloadPolicies(applied, isGlobal))
            .thenApply(asyncResponse::resume)
            .exceptionally(ex -> {
                handleTopicPolicyException("getOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @Operation(summary = "Set offload policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void setOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @RequestBody(description = "Offload policies for the specified topic")
                    OffloadPoliciesImpl offloadPolicies) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenAccept(__ -> validateOffloadPolicies(offloadPolicies))
            .thenCompose(__ -> internalSetOffloadPolicies(offloadPolicies, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("setOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/offloadPolicies")
    @Operation(summary = "Delete offload policies on a topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404",
                    description = "Tenant or cluster or namespace or topic doesn't exist"), })
    public void removeOffloadPolicies(@Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("isGlobal") @DefaultValue("false") boolean isGlobal,
            @Parameter(description = "Whether leader broker redirected this call to this broker. For internal use.")
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(tenant, namespace, encodedTopic);
        validateTopicPolicyOperationAsync(topicName, PolicyName.OFFLOAD, PolicyOperation.WRITE)
            .thenCompose(__ -> preValidation(authoritative))
            .thenCompose(__ -> internalSetOffloadPolicies(null, isGlobal))
            .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
            .exceptionally(ex -> {
                handleTopicPolicyException("removeOffloadPolicies", ex, asyncResponse);
                return null;
            });
    }

    // --- Everything else: not supported on scalable topics ---
    // A literal path segment wins the JAX-RS match, so these only take the operations not declared above, also
    // the ones further down the path, like {subName}/dispatchRate, and the ones on the topic path itself, like
    // deleting the topic. The latter are named after their HTTP method in the response.

    @GET
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(hidden = true)
    public void getUnsupportedTopicOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, HttpMethod.GET);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(hidden = true)
    public void putUnsupportedTopicOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, HttpMethod.PUT);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(hidden = true)
    public void postUnsupportedTopicOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, HttpMethod.POST);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}")
    @Operation(hidden = true)
    public void deleteUnsupportedTopicOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, HttpMethod.DELETE);
    }

    @GET
    @Path("/{tenant}/{namespace}/{topic}/{operation: .+}")
    @Operation(hidden = true)
    public void getUnsupportedOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("operation") String operation) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, operation);
    }

    @PUT
    @Path("/{tenant}/{namespace}/{topic}/{operation: .+}")
    @Operation(hidden = true)
    public void putUnsupportedOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("operation") String operation) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, operation);
    }

    @POST
    @Path("/{tenant}/{namespace}/{topic}/{operation: .+}")
    @Operation(hidden = true)
    public void postUnsupportedOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("operation") String operation) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, operation);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{topic}/{operation: .+}")
    @Operation(hidden = true)
    public void deleteUnsupportedOperation(@PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @PathParam("operation") String operation) {
        rejectUnsupportedOperation(tenant, namespace, encodedTopic, operation);
    }

    private void rejectUnsupportedOperation(String tenant, String namespace, String encodedTopic,
                                            String operation) {
        validateTopicName(tenant, namespace, encodedTopic);
        throw new RestException(Status.METHOD_NOT_ALLOWED, "'" + operation + "' is not supported on scalable topic "
                + topicName + ". Supported topic policies: " + String.join(", ", SUPPORTED_POLICIES)
                + ". Use the /admin/v2/scalable API (pulsar-admin scalable-topics) for other operations.");
    }
}
