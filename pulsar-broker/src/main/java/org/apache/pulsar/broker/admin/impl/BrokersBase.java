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
package org.apache.pulsar.broker.admin.impl;

import com.google.common.collect.Maps;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.PulsarService.State;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.BrokerOperation;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ThreadDumpUtil;

/**
 * Broker admin base.
 */
public class BrokersBase extends AdminResource {
    // log a full thread dump when a deadlock is detected in healthcheck once every 10 minutes
    // to prevent excessive logging
    private static final long LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED = 600000L;
    private static volatile long threadDumpLoggedTimestamp;

    @GET
    @Path("/{cluster}")
    @Operation(
        summary = "Get the list of active brokers (broker ids) in the cluster. "
                + "If authorization is not enabled, any cluster name is valid.")
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200",
                description = "Get the list of active brokers (broker ids) in the cluster. "
                        + "If authorization is not enabled, any cluster name is valid.",
                content = @Content(array = @ArraySchema(uniqueItems = true,
                        schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve this cluster"),
            @ApiResponse(responseCode = "401", description = "Authentication required"),
            @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
            @ApiResponse(responseCode = "404", description = "Cluster does not exist: cluster={clustername}") })
    public void getActiveBrokers(@Suspended final AsyncResponse asyncResponse,
                                 @PathParam("cluster") String cluster) {
        validateBothSuperuserAndBrokerOperation(cluster == null ? pulsar().getConfiguration().getClusterName()
                        : cluster, pulsar().getBrokerId(), BrokerOperation.LIST_BROKERS)
                .thenCompose(__ -> validateClusterOwnershipAsync(cluster))
                .thenCompose(__ -> pulsar().getLoadManager().get().getAvailableBrokersAsync())
                .thenAccept(activeBrokers -> {
                    log.info()
                            .attr("cluster", cluster)
                            .log("Successfully to get active brokers, cluster");
                    asyncResponse.resume(activeBrokers);
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("cluster", cluster)
                                .exception(ex)
                                .log("Fail to get active brokers, cluster");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Operation(
            summary = "Get the list of active brokers (broker ids) in the local cluster. "
                    + "If authorization is not enabled")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200",
                            description = "Get the list of active brokers (broker ids) in the local cluster. "
                                    + "If authorization is not enabled",
                            content = @Content(array = @ArraySchema(uniqueItems = true,
                                    schema = @Schema(implementation = String.class)))),
                    @ApiResponse(responseCode = "401", description = "Authentication required"),
                    @ApiResponse(responseCode = "403", description = "This operation requires super-user access") })
    public void getActiveBrokers(@Suspended final AsyncResponse asyncResponse) throws Exception {
        getActiveBrokers(asyncResponse, null);
    }

    @GET
    @Path("/leaderBroker")
    @Operation(
            summary = "Get the information of the leader broker.")
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Get the information of the leader broker.",
                            content = @Content(schema = @Schema(implementation = BrokerInfo.class))),
                    @ApiResponse(responseCode = "401", description = "Authentication required"),
                    @ApiResponse(responseCode = "403", description = "This operation requires super-user access"),
                    @ApiResponse(responseCode = "404", description = "Leader broker not found") })
    public void getLeaderBroker(@Suspended final AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(),
                pulsar().getBrokerId(), BrokerOperation.GET_LEADER_BROKER)
                // The authoritative read: waits for an in-progress leader election to settle
                // instead of returning 404 while a re-election is still in flight.
                .thenCompose(__ -> pulsar().getLeaderElectionService().readCurrentLeader())
                .thenAccept(leader -> {
                    LeaderBroker leaderBroker = leader
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Couldn't find leader broker"));
                    BrokerInfo brokerInfo = BrokerInfo.builder()
                            .serviceUrl(leaderBroker.getServiceUrl())
                            .brokerId(leaderBroker.getBrokerId()).build();
                    log.info("Successfully got the information of the leader broker");
                    asyncResponse.resume(brokerInfo);
                })
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to get the information of the leader broker.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{clusterName}/{brokerId}/ownedNamespaces")
    @Operation(summary = "Get the list of namespaces served by the specific broker id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get the list of namespaces served by the specific broker id",
                    content = @Content(schema = @Schema(type = "object",
                            additionalPropertiesSchema = NamespaceOwnershipStatus.class))),
            @ApiResponse(responseCode = "307", description = "Current broker doesn't serve the cluster"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "404", description = "Cluster doesn't exist") })
    public void getOwnedNamespaces(@Suspended final AsyncResponse asyncResponse,
                                   @PathParam("clusterName") String cluster,
                                   @PathParam("brokerId") String brokerId) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(),
                pulsar().getBrokerId(), BrokerOperation.LIST_OWNED_NAMESPACES)
                .thenCompose(__ -> maybeRedirectToBroker(brokerId))
                .thenCompose(__ -> validateClusterOwnershipAsync(cluster))
                .thenCompose(__ -> pulsar().getNamespaceService().getOwnedNameSpacesStatusAsync())
                .thenAccept(asyncResponse::resume)
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error()
                                .attr("cluster", cluster)
                                .attr("broker", brokerId)
                                .log("Failed to get the namespace ownership status");
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @POST
    @Path("/configuration/{configName}/{configValue}")
    @Operation(summary =
            "Update dynamic ServiceConfiguration into zk only. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Service configuration updated successfully"),
            @ApiResponse(responseCode = "403",
                    description = "You don't have admin permission to update service-configuration"),
            @ApiResponse(responseCode = "404", description = "Configuration not found"),
            @ApiResponse(responseCode = "412", description = "Invalid dynamic-config value"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void updateDynamicConfiguration(@Suspended AsyncResponse asyncResponse,
                                           @PathParam("configName") String configName,
                                           @PathParam("configValue") String configValue) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.UPDATE_DYNAMIC_CONFIGURATION)
                .thenCompose(__ -> persistDynamicConfigurationAsync(configName, configValue))
                .thenAccept(__ -> {
                    log.info()
                            .attr("configuration", configName)
                            .attr("configValue", configValue)
                            .log("Updated Service configuration");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("configuration", configName)
                            .attr("configValue", configValue)
                            .exception(ex)
                            .log("Failed to update configuration");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/configuration/{configName}")
    @Operation(summary =
            "Delete dynamic ServiceConfiguration from metadata only."
                    + " This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Service configuration deleted successfully"),
            @ApiResponse(responseCode = "403",
                    description = "You don't have admin permission to update service-configuration"),
            @ApiResponse(responseCode = "412", description = "Invalid dynamic-config value"),
            @ApiResponse(responseCode = "500", description = "Internal server error") })
    public void deleteDynamicConfiguration(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("configName") String configName) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.DELETE_DYNAMIC_CONFIGURATION)
                .thenCompose(__ -> internalDeleteDynamicConfigurationOnMetadataAsync(configName))
                .thenAccept(__ -> {
                    log.info()
                            .attr("configuration", configName)
                            .log("Successfully to delete dynamic configuration");
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    log.error()
                            .attr("configuration", configName)
                            .exception(ex)
                            .log("Failed to delete dynamic configuration");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/configuration/values")
    @Operation(summary = "Get the values of all dynamic configurations overridden on local config")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Get the values of all dynamic configurations overridden on local config",
            content = @Content(schema = @Schema(type = "object", additionalPropertiesSchema = String.class))),
        @ApiResponse(responseCode = "403", description = "You don't have admin permission to view configuration"),
        @ApiResponse(responseCode = "404", description = "Configuration not found"),
        @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void getAllDynamicConfigurations(@Suspended AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.LIST_DYNAMIC_CONFIGURATIONS)
                .thenCompose(__ -> dynamicConfigurationResources().getDynamicConfigurationAsync())
                .thenAccept(configOpt -> asyncResponse.resume(configOpt.orElseGet(Collections::emptyMap)))
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to get all dynamic configuration.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/configuration")
    @Operation(summary = "Get all updatable dynamic configurations' names")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get all updatable dynamic configurations' names",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "403", description = "You don't have admin permission to get configuration")})
    public void getDynamicConfigurationName(@Suspended AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.LIST_DYNAMIC_CONFIGURATIONS)
                .thenAccept(__ -> asyncResponse.resume(pulsar().getBrokerService().getDynamicConfiguration()))
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to get all dynamic configuration names.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/configuration/runtime")
    @Operation(summary = "Get all runtime configurations. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Get all runtime configurations. This operation requires Pulsar super-user "
                            + "privileges.",
                    content = @Content(schema = @Schema(type = "object",
                            additionalPropertiesSchema = String.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void getRuntimeConfiguration(@Suspended AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.LIST_RUNTIME_CONFIGURATIONS)
                .thenAccept(__ -> asyncResponse.resume(pulsar().getBrokerService().getRuntimeConfiguration()))
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to get runtime configuration.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    /**
     * if {@link ServiceConfiguration}-field is allowed to be modified dynamically, update configuration-map into zk, so
     * all other brokers get the watch and can see the change and take appropriate action on the change.
     *
     * @param configName
     *            : configuration key
     * @param configValue
     *            : configuration value
     */
    private synchronized CompletableFuture<Void> persistDynamicConfigurationAsync(
            String configName, String configValue) {
        if (!pulsar().getBrokerService().validateDynamicConfiguration(configName, configValue)) {
            return FutureUtil
                    .failedFuture(new RestException(Status.PRECONDITION_FAILED, " Invalid dynamic-config value"));
        }
        if (pulsar().getBrokerService().isDynamicConfiguration(configName)) {
            return dynamicConfigurationResources().setDynamicConfigurationWithCreateAsync(old -> {
                Map<String, String> configurationMap = old.orElseGet(Maps::newHashMap);
                configurationMap.put(configName, configValue);
                return configurationMap;
            });
        } else {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Can't update non-dynamic configuration"));
        }
    }

    @GET
    @Path("/internal-configuration")
    @Operation(summary = "Get the internal configuration data")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Get the internal configuration data",
                    content = @Content(schema = @Schema(implementation = InternalConfigurationData.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission") })
    public void getInternalConfigurationData(@Suspended AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.GET_INTERNAL_CONFIGURATION_DATA)
                .thenAccept(__ -> asyncResponse.resume(pulsar().getInternalConfigurationData()))
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to get internal configuration data.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/backlog-quota-check")
    @Operation(summary = "A REST endpoint to trigger backlogQuotaCheck")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Everything is OK"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void backlogQuotaCheck(@Suspended AsyncResponse asyncResponse) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.CHECK_BACKLOG_QUOTA)
                .thenAcceptAsync(__ -> {
                    pulsar().getBrokerService().monitorBacklogQuota();
                    asyncResponse.resume(Response.noContent().build());
                } , pulsar().getBrokerService().getBacklogQuotaChecker())
                .exceptionally(ex -> {
                    log.error()
                            .exception(ex)
                            .log("Failed to trigger backlog quota check.");
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/ready")
    @Operation(summary = "Check if the broker is fully initialized")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Broker is ready"),
            @ApiResponse(responseCode = "500", description = "Broker is not ready") })
    public void isReady(@Suspended AsyncResponse asyncResponse) {
        if (pulsar().getState() == State.Started) {
            asyncResponse.resume(Response.ok("ok").build());
        } else {
            asyncResponse.resume(Response.serverError().build());
        }
    }

    @GET
    @Path("/health")
    @Operation(summary = "Run a healthCheck against the broker")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Everything is OK"),
        @ApiResponse(responseCode = "307", description = "Current broker is not the target broker"),
        @ApiResponse(responseCode = "403", description = "Don't have admin permission"),
        @ApiResponse(responseCode = "404", description = "Cluster doesn't exist"),
        @ApiResponse(responseCode = "500", description = "Internal server error"),
        @ApiResponse(responseCode = "503", description = "Service unavailable")})
    public void healthCheck(@Suspended AsyncResponse asyncResponse,
                            @QueryParam("brokerId") String brokerId) {
        if (pulsar().getState() == State.Closed || pulsar().getState() == State.Closing) {
            asyncResponse.resume(Response.status(Status.SERVICE_UNAVAILABLE).build());
            return;
        }
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), StringUtils.isBlank(brokerId)
                ? pulsar().getBrokerId() : brokerId, BrokerOperation.HEALTH_CHECK)
                .thenCompose(__ -> maybeRedirectToBroker(
                        StringUtils.isBlank(brokerId) ? pulsar().getBrokerId() : brokerId))
                .thenAccept(__ -> checkDeadlockedThreads())
                .thenCompose(__ -> internalRunHealthCheck())
                .thenAccept(__ -> {
                    log.info("Successfully ran health check");
                    asyncResponse.resume(Response.ok("ok").build());
                }).exceptionally(ex -> {
                    if (!isRedirectException(ex)) {
                        if (isNotFoundException(ex)) {
                            log.warn()
                                    .exceptionMessage(ex)
                                    .log("Failed to run health check");
                        } else {
                            log.error()
                                    .exception(ex)
                                    .log("Failed to run health check.");
                        }
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private void checkDeadlockedThreads() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.findDeadlockedThreads();
        if (threadIds != null && threadIds.length > 0) {
            ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, false, false);
            String threadNames = Arrays.stream(threadInfos)
                    .map(threadInfo -> threadInfo.getThreadName() + "(tid=" + threadInfo.getThreadId() + ")").collect(
                            Collectors.joining(", "));
            if (System.currentTimeMillis() - threadDumpLoggedTimestamp
                    > LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED) {
                threadDumpLoggedTimestamp = System.currentTimeMillis();
                log.error()
                        .attr("detected", threadNames)
                        .attr("n", ThreadDumpUtil.buildThreadDiagnosticString())
                        .log("Deadlocked threads detected. \n");
            } else {
                log.error().attr("detected", threadNames).log("Deadlocked threads detected.");
            }
            throw new IllegalStateException("Deadlocked threads detected. " + threadNames);
        }
    }

    private CompletableFuture<Void> internalRunHealthCheck() {
        return pulsar().runHealthCheck(clientAppId());
    }

    private CompletableFuture<Void> internalDeleteDynamicConfigurationOnMetadataAsync(String configName) {
        if (!pulsar().getBrokerService().isDynamicConfiguration(configName)) {
            return FutureUtil.failedFuture(
                    new RestException(Status.PRECONDITION_FAILED, "Can't delete non-dynamic configuration"));
        } else {
            return dynamicConfigurationResources().setDynamicConfigurationAsync(old -> {
                if (old != null) {
                    old.remove(configName);
                }
                return old;
            });
        }
    }

    @GET
    @Path("/version")
    @Operation(summary = "Get version of current broker")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The Pulsar version",
                    content = @Content(schema = @Schema(implementation = String.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public String version() throws Exception {
        return PulsarVersion.getVersion();
    }

    @POST
    @Path("/shutdown")
    @Operation(summary =
            "Shutdown broker gracefully.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Execute shutdown command successfully"),
            @ApiResponse(responseCode = "403",
                    description = "You don't have admin permission to update service-configuration"),
            @ApiResponse(responseCode = "500", description = "Internal server error")})
    public void shutDownBrokerGracefully(
            @Parameter(name = "maxConcurrentUnloadPerSec",
                    description = "If the value is absent (value=0), it means there is no concurrency limit.")
            @QueryParam("maxConcurrentUnloadPerSec") int maxConcurrentUnloadPerSec,
            @QueryParam("forcedTerminateTopic") @DefaultValue("true") boolean forcedTerminateTopic,
            @Suspended final AsyncResponse asyncResponse
    ) {
        validateBothSuperuserAndBrokerOperation(pulsar().getConfig().getClusterName(), pulsar().getBrokerId(),
                BrokerOperation.SHUTDOWN)
                .thenCompose(__ -> doShutDownBrokerGracefullyAsync(maxConcurrentUnloadPerSec, forcedTerminateTopic))
                .thenAccept(__ -> {
                    log.info("Successfully shutdown broker gracefully");
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
            log.error().exception(ex).log("Failed to shutdown broker gracefully");
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private CompletableFuture<Void> doShutDownBrokerGracefullyAsync(int maxConcurrentUnloadPerSec,
                                                                    boolean forcedTerminateTopic) {
        pulsar().getBrokerService().unloadNamespaceBundlesGracefully(maxConcurrentUnloadPerSec, forcedTerminateTopic);
        return pulsar().closeAsync(false);
    }

    private CompletableFuture<Void> validateBothSuperuserAndBrokerOperation(String cluster, String brokerId,
                                                                            BrokerOperation operation) {
        final var superUserAccessValidation = validateSuperUserAccessAsync();
        final var brokerOperationValidation = validateBrokerOperationAsync(cluster, brokerId, operation);
        return FutureUtil.waitForAll(List.of(superUserAccessValidation, brokerOperationValidation))
                .handle((result, err) -> {
                    if (!superUserAccessValidation.isCompletedExceptionally()
                        || !brokerOperationValidation.isCompletedExceptionally()) {
                        return null;
                    }
                    Throwable superUserValidationException = null;
                    try {
                        superUserAccessValidation.join();
                    } catch (Throwable ex) {
                        superUserValidationException = FutureUtil.unwrapCompletionException(ex);
                    }
                    Throwable brokerOperationValidationException = null;
                    try {
                        brokerOperationValidation.join();
                    } catch (Throwable ex) {
                        brokerOperationValidationException = FutureUtil.unwrapCompletionException(ex);
                    }
                    log.debug().attr("originalPrincipal", originalPrincipal())
                            .attr("operation", operation.toString())
                            .attr("broker", brokerId)
                            .attr("superuserValidationError", superUserValidationException)
                            .attr("brokerOperationValidationError", brokerOperationValidationException)
                            .log("validateBothSuperuserAndBrokerOperation failed");
                    throw new RestException(Status.UNAUTHORIZED,
                            String.format("Unauthorized to validateBothSuperuserAndBrokerOperation for"
                                          + " originalPrincipal [%s] and clientAppId [%s] "
                                          + "about operation [%s] on broker [%s]",
                                    originalPrincipal(), clientAppId(), operation.toString(), brokerId));
                });
    }

    private CompletableFuture<Void> validateBrokerOperationAsync(String cluster, String brokerId,
                                                                 BrokerOperation operation) {
        final var pulsar = pulsar();
        if (pulsar.getBrokerService().isAuthenticationEnabled()
            && pulsar.getBrokerService().isAuthorizationEnabled()) {
            return pulsar.getBrokerService().getAuthorizationService()
                    .allowBrokerOperationAsync(cluster, brokerId, operation, originalPrincipal(),
                            clientAppId(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Unauthorized to validateBrokerOperation for"
                                                  + " originalPrincipal [%s] and clientAppId [%s] "
                                                  + "about operation [%s] on broker [%s]",
                                            originalPrincipal(), clientAppId(), operation.toString(), brokerId));
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }
}
