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
package org.apache.pulsar.broker.admin.impl;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService.State;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker admin base.
 */
public class BrokersBase extends AdminResource {
    private static final Logger LOG = LoggerFactory.getLogger(BrokersBase.class);
    public static final String HEALTH_CHECK_TOPIC_SUFFIX = "healthcheck";

    @GET
    @Path("/{cluster}")
    @ApiOperation(
        value = "Get the list of active brokers (web service addresses) in the cluster."
                + "If authorization is not enabled, any cluster name is valid.",
        response = String.class,
        responseContainer = "Set")
    @ApiResponses(
        value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve this cluster"),
            @ApiResponse(code = 401, message = "Authentication required"),
            @ApiResponse(code = 403, message = "This operation requires super-user access"),
            @ApiResponse(code = 404, message = "Cluster does not exist: cluster={clustername}") })
    public void getActiveBrokers(@Suspended final AsyncResponse asyncResponse,
                                 @PathParam("cluster") String cluster) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> validateClusterOwnershipAsync(cluster))
                .thenCompose(__ -> pulsar().getLoadManager().get().getAvailableBrokersAsync())
                .thenAccept(activeBrokers -> {
                    LOG.info("[{}] Successfully to get active brokers, cluster={}", clientAppId(), cluster);
                    asyncResponse.resume(activeBrokers);
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        LOG.error("[{}] Fail to get active brokers, cluster={}", clientAppId(), cluster, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/leaderBroker")
    @ApiOperation(
            value = "Get the information of the leader broker.",
            response = BrokerInfo.class)
    @ApiResponses(
            value = {
                    @ApiResponse(code = 401, message = "Authentication required"),
                    @ApiResponse(code = 403, message = "This operation requires super-user access"),
                    @ApiResponse(code = 404, message = "Leader broker not found") })
    public void getLeaderBroker(@Suspended final AsyncResponse asyncResponse) {
        validateSuperUserAccessAsync().thenAccept(__ -> {
                    LeaderBroker leaderBroker = pulsar().getLeaderElectionService().getCurrentLeader()
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Couldn't find leader broker"));
                    BrokerInfo brokerInfo = BrokerInfo.builder().serviceUrl(leaderBroker.getServiceUrl()).build();
                    LOG.info("[{}] Successfully to get the information of the leader broker.", clientAppId());
                    asyncResponse.resume(brokerInfo);
                })
                .exceptionally(ex -> {
                    LOG.error("[{}] Failed to get the information of the leader broker.", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/{clusterName}/{broker-webserviceurl}/ownedNamespaces")
    @ApiOperation(value = "Get the list of namespaces served by the specific broker",
            response = NamespaceOwnershipStatus.class, responseContainer = "Map")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the cluster"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(@PathParam("clusterName") String cluster,
            @PathParam("broker-webserviceurl") String broker) throws Exception {
        validateSuperUserAccess();
        validateClusterOwnership(cluster);
        validateBrokerName(broker);

        try {
            // now we validated that this is the broker specified in the request
            return pulsar().getNamespaceService().getOwnedNameSpacesStatus();
        } catch (Exception e) {
            LOG.error("[{}] Failed to get the namespace ownership status. cluster={}, broker={}", clientAppId(),
                    cluster, broker);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/configuration/{configName}/{configValue}")
    @ApiOperation(value =
            "Update dynamic serviceconfiguration into zk only. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Service configuration updated successfully"),
            @ApiResponse(code = 403, message = "You don't have admin permission to update service-configuration"),
            @ApiResponse(code = 404, message = "Configuration not found"),
            @ApiResponse(code = 412, message = "Invalid dynamic-config value"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void updateDynamicConfiguration(@Suspended AsyncResponse asyncResponse,
                                           @PathParam("configName") String configName,
                                           @PathParam("configValue") String configValue) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> persistDynamicConfigurationAsync(configName, configValue))
                .thenAccept(__ -> {
                    LOG.info("[{}] Updated Service configuration {}/{}", clientAppId(), configName, configValue);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    LOG.error("[{}] Failed to update configuration {}/{}", clientAppId(), configName, configValue, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @DELETE
    @Path("/configuration/{configName}")
    @ApiOperation(value =
            "Delete dynamic ServiceConfiguration into metadata only."
                    + " This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Service configuration updated successfully"),
            @ApiResponse(code = 403, message = "You don't have admin permission to update service-configuration"),
            @ApiResponse(code = 412, message = "Invalid dynamic-config value"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void deleteDynamicConfiguration(
            @Suspended AsyncResponse asyncResponse,
            @PathParam("configName") String configName) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> internalDeleteDynamicConfigurationOnMetadataAsync(configName))
                .thenAccept(__ -> {
                    LOG.info("[{}] Successfully to delete dynamic configuration {}", clientAppId(), configName);
                    asyncResponse.resume(Response.ok().build());
                }).exceptionally(ex -> {
                    LOG.error("[{}] Failed to delete dynamic configuration {}", clientAppId(), configName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    @GET
    @Path("/configuration/values")
    @ApiOperation(value = "Get value of all dynamic configurations' value overridden on local config")
    @ApiResponses(value = {
        @ApiResponse(code = 403, message = "You don't have admin permission to view configuration"),
        @ApiResponse(code = 404, message = "Configuration not found"),
        @ApiResponse(code = 500, message = "Internal server error")})
    public Map<String, String> getAllDynamicConfigurations() throws Exception {
        validateSuperUserAccess();
        try {
            return dynamicConfigurationResources().getDynamicConfiguration().orElseGet(Collections::emptyMap);
        } catch (RestException e) {
            LOG.error("[{}] couldn't find any configuration in zk {}", clientAppId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error("[{}] Failed to retrieve configuration from zk {}", clientAppId(), e.getMessage(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/configuration")
    @ApiOperation(value = "Get all updatable dynamic configurations's name")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "You don't have admin permission to get configuration")})
    public List<String> getDynamicConfigurationName() {
        validateSuperUserAccess();
        return BrokerService.getDynamicConfiguration();
    }

    @GET
    @Path("/configuration/runtime")
    @ApiOperation(value = "Get all runtime configurations. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public Map<String, String> getRuntimeConfiguration() {
        validateSuperUserAccess();
        return pulsar().getBrokerService().getRuntimeConfiguration();
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
        if (!BrokerService.validateDynamicConfiguration(configName, configValue)) {
            return FutureUtil
                    .failedFuture(new RestException(Status.PRECONDITION_FAILED, " Invalid dynamic-config value"));
        }
        if (BrokerService.isDynamicConfiguration(configName)) {
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
    @ApiOperation(value = "Get the internal configuration data", response = InternalConfigurationData.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public InternalConfigurationData getInternalConfigurationData() {
        validateSuperUserAccess();
        return pulsar().getInternalConfigurationData();
    }

    @GET
    @Path("/backlog-quota-check")
    @ApiOperation(value = "An REST endpoint to trigger backlogQuotaCheck")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Everything is OK"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void backlogQuotaCheck(@Suspended AsyncResponse asyncResponse) {
        validateSuperUserAccess();
        pulsar().getBrokerService().getBacklogQuotaChecker().execute(safeRun(()->{
            try {
                pulsar().getBrokerService().monitorBacklogQuota();
                asyncResponse.resume(Response.noContent().build());
            } catch (Exception e) {
                LOG.error("trigger backlogQuotaCheck fail", e);
                asyncResponse.resume(new RestException(e));
            }
        }));
    }

    @GET
    @Path("/ready")
    @ApiOperation(value = "Check if the broker is fully initialized")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Broker is ready"),
            @ApiResponse(code = 500, message = "Broker is not ready") })
    public void isReady(@Suspended AsyncResponse asyncResponse) {
        if (pulsar().getState() == State.Started) {
            asyncResponse.resume(Response.ok("ok").build());
        } else {
            asyncResponse.resume(Response.serverError().build());
        }
    }

    @GET
    @Path("/health")
    @ApiOperation(value = "Run a healthCheck against the broker")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Everything is OK"),
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")})
    @ApiParam(value = "Topic Version")
    public void healthCheck(@Suspended AsyncResponse asyncResponse,
                            @QueryParam("topicVersion") TopicVersion topicVersion) {
        validateSuperUserAccessAsync()
                .thenCompose(__ -> internalRunHealthCheck(topicVersion))
                .thenAccept(__ -> {
                    LOG.info("[{}] Successfully run health check.", clientAppId());
                    asyncResponse.resume("ok");
                }).exceptionally(ex -> {
                    LOG.error("[{}] Fail to run health check.", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> internalRunHealthCheck(TopicVersion topicVersion) {
        NamespaceName namespaceName = (topicVersion == TopicVersion.V2)
                ? NamespaceService.getHeartbeatNamespaceV2(pulsar().getAdvertisedAddress(), pulsar().getConfiguration())
                : NamespaceService.getHeartbeatNamespace(pulsar().getAdvertisedAddress(), pulsar().getConfiguration());
        final String topicName = String.format("persistent://%s/%s", namespaceName, HEALTH_CHECK_TOPIC_SUFFIX);
        LOG.info("[{}] Running healthCheck with topic={}", clientAppId(), topicName);
        final String messageStr = UUID.randomUUID().toString();
        final String subscriptionName = "healthCheck-" + messageStr;
        // create non-partitioned topic manually and close the previous reader if present.
        return pulsar().getBrokerService().getTopic(topicName, true)
            .thenCompose(topicOptional -> {
                if (!topicOptional.isPresent()) {
                    LOG.error("[{}] Fail to run health check while get topic {}. because get null value.",
                            clientAppId(), topicName);
                    throw new RestException(Status.NOT_FOUND,
                            String.format("Topic [%s] not found after create.", topicName));
                }
                PulsarClient client;
                try {
                    client = pulsar().getClient();
                } catch (PulsarServerException e) {
                    LOG.error("[{}] Fail to run health check while get client.", clientAppId());
                    throw new RestException(e);
                }
                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                client.newProducer(Schema.STRING).topic(topicName).createAsync()
                        .thenCompose(producer -> client.newReader(Schema.STRING).topic(topicName)
                                .subscriptionName(subscriptionName)
                                .startMessageId(MessageId.latest)
                                .createAsync().exceptionally(createException -> {
                                    producer.closeAsync().exceptionally(ex -> {
                                        LOG.error("[{}] Close producer fail while heath check.", clientAppId());
                                        return null;
                                    });
                                    throw FutureUtil.wrapToCompletionException(createException);
                                }).thenCompose(reader -> producer.sendAsync(messageStr)
                                        .thenCompose(__ -> healthCheckRecursiveReadNext(reader, messageStr))
                                        .whenComplete((__, ex) -> {
                                            closeAndReCheck(producer, reader, topicOptional.get(), subscriptionName)
                                                    .whenComplete((unused, innerEx) -> {
                                                        if (ex != null) {
                                                            resultFuture.completeExceptionally(ex);
                                                        } else {
                                                            resultFuture.complete(null);
                                                        }
                                                    });
                                        }
                                ))
                        ).exceptionally(ex -> {
                            resultFuture.completeExceptionally(ex);
                            return null;
                        });
                return resultFuture;
            });
    }

    /**
     * Close producer and reader and then to re-check if this operation is success.
     *
     * Re-check
     * - Producer: If close fails we will print error log to notify user.
     * - Consumer: If close fails we will force delete subscription.
     *
     * @param producer Producer
     * @param reader Reader
     * @param topic  Topic
     * @param subscriptionName  Subscription name
     */
    private CompletableFuture<Void> closeAndReCheck(Producer<String> producer, Reader<String> reader,
                                                    Topic topic, String subscriptionName) {
        // no matter exception or success, we still need to
        // close producer/reader
        CompletableFuture<Void> producerFuture = producer.closeAsync();
        CompletableFuture<Void> readerFuture = reader.closeAsync();
        List<CompletableFuture<Void>> futures = new ArrayList<>(2);
        futures.add(producerFuture);
        futures.add(readerFuture);
        return FutureUtil.waitForAll(Collections.unmodifiableList(futures))
                .exceptionally(closeException -> {
                    if (readerFuture.isCompletedExceptionally()) {
                        LOG.error("[{}] Close reader fail while heath check.", clientAppId());
                        Subscription subscription =
                                topic.getSubscription(subscriptionName);
                        // re-check subscription after reader close
                        if (subscription != null) {
                            LOG.warn("[{}] Force delete subscription {} "
                                            + "when it still exists after the"
                                            + " reader is closed.",
                                    clientAppId(), subscription);
                            subscription.deleteForcefully()
                                    .exceptionally(ex -> {
                                        LOG.error("[{}] Force delete subscription fail"
                                                        + " while health check",
                                                clientAppId(), ex);
                                        return null;
                                    });
                        }
                    } else {
                        // producer future fail.
                        LOG.error("[{}] Close producer fail while heath check.", clientAppId());
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> healthCheckRecursiveReadNext(Reader<String> reader, String content) {
        return reader.readNextAsync()
                .thenCompose(msg -> {
                    if (!Objects.equals(content, msg.getValue())) {
                        return healthCheckRecursiveReadNext(reader, content);
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    private CompletableFuture<Void> internalDeleteDynamicConfigurationOnMetadataAsync(String configName) {
        if (!BrokerService.isDynamicConfiguration(configName)) {
            throw new RestException(Status.PRECONDITION_FAILED, " Can't update non-dynamic configuration");
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
    @ApiOperation(value = "Get version of current broker")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Everything is OK"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public String version() throws Exception {
        return PulsarVersion.getVersion();
    }

    @POST
    @Path("/shutdown")
    @ApiOperation(value =
            "Shutdown broker gracefully.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Execute shutdown command successfully"),
            @ApiResponse(code = 403, message = "You don't have admin permission to update service-configuration"),
            @ApiResponse(code = 500, message = "Internal server error")})
    public void shutDownBrokerGracefully(
            @ApiParam(name = "maxConcurrentUnloadPerSec",
                    value = "if the value absent(value=0) means no concurrent limitation.")
            @QueryParam("maxConcurrentUnloadPerSec") int maxConcurrentUnloadPerSec,
            @QueryParam("forcedTerminateTopic") @DefaultValue("true") boolean forcedTerminateTopic
    ) {
        validateSuperUserAccess();
        doShutDownBrokerGracefully(maxConcurrentUnloadPerSec, forcedTerminateTopic);
    }

    private void doShutDownBrokerGracefully(int maxConcurrentUnloadPerSec,
                                            boolean forcedTerminateTopic) {
        pulsar().getBrokerService().unloadNamespaceBundlesGracefully(maxConcurrentUnloadPerSec, forcedTerminateTopic);
        pulsar().closeAsync();
    }
}

