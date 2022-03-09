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

import static org.apache.pulsar.broker.service.BrokerService.BROKER_SERVICE_CONFIGURATION_PATH;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService.State;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker admin base.
 */
public class BrokersBase extends PulsarWebResource {
    private static final Logger LOG = LoggerFactory.getLogger(BrokersBase.class);
    private static final Duration HEALTHCHECK_READ_TIMEOUT = Duration.ofSeconds(10);
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
    public Set<String> getActiveBrokers(@PathParam("cluster") String cluster) throws Exception {
        validateSuperUserAccess();
        validateClusterOwnership(cluster);

        try {
            // Add Native brokers
            return new HashSet<>(dynamicConfigurationResources().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT));
        } catch (Exception e) {
            LOG.error("[{}] Failed to get active broker list: cluster={}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
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
    public BrokerInfo getLeaderBroker() throws Exception {
        validateSuperUserAccess();

        try {
            LeaderBroker leaderBroker = pulsar().getLeaderElectionService().getCurrentLeader()
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Couldn't find leader broker"));
            return BrokerInfo.builder().serviceUrl(leaderBroker.getServiceUrl()).build();
        } catch (Exception e) {
            LOG.error("[{}] Failed to get the information of the leader broker.", clientAppId(), e);
            throw new RestException(e);
        }
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
    public void updateDynamicConfiguration(@PathParam("configName") String configName,
                                           @PathParam("configValue") String configValue) throws Exception {
        validateSuperUserAccess();
        persistDynamicConfiguration(configName, configValue);
    }

    @DELETE
    @Path("/configuration/{configName}")
    @ApiOperation(value =
            "Delete dynamic serviceconfiguration into zk only. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Service configuration updated successfully"),
            @ApiResponse(code = 403, message = "You don't have admin permission to update service-configuration"),
            @ApiResponse(code = 412, message = "Invalid dynamic-config value"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void deleteDynamicConfiguration(@PathParam("configName") String configName) throws Exception {
        validateSuperUserAccess();
        deleteDynamicConfigurationOnZk(configName);
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
            return dynamicConfigurationResources().get(BROKER_SERVICE_CONFIGURATION_PATH)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Couldn't find configuration in zk"));
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
    private synchronized void persistDynamicConfiguration(String configName, String configValue) {
        try {
            if (!BrokerService.validateDynamicConfiguration(configName, configValue)) {
                throw new RestException(Status.PRECONDITION_FAILED, " Invalid dynamic-config value");
            }
            if (BrokerService.isDynamicConfiguration(configName)) {
                dynamicConfigurationResources().setWithCreate(BROKER_SERVICE_CONFIGURATION_PATH, (old) -> {
                    Map<String, String> configurationMap = old.isPresent() ? old.get() : Maps.newHashMap();
                    configurationMap.put(configName, configValue);
                    return configurationMap;
                });
                LOG.info("[{}] Updated Service configuration {}/{}", clientAppId(), configName, configValue);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Can't update non-dynamic configuration {}/{}", clientAppId(), configName,
                            configValue);
                }
                throw new RestException(Status.PRECONDITION_FAILED, " Can't update non-dynamic configuration");
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception ie) {
            LOG.error("[{}] Failed to update configuration {}/{}, {}", clientAppId(), configName, configValue,
                    ie.getMessage(), ie);
            throw new RestException(ie);
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
        pulsar().getBrokerService().executor().execute(()->{
            try {
                pulsar().getBrokerService().monitorBacklogQuota();
                asyncResponse.resume(Response.noContent().build());
            } catch (Exception e) {
                LOG.error("trigger backlogQuotaCheck fail", e);
                asyncResponse.resume(new RestException(e));
            }
        });
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
    @ApiOperation(value = "Run a healthcheck against the broker")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Everything is OK"),
        @ApiResponse(code = 403, message = "Don't have admin permission"),
        @ApiResponse(code = 404, message = "Cluster doesn't exist"),
        @ApiResponse(code = 500, message = "Internal server error")})
    public void healthcheck(@Suspended AsyncResponse asyncResponse) {
        validateSuperUserAccess();
        internalRunHealthCheck()
                .thenAccept(__ -> {
                    LOG.info("[{}] Successfully run health check.", clientAppId());
                    asyncResponse.resume("ok");
                }).exceptionally(ex -> {
                    LOG.error("[{}] Fail to run health check.", clientAppId(), ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> internalRunHealthCheck() {
        String topic;
        PulsarClient client;
        try {
            String heartbeatNamespace = NamespaceService.getHeartbeatNamespace(
                    pulsar().getAdvertisedAddress(), pulsar().getConfiguration());
            topic = String.format("persistent://%s/%s", heartbeatNamespace, HEALTH_CHECK_TOPIC_SUFFIX);
            LOG.info("Running healthCheck with topic={}", topic);
            client = pulsar().getClient();
        } catch (PulsarServerException e) {
            LOG.error("[{}] Fail to run health check while get client.", clientAppId());
            throw new RestException(e);
        }
        String messageStr = UUID.randomUUID().toString();
        final String subscriptionName = "healthCheck-" + messageStr;
        // create non-partitioned topic manually and close the previous reader if present.
        return pulsar().getBrokerService().getTopic(topic, true).thenCompose(topicOptional -> {
            if (!topicOptional.isPresent()) {
                LOG.error("[{}] Fail to run health check while get topic {}. because get null value.",
                        clientAppId(), topic);
                throw new RestException(Status.NOT_FOUND,
                        String.format("Topic [%s] not found after create.", topic));
            }
            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            client.newProducer(Schema.STRING).topic(topic).createAsync()
                    .thenCompose(producer -> client.newReader(Schema.STRING)
                            .topic(topic)
                            .subscriptionName(subscriptionName)
                            .startMessageId(MessageId.latest)
                            .createAsync()
                            .exceptionally(createException -> {
                                producer.closeAsync().exceptionally(ex -> {
                                    LOG.error("[{}] Close producer fail while heath check.", clientAppId());
                                    return null;
                                });
                                throw FutureUtil.wrapToCompletionException(createException);
                            })
                            .thenCompose(reader -> producer.sendAsync(messageStr)
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
                                    }))
                    ).exceptionally(ex -> {
                        resultFuture.completeExceptionally(ex);
                        return null;
                    });
            return resultFuture;
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

    private synchronized void deleteDynamicConfigurationOnZk(String configName) {
        try {
            if (BrokerService.isDynamicConfiguration(configName)) {
                dynamicConfigurationResources().set(BROKER_SERVICE_CONFIGURATION_PATH, (old) -> {
                    if (old != null) {
                        old.remove(configName);
                    }
                    return old;
                });
                LOG.info("[{}] Deleted Service configuration {}", clientAppId(), configName);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Can't update non-dynamic configuration {}", clientAppId(), configName);
                }
                throw new RestException(Status.PRECONDITION_FAILED, " Can't update non-dynamic configuration");
            }
        } catch (RestException re) {
            throw re;
        } catch (Exception ie) {
            LOG.error("[{}] Failed to update configuration {}, {}", clientAppId(), configName, ie.getMessage(), ie);
            throw new RestException(ie);
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
}

