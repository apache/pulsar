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
package org.apache.pulsar.broker.service.replicator;

import static org.apache.pulsar.broker.admin.AdminResource.jsonMapper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;
import static org.apache.pulsar.replicator.api.AbstractReplicatorManager.formSubscriptionName;
import static org.apache.pulsar.replicator.function.ReplicatorFunction.CONF_REPLICATOR_NAMESPACE_VAL;
import static org.apache.pulsar.replicator.function.ReplicatorFunction.CONF_REPLICATOR_TENANT_VAL;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImpl;
import org.apache.pulsar.replicator.function.ReplicatorFunction;
import org.apache.pulsar.replicator.function.ReplicatorTopicData;
import org.apache.pulsar.replicator.function.utils.ReplicatorTopicDataSerDe;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition.FormDataContentDispositionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Utility class that gives Apis to integrate onboarding of external replicator providers.
 *
 */
public class ExternReplicatorUtil {

    /**
     * Register external-replicator for a topic
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param regionName
     * @return
     */
    public static Response internalRegisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName) {
        validateReplicatorTopic(pulsar, topicName, replicatorType, regionName);
        FunctionDetails functionDetails = createFunctionDetails(pulsar, topicName, replicatorType, regionName);
        String replicatorFilePath = null;

        try {
            replicatorFilePath = ReplicatorFunction.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (Exception e) {
            log.warn("Replicator function jar not found in classpath", e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Replicator function jar not found in classpath");
        }

        // create a replicator-function namespace if not exist yet (eg: /admin/pulsar/global/<provider>)
        String replicatorFunctionTopic = ReplicatorFunction.getFunctionTopicName(replicatorType, topicName.getNamespacePortion());
        createNamespaceIfNotCreated(pulsar, TopicName.get(replicatorFunctionTopic));

        if (pulsar.getWorkerService() == null) {
            // redirect request to function worker service
            try {
                pulsar.getFunctionAdminClient().functions().createFunction(convert(functionDetails), replicatorFilePath);
                return Response.status(Status.OK).build();
            } catch (PulsarServerException e) {
                log.warn("[{}]-{} Worker-service admin creation failed {}", topicName, replicatorType, e.getMessage());
                throw new RestException(Status.SERVICE_UNAVAILABLE,
                        "Worker-service admin creation failed " + e.getMessage());
            } catch (PulsarAdminException e) {
                log.warn("[{}]-{} Failed to create replicator-function {}", topicName, replicatorType, e.getMessage());
                throw new RestException(Status.INTERNAL_SERVER_ERROR,
                        "Worker-service admin creation failed " + e.getMessage());
            } catch (IOException e) {
                log.warn("[{}]-{} Failed to create function-config {}", topicName, replicatorType, e.getMessage());
                throw new RestException(Status.INTERNAL_SERVER_ERROR,
                        "Worker-service admin creation failed " + e.getMessage());
            }
        }

        return internalRegisterReplicatorOnTopic(pulsar, topicName, replicatorType, regionName, functionDetails,
                replicatorFilePath);
    }

    /**
     * Start/Stop external-replicator of a topic.
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param action
     */
    public static void internalUpdateReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName, Action action) {
        try {
            internalUpdateReplicatorOnTopicAsync(pulsar, topicName, replicatorType, regionName, action).get();
        } catch (Exception e) {
            log.error("Failed to update replicator function for topic {} {}-{}", topicName, replicatorType, action, e);
            throw new RestException(e);
        }
    }

    /**
     * Removes external-replicator of a topic.
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     */
    public static Response internalDeregisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName) {
        String functionName = formFunctionName(replicatorType, topicName);
        // stop replicator and remove subscription
        try {
            deleteReplicator(pulsar, topicName, replicatorType, regionName).get();
        } catch (Exception e) {
            log.warn("[{}]-{} Failed to delete replicator subscription {}", topicName, replicatorType, e.getMessage());
            // continue
        }
        if (pulsar.getWorkerService() == null) {
            // redirect request to function worker service
            try {
                pulsar.getFunctionAdminClient().functions().deleteFunction(CONF_REPLICATOR_TENANT_VAL,
                        CONF_REPLICATOR_NAMESPACE_VAL, functionName);
                return Response.status(Status.OK).build();
            } catch (PulsarServerException e) {
                log.warn("[{}]-{} Worker-service admin creation failed {}", topicName, replicatorType, e.getMessage());
                throw new RestException(Status.SERVICE_UNAVAILABLE,
                        "Worker-service admin creation failed " + e.getMessage());
            } catch (PulsarAdminException e) {
                log.warn("[{}]-{} Failed to create replicator-function {}", topicName, replicatorType, e.getMessage());
                throw new RestException(Status.INTERNAL_SERVER_ERROR,
                        "Worker-service admin creation failed " + e.getMessage());
            }
        } else {
            return FunctionsImpl.deregisterFunction(CONF_REPLICATOR_TENANT_VAL, CONF_REPLICATOR_NAMESPACE_VAL,
                    functionName, pulsar.getWorkerService());
        }
    }

    /**
     * Deletes the replicator of the topic : stops the replicator and replicator-consumer and then deletes the
     * replicator-subscription
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param regionName
     * @return
     */
    private static CompletableFuture<Void> deleteReplicator(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName) {
        CompletableFuture<Void> deleteResult = new CompletableFuture<>();
        try {
            PulsarAdmin admin = pulsar.getAdminClient();
            internalUpdateReplicatorOnTopicAsync(pulsar, topicName, replicatorType, regionName, Action.Stop)
                    .thenAccept(stopResult -> {
                        String subName = formSubscriptionName(replicatorType, regionName);
                        deleteSubscriptionAsyncWithRetry(topicName, subName, deleteResult, admin, pulsar.getExecutor(),
                                0 /* retry-count */);
                    }).exceptionally(ex -> {
                        deleteResult.completeExceptionally(ex);
                        return null;
                    });
        } catch (PulsarServerException e) {
            log.warn("[{}]-{} Admin creation failed {}", topicName, replicatorType, e.getMessage());
            deleteResult.completeExceptionally(e);
        } catch (Exception e) {
            log.warn("[{}]-{} Failed to delete replicator subscription {}", topicName, replicatorType, e.getMessage());
            deleteResult.completeExceptionally(e);
        }
        return deleteResult;
    }

    /**
     * Removing consumer is async which may fail delete subscription due to race-condition. so, retry if subscription
     * deletion fails.
     * 
     * @param topicName
     * @param subName
     * @param deleteResult
     * @param admin
     * @param executor
     * @param retryCount
     */
    private static void deleteSubscriptionAsyncWithRetry(TopicName topicName, String subName,
            CompletableFuture<Void> deleteResult, PulsarAdmin admin, ScheduledExecutorService executor,
            int retryCount) {
        admin.persistentTopics().deleteSubscriptionAsync(topicName.toString(), subName).thenAccept(deleteRes -> {
            deleteResult.complete(null);
        }).exceptionally(e -> {
            log.warn("[{}]-{} Failed to delete subscription {}, {}", topicName, subName, e.getMessage());
            if ((retryCount + 1) < 5 /* MAX_RETRY */) {
                executor.submit(() -> deleteSubscriptionAsyncWithRetry(topicName, subName, deleteResult, admin,
                        executor, retryCount + 1));
            } else {
                deleteResult.completeExceptionally(e);
            }

            return null;
        });
    }

    /**
     * Prepares the function config for replicator and submits the replicator-function.
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param regionName
     * @param functionDetails
     * @param replicatorFilePath
     * @return
     */
    private static Response internalRegisterReplicatorOnTopic(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName, FunctionDetails functionDetails,
            String replicatorFilePath) {

        InputStream inputStream = null;
        File file = null;
        try {
            inputStream = new FileInputStream(replicatorFilePath);
            file = new File(replicatorFilePath);
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Replicator function jar not found in classpath", e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Replicator function jar not found in classpath");
        }
        FormDataContentDispositionBuilder builder = FormDataContentDisposition.name(file.getName());
        FormDataContentDisposition fileDetail = builder.fileName(file.getName())
                .creationDate(new Date(file.lastModified())).build();

        return FunctionsImpl.registerFunction(CONF_REPLICATOR_TENANT_VAL, CONF_REPLICATOR_NAMESPACE_VAL,
                functionDetails.getName(), inputStream, fileDetail, functionDetails, pulsar.getWorkerService());
    }

    /**
     * publishes message with action:stop/start for a replicator-topic so, replicator-function can stop/start
     * replicator-provider for an appropriate topic
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param regionName
     * @param action
     * @return
     */
    private static CompletableFuture<Void> internalUpdateReplicatorOnTopicAsync(PulsarService pulsar,
            TopicName topicName, ReplicatorType replicatorType, String regionName, Action action) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        final String clusterName = pulsar.getConfiguration().getClusterName();
        PulsarClient client = pulsar.getBrokerService().getReplicationClient(clusterName);
        if (client == null) {
            throw new RestException(Status.NOT_FOUND, "Couldn't initialize client for cluster " + clusterName);
        }
        ProducerBuilder<byte[]> producerBuilder = client.newProducer() //
                .topic(ReplicatorFunction.getFunctionTopicName(replicatorType, topicName.getNamespacePortion())).sendTimeout(0, TimeUnit.SECONDS) //
                .maxPendingMessages(10) //
                .producerName(String.format("%s-%s", replicatorType.toString(), clusterName));
        ReplicatorTopicData topicActionData = new ReplicatorTopicData();
        topicActionData.setAction(action);
        topicActionData.setTopicName(topicName.toString());
        topicActionData.setRegionName(regionName);
        byte[] data = ReplicatorTopicDataSerDe.instance().serialize(topicActionData);
        // publish message that can be processed by pulsar function of a replicator-topic
        producerBuilder.createAsync().thenAccept(producer -> {
            producer.sendAsync(data).thenAccept(res -> {
                pulsar.getExecutor().submit(() -> producer.closeAsync());
                result.complete(null);
            }).exceptionally(e -> {
                pulsar.getExecutor().submit(() -> producer.closeAsync());
                result.completeExceptionally(e);
                return null;
            });
        }).exceptionally(ex -> {
            result.completeExceptionally(ex);
            return null;
        });
        return result;
    }

    private static FunctionDetails createFunctionDetails(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName) {
        String replicatorFunctionTopic = ReplicatorFunction.getFunctionTopicName(replicatorType, topicName.getNamespacePortion());
        String functionName = formFunctionName(replicatorType, topicName);
        String className = ReplicatorFunction.class.getName();

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        String replicatorTopicSerClassName = ReplicatorTopicDataSerDe.class.getName();
        Map<String, String> userConfigs = Maps.newHashMap();
        userConfigs.put(ReplicatorFunction.CONF_BROKER_SERVICE_URL, pulsar.getBrokerServiceUrl());
        userConfigs.put(ReplicatorFunction.CONF_ZK_SERVER_URL, pulsar.getConfiguration().getZookeeperServers());
        userConfigs.put(ReplicatorFunction.CONF_REPLICATION_TOPIC_NAME, topicName.toString());
        userConfigs.put(ReplicatorFunction.CONF_REPLICATION_REGION_NAME, regionName);
        String replicatorManagerClassName = ReplicatorRegistry.getReplicatorManagerName(replicatorType);
        userConfigs.put(ReplicatorFunction.CONF_REPLICATOR_MANAGER_CLASS_NAME, replicatorManagerClassName);
        functionDetailsBuilder.setTenant(CONF_REPLICATOR_TENANT_VAL).setNamespace(CONF_REPLICATOR_NAMESPACE_VAL)
                .setName(functionName).setClassName(className).setParallelism(1).setRuntime(Runtime.JAVA)
                .setAutoAck(true).putCustomSerdeInputs(replicatorFunctionTopic, replicatorTopicSerClassName)
                .putAllUserConfig(userConfigs);
        return functionDetailsBuilder.build();
    }

    protected static void createNamespaceIfNotCreated(PulsarService pulsar, TopicName topic) {
        try {
            String propertyPath = path(POLICIES, topic.getTenant());
            if (!pulsar.getConfigurationCache().propertiesCache().get(propertyPath).isPresent()) {
                TenantInfo TenantInfo = new TenantInfo();
                TenantInfo.setAdminRoles(pulsar.getConfiguration().getSuperUserRoles());
                Set<String> clusters = pulsar.getConfigurationCache().clustersListCache().get();
                clusters.remove("global");
                TenantInfo.setAllowedClusters(clusters);
                zkCreateOptimistic(pulsar, propertyPath, jsonMapper().writeValueAsBytes(TenantInfo));
            }
        } catch (Exception e) {
            log.warn("Failed to create property {} ", topic.getTenant(), e);
            throw new RestException(e);
        }
        try {
            String namespacePath = path(POLICIES, topic.getNamespace());
            if (!pulsar.getConfigurationCache().policiesCache().get(namespacePath).isPresent()) {
                Policies policies = new Policies();
                Set<String> clusters = pulsar.getConfigurationCache().clustersListCache().get();
                clusters.remove("global");
                policies.replication_clusters = clusters;
                zkCreateOptimistic(pulsar, namespacePath, jsonMapper().writeValueAsBytes(policies));
            }
        } catch (Exception e) {
            log.warn("Failed to create namespace policies {}/{} ", topic.getTenant(), topic.getNamespace(), e);
            throw new RestException(e);
        }
    }

    /**
     * Calls actual replicator-provider and validates input replicator properties. eg. Every provider requires mandatory
     * input-properties in a defined format. so, this utility calls provider to validate properties.
     * 
     * @param pulsar
     * @param topicName
     * @param replicatorType
     * @param regionName
     * @throws RestException
     */
    private static void validateReplicatorTopic(PulsarService pulsar, TopicName topicName,
            ReplicatorType replicatorType, String regionName) throws RestException {

        Policies policies;
        try {
            policies = pulsar.getConfigurationCache().policiesCache()
                    .get(AdminResource.path(POLICIES, topicName.getNamespace()))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            if (policies.replicatorPolicies != null && policies.replicatorPolicies.containsKey(replicatorType)
                    && policies.replicatorPolicies.get(replicatorType).containsKey(regionName)) {
                ReplicatorPolicies replicatorMetadata = policies.replicatorPolicies.get(replicatorType).get(regionName);
                if (replicatorMetadata == null || replicatorMetadata.topicNameMapping == null
                        || !replicatorMetadata.topicNameMapping.containsKey(topicName.getLocalName())) {
                    log.warn("Replicator mapping not found for topic {} with replicator {}", topicName, replicatorType);
                    throw new RestException(Status.NOT_FOUND,
                            "Replicator-stream name mapping not found for topic " + topicName + ", " + replicatorType);
                }
            } else {
                throw new RestException(Status.NOT_FOUND, "Replicator is not configured for namespace "
                        + topicName.getNamespace() + ", " + replicatorType);
            }
        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to validate Replicator mapping  for topic {} with replicator {}", topicName,
                    replicatorType);
            throw new RestException(e);
        }
    }

    protected static String formFunctionName(ReplicatorType replicatorType, TopicName topicName) {
        return String.format("%s-%s-%s-%s", replicatorType.toString(), topicName.getTenant(),
                topicName.getNamespacePortion(), topicName.getLocalName());
    }

    private static void zkCreateOptimistic(PulsarService pulsar, String path, byte[] content)
            throws KeeperException, InterruptedException {
        ZkUtils.createFullPathOptimistic(pulsar.getGlobalZkCache().getZooKeeper(), path, content,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails convert(
            FunctionDetails functionConfig) throws IOException {
        org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails.Builder functionConfigBuilder = org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails
                .newBuilder();
        Utils.mergeJson(Utils.printJson(functionConfig), functionConfigBuilder);
        return functionConfigBuilder.build();
    }

    private static final Logger log = LoggerFactory.getLogger(ExternReplicatorUtil.class);
}
