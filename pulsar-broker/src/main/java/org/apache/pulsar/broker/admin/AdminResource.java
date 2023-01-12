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
package org.apache.pulsar.broker.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;

@Slf4j
public abstract class AdminResource extends PulsarWebResource {

    protected NamespaceName namespaceName;
    protected TopicName topicName;

    protected BookKeeper bookKeeper() {
        return pulsar().getBookKeeperClient();
    }

    /**
     * Get the domain of the topic (whether it's persistent or non-persistent).
     */
    protected String domain() {
        if (uri.getPath().startsWith("persistent/")) {
            return "persistent";
        } else if (uri.getPath().startsWith("non-persistent/")) {
            return "non-persistent";
        } else {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "domain() invoked from wrong resource");
        }
    }

    // This is a stub method for Mockito
    @Override
    public void validateSuperUserAccess() {
        super.validateSuperUserAccess();
    }

    // This is a stub method for Mockito
    @Override
    protected void validateAdminAccessForTenant(String property) {
        super.validateAdminAccessForTenant(property);
    }

    // This is a stub method for Mockito
    @Override
    protected void validateBundleOwnership(String property, String cluster, String namespace, boolean authoritative,
            boolean readOnly, NamespaceBundle bundle) {
        super.validateBundleOwnership(property, cluster, namespace, authoritative, readOnly, bundle);
    }

    // This is a stub method for Mockito
    @Override
    protected boolean isLeaderBroker() {
        return super.isLeaderBroker();
    }

    /**
     * Checks whether the broker is allowed to do read-write operations based on the existence of a node in
     * configuration metadata-store.
     *
     * @throws WebApplicationException
     *             if broker has a read only access if broker is not connected to the configuration metadata-store
     */

    public void validatePoliciesReadOnlyAccess() {
        try {
            validatePoliciesReadOnlyAccessAsync().join();
        } catch (CompletionException ce) {
            throw new RestException(ce.getCause());
        }
    }

    public CompletableFuture<Void> validatePoliciesReadOnlyAccessAsync() {

        return pulsar().getPulsarResources().getNamespaceResources().getPoliciesReadOnlyAsync()
                .thenAccept(arePoliciesReadOnly -> {
                    if (arePoliciesReadOnly) {
                        if (log.isDebugEnabled()) {
                            log.debug("Policies are read-only. Broker cannot do read-write operations");
                        }
                        throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
                    } else {
                        // Do nothing, just log the message.
                        if (log.isDebugEnabled()) {
                            log.debug("Broker is allowed to make read-write operations");
                        }
                    }
                });
    }

    protected CompletableFuture<Void> tryCreatePartitionsAsync(int numPartitions) {
        if (!topicName.isPersistent()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            futures.add(tryCreatePartitionAsync(i));
        }
        return FutureUtil.waitForAll(futures);
    }

    protected CompletableFuture<Void> tryCreateExtendedPartitionsAsync(int oldNumPartitions, int numPartitions) {
        if (!topicName.isPersistent()) {
            return CompletableFuture.completedFuture(null);
        }
        if (numPartitions <= oldNumPartitions) {
            return CompletableFuture.failedFuture(new RestException(Status.NOT_ACCEPTABLE,
                    "Number of new partitions must be greater than existing number of partitions"));
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(numPartitions - oldNumPartitions);
        for (int i = oldNumPartitions; i < numPartitions; i++) {
            futures.add(tryCreatePartitionAsync(i));
        }
        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> tryCreatePartitionAsync(final int partition) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        getPulsarResources().getTopicResources().createPersistentTopicAsync(topicName.getPartition(partition))
                .thenAccept(r -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Topic partition {} created.", clientAppId(), topicName.getPartition(partition));
                    }
                    result.complete(null);
                }).exceptionally(ex -> {
                    if (ex.getCause() instanceof AlreadyExistsException) {
                        log.info("[{}] Topic partition {} is exists, doing nothing.", clientAppId(),
                                topicName.getPartition(partition));
                        result.complete(null);
                    } else if (ex.getCause() instanceof BadVersionException) {
                        log.warn("[{}] Partitioned topic {} is already created.", clientAppId(),
                                topicName.getPartition(partition));
                        // metadata-store api returns BadVersionException if node already exists while creating the
                        // resource
                        result.complete(null);
                    } else {
                        log.error("[{}] Fail to create topic partition {}", clientAppId(),
                                topicName.getPartition(partition), ex.getCause());
                        result.completeExceptionally(ex.getCause());
                    }
                    return null;
                });
        return result;
    }

    protected void validateNamespaceName(String property, String namespace) {
        try {
            this.namespaceName = NamespaceName.get(property, namespace);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Invalid namespace name [{}/{}]", clientAppId(), property, namespace);
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace name is not valid");
        }
    }

    protected void validateGlobalNamespaceOwnership() {
        try {
            validateGlobalNamespaceOwnership(this.namespaceName);
        } catch (IllegalArgumentException e) {
            throw new RestException(Status.PRECONDITION_FAILED, "Tenant name or namespace is not valid");
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.warn("Failed to validate global cluster configuration : ns={}  emsg={}", namespaceName, e.getMessage());
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Failed to validate global cluster configuration");
        }
    }
    @Deprecated
    protected void validateNamespaceName(String property, String cluster, String namespace) {
        try {
            this.namespaceName = NamespaceName.get(property, cluster, namespace);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Invalid namespace name [{}/{}/{}]", clientAppId(), property, cluster, namespace);
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace name is not valid");
        }
    }

    protected void validateTopicName(String property, String namespace, String encodedTopic) {
        String topic = Codec.decode(encodedTopic);
        try {
            this.namespaceName = NamespaceName.get(property, namespace);
            this.topicName = TopicName.get(domain(), namespaceName, topic);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Invalid topic name [{}://{}/{}/{}]", clientAppId(), domain(), property, namespace, topic);
            throw new RestException(Status.PRECONDITION_FAILED, "Topic name is not valid");
        }
    }

    protected void validatePersistentTopicName(String property, String namespace, String encodedTopic) {
        validateTopicName(property, namespace, encodedTopic);
        if (topicName.getDomain() != TopicDomain.persistent) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Need to provide a persistent topic name");
        }
    }

    protected void validatePartitionedTopicName(String tenant, String namespace, String encodedTopic) {
        // first, it has to be a validate topic name
        validateTopicName(tenant, namespace, encodedTopic);
        // second, "-partition-" is not allowed
        if (encodedTopic.contains(TopicName.PARTITIONED_TOPIC_SUFFIX)) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "Partitioned Topic Name should not contain '-partition-'");
        }
    }

    protected CompletableFuture<Void> validatePartitionedTopicMetadataAsync() {
        return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
                .thenAccept(metadata -> {
                    if (metadata.partitions < 1) {
                        throw new RestException(Status.CONFLICT, "Topic is not partitioned topic");
                    }
                });
    }

    @Deprecated
    protected void validateTopicName(String property, String cluster, String namespace, String encodedTopic) {
        String topic = Codec.decode(encodedTopic);
        try {
            this.namespaceName = NamespaceName.get(property, cluster, namespace);
            this.topicName = TopicName.get(domain(), namespaceName, topic);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Invalid topic name {}://{}/{}/{}/{}", clientAppId(), domain(), property, cluster,
                    namespace, topic);
            throw new RestException(Status.PRECONDITION_FAILED, "Topic name is not valid");
        }
    }

    @Deprecated
    protected void validatePersistentTopicName(String property, String cluster, String namespace, String encodedTopic) {
        validateTopicName(property, cluster, namespace, encodedTopic);
        if (topicName.getDomain() != TopicDomain.persistent) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Need to provide a persistent topic name");
        }
    }

    protected WorkerService validateAndGetWorkerService() {
        try {
            return pulsar().getWorkerService();
        } catch (UnsupportedOperationException e) {
            throw new RestException(Status.CONFLICT, e.getMessage());
        }
    }

    protected Policies getNamespacePolicies(NamespaceName namespaceName) {
        try {
            Policies policies = namespaceResources().getPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            // fetch bundles from LocalZK-policies
            BundlesData bundleData = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName).getBundlesData();
            policies.bundles = bundleData != null ? bundleData : policies.bundles;
            if (policies.is_allow_auto_update_schema == null) {
                // the type changed from boolean to Boolean. return broker value here for keeping compatibility.
                policies.is_allow_auto_update_schema = pulsar().getConfig().isAllowAutoUpdateSchemaEnabled();
            }

            return policies;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

    }

    protected CompletableFuture<Policies> getNamespacePoliciesAsync(NamespaceName namespaceName) {
        return namespaceResources().getPoliciesAsync(namespaceName).thenCompose(policies -> {
            if (policies.isPresent()) {
                return pulsar()
                        .getNamespaceService()
                        .getNamespaceBundleFactory()
                        .getBundlesAsync(namespaceName)
                        .thenCompose(bundles -> {
                    BundlesData bundleData = null;
                    try {
                        bundleData = bundles.getBundlesData();
                    } catch (Exception e) {
                        log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
                        return FutureUtil.failedFuture(new RestException(e));
                    }
                    policies.get().bundles = bundleData != null ? bundleData : policies.get().bundles;
                    if (policies.get().is_allow_auto_update_schema == null) {
                        // the type changed from boolean to Boolean. return broker value here for keeping compatibility.
                        policies.get().is_allow_auto_update_schema = pulsar().getConfig()
                                .isAllowAutoUpdateSchemaEnabled();
                    }
                    return CompletableFuture.completedFuture(policies.get());
                });
            } else {
                return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            }
        });
    }

    protected BacklogQuota namespaceBacklogQuota(NamespaceName namespace,
                                                 BacklogQuota.BacklogQuotaType backlogQuotaType) {
        return pulsar().getBrokerService().getBacklogQuotaManager()
                .getBacklogQuota(namespace, backlogQuotaType);
    }

    protected CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsyncWithRetry(TopicName topicName) {
        return getTopicPoliciesAsyncWithRetry(topicName, false);
    }

    protected CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsyncWithRetry(TopicName topicName,
                                                                                        boolean isGlobal) {
        try {
            checkTopicLevelPolicyEnable();
            return pulsar().getTopicPoliciesService()
                    .getTopicPoliciesAsyncWithRetry(topicName, null, pulsar().getExecutor(), isGlobal);
        } catch (Exception e) {
            log.error("[{}] Failed to get topic policies {}", clientAppId(), topicName, e);
            return FutureUtil.failedFuture(e);
        }
    }

    protected boolean checkBacklogQuota(BacklogQuota quota, RetentionPolicies retention) {
        if (retention == null
                || (retention.getRetentionSizeInMB() <= 0 && retention.getRetentionTimeInMinutes() <= 0)) {
            return true;
        }
        if (quota == null) {
            quota = pulsar().getBrokerService().getBacklogQuotaManager().getDefaultQuota();
        }

        if (retention.getRetentionSizeInMB() > 0
                && quota.getLimitSize() >= (retention.getRetentionSizeInMB() * 1024 * 1024)) {
            return false;
        }
        // time based quota is in second
        if (retention.getRetentionTimeInMinutes() > 0
                && quota.getLimitTime() >= retention.getRetentionTimeInMinutes() * 60) {
            return false;
        }
        return true;
    }

    protected void checkTopicLevelPolicyEnable() {
        if (!config().isTopicLevelPoliciesEnabled()) {
            throw new RestException(Status.METHOD_NOT_ALLOWED,
                    "Topic level policies is disabled, to enable the topic level policy and retry.");
        }
    }

    protected DispatchRateImpl dispatchRate() {
        return DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(config().getDispatchThrottlingRatePerTopicInMsg())
                .dispatchThrottlingRateInByte(config().getDispatchThrottlingRatePerTopicInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    protected DispatchRateImpl subscriptionDispatchRate() {
        return DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(config().getDispatchThrottlingRatePerSubscriptionInMsg())
                .dispatchThrottlingRateInByte(config().getDispatchThrottlingRatePerSubscriptionInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    protected DispatchRateImpl replicatorDispatchRate() {
        return DispatchRateImpl.builder()
                .dispatchThrottlingRateInMsg(config().getDispatchThrottlingRatePerReplicatorInMsg())
                .dispatchThrottlingRateInByte(config().getDispatchThrottlingRatePerReplicatorInByte())
                .ratePeriodInSecond(1)
                .build();
    }

    protected SubscribeRate subscribeRate() {
        return new SubscribeRate(
                pulsar().getConfiguration().getSubscribeThrottlingRatePerConsumer(),
                pulsar().getConfiguration().getSubscribeRatePeriodPerConsumerInSecond()
        );
    }

    protected AutoSubscriptionCreationOverrideImpl autoSubscriptionCreationOverride() {
        boolean allowAutoSubscriptionCreation = pulsar().getConfiguration().isAllowAutoSubscriptionCreation();
        return AutoSubscriptionCreationOverrideImpl.builder()
                .allowAutoSubscriptionCreation(allowAutoSubscriptionCreation)
                .build();
    }

    public static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    protected Set<String> clusters() {
        try {
            // Remove "global" cluster from returned list
            Set<String> clusters = clusterResources().list().stream()
                    .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster)).collect(Collectors.toSet());
            return clusters;
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Set<String>> clustersAsync() {
        return clusterResources().listAsync()
                .thenApply(list ->
                        list.stream()
                                .filter(cluster -> !Constants.GLOBAL_CLUSTER.equals(cluster))
                                .collect(Collectors.toSet())
                );
    }

    protected void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    protected PartitionedTopicMetadata getPartitionedTopicMetadata(TopicName topicName,
                                                                   boolean authoritative,
                                                                   boolean checkAllowAutoCreation) {
        return sync(() -> getPartitionedTopicMetadataAsync(topicName, authoritative, checkAllowAutoCreation));
    }

    protected CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(
            TopicName topicName, boolean authoritative, boolean checkAllowAutoCreation) {
        // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
        // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
        // producer/consumer
        return validateClusterOwnershipAsync(topicName.getCluster())
                .thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(topicName.getNamespaceObject()))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.LOOKUP))
                .thenCompose(__ -> {
                    if (checkAllowAutoCreation) {
                        return pulsar().getBrokerService()
                                .fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName);
                    } else {
                        return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName);
                    }
                });
    }

   protected void validateClusterExists(String cluster) {
        try {
            if (!clusterResources().getCluster(cluster).isPresent()) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected Policies getNamespacePolicies(String tenant, String cluster, String namespace) {
        NamespaceName ns = NamespaceName.get(tenant, cluster, namespace);

        return getNamespacePolicies(ns);
    }

    protected boolean isNamespaceReplicated(NamespaceName namespaceName) {
        return getNamespaceReplicatedClusters(namespaceName).size() > 1;
    }

    protected Set<String> getNamespaceReplicatedClusters(NamespaceName namespaceName) {
        try {
            final Policies policies = namespaceResources().getPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            return policies.replication_clusters;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Set<String>> getNamespaceReplicatedClustersAsync(NamespaceName namespaceName) {
        return namespaceResources().getPoliciesAsync(namespaceName)
                .thenApply(policies -> {
                    if (policies.isPresent()) {
                        return policies.get().replication_clusters;
                    } else {
                        throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                    }
                });
    }

    protected List<String> getPartitionedTopicList(TopicDomain topicDomain) {
        try {
            return namespaceResources().getPartitionedTopicResources()
                    .listPartitionedTopicsAsync(namespaceName, topicDomain)
                    .join();
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<List<String>> getPartitionedTopicListAsync(TopicDomain topicDomain) {
        return namespaceResources().getPartitionedTopicResources()
                .listPartitionedTopicsAsync(namespaceName, topicDomain);
    }

    protected List<String> getTopicPartitionList(TopicDomain topicDomain) {
        try {
            return getPulsarResources().getTopicResources().getExistingPartitions(topicName)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("[{}] Failed to get topic partition list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }
    }

    protected void internalCreatePartitionedTopic(AsyncResponse asyncResponse, int numPartitions,
                                                  boolean createLocalTopicOnly) {
        internalCreatePartitionedTopic(asyncResponse, numPartitions, createLocalTopicOnly, null);
    }

    protected void internalCreatePartitionedTopic(AsyncResponse asyncResponse, int numPartitions,
                                                  boolean createLocalTopicOnly, Map<String, String> properties) {
        Integer maxTopicsPerNamespace = null;

        try {
            Policies policies = getNamespacePolicies(namespaceName);
            maxTopicsPerNamespace = policies.max_topics_per_namespace;
        } catch (RestException e) {
            if (e.getResponse().getStatus() != Status.NOT_FOUND.getStatusCode()) {
                log.error("[{}] Failed to create partitioned topic {}", clientAppId(), namespaceName, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
                return;
            }
        }

        try {
            if (maxTopicsPerNamespace == null) {
                maxTopicsPerNamespace = pulsar().getConfig().getMaxTopicsPerNamespace();
            }

            // new create check
            if (maxTopicsPerNamespace > 0 && !pulsar().getBrokerService().isSystemTopic(topicName)) {
                List<String> partitionedTopics = getTopicPartitionList(TopicDomain.persistent);
                // exclude created system topic
                long topicsCount =
                        partitionedTopics.stream().filter(t ->
                                        !pulsar().getBrokerService().isSystemTopic(TopicName.get(t))).count();
                if (topicsCount + numPartitions > maxTopicsPerNamespace) {
                    log.error("[{}] Failed to create partitioned topic {}, "
                            + "exceed maximum number of topics in namespace", clientAppId(), topicName);
                    resumeAsyncResponseExceptionally(asyncResponse, new RestException(Status.PRECONDITION_FAILED,
                            "Exceed maximum number of topics in namespace."));
                    return;
                }
            }
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), namespaceName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }

        final int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
        try {
            validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.CREATE_TOPIC);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
            return;
        }
        if (numPartitions <= 0) {
            asyncResponse.resume(new RestException(Status.NOT_ACCEPTABLE,
                    "Number of partitions should be more than 0"));
            return;
        }
        if (maxPartitions > 0 && numPartitions > maxPartitions) {
            asyncResponse.resume(new RestException(Status.NOT_ACCEPTABLE,
                    "Number of partitions should be less than or equal to " + maxPartitions));
            return;
        }

        CompletableFuture<Void> createLocalFuture = new CompletableFuture<>();
        checkTopicExistsAsync(topicName).thenAccept(exists -> {
            if (exists) {
                log.warn("[{}] Failed to create already existing topic {}", clientAppId(), topicName);
                asyncResponse.resume(new RestException(Status.CONFLICT, "This topic already exists"));
                return;
            }

            provisionPartitionedTopicPath(numPartitions, createLocalTopicOnly, properties)
                    .thenCompose(ignored -> tryCreatePartitionsAsync(numPartitions))
                    .whenComplete((ignored, ex) -> {
                        if (ex != null) {
                            createLocalFuture.completeExceptionally(ex);
                            return;
                        }
                        createLocalFuture.complete(null);
                    });
        }).exceptionally(ex -> {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, ex);
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });

        List<String> replicatedClusters = new ArrayList<>();
        if (!createLocalTopicOnly && topicName.isGlobal() && isNamespaceReplicated(namespaceName)) {
            getNamespaceReplicatedClusters(namespaceName)
                    .stream().filter(cluster -> !cluster.equals(pulsar().getConfiguration().getClusterName()))
                    .forEach(replicatedClusters::add);
        }
        createLocalFuture.whenComplete((ignored, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to create partitions for topic {}", clientAppId(), topicName, ex.getCause());
                if (ex.getCause() instanceof RestException) {
                    asyncResponse.resume(ex.getCause());
                } else {
                    resumeAsyncResponseExceptionally(asyncResponse, ex.getCause());
                }
                return;
            }

            if (!replicatedClusters.isEmpty()) {
                replicatedClusters.forEach(cluster -> {
                    pulsar().getPulsarResources().getClusterResources().getClusterAsync(cluster)
                            .thenAccept(clusterDataOp -> {
                                ((TopicsImpl) pulsar().getBrokerService()
                                        .getClusterPulsarAdmin(cluster, clusterDataOp).topics())
                                        .createPartitionedTopicAsync(
                                                topicName.getPartitionedTopicName(), numPartitions, true, null);
                            })
                            .exceptionally(throwable -> {
                                log.error("Failed to create partition topic in cluster {}.", cluster, throwable);
                                return null;
                            });
                });
            }

            log.info("[{}] Successfully created partitions for topic {} in cluster {}",
                    clientAppId(), topicName, pulsar().getConfiguration().getClusterName());
            asyncResponse.resume(Response.noContent().build());
        });
    }

    /**
     * Check the exists topics contains the given topic.
     * Since there are topic partitions and non-partitioned topics in Pulsar, must ensure both partitions
     * and non-partitioned topics are not duplicated. So, if compare with a partition name, we should compare
     * to the partitioned name of this partition.
     *
     * @param topicName given topic name
     */
    protected CompletableFuture<Boolean> checkTopicExistsAsync(TopicName topicName) {
        return pulsar().getNamespaceService().getListOfTopics(topicName.getNamespaceObject(),
                CommandGetTopicsOfNamespace.Mode.ALL)
                .thenCompose(topics -> {
                    boolean exists = false;
                    for (String topic : topics) {
                        if (topicName.getPartitionedTopicName().equals(
                                TopicName.get(topic).getPartitionedTopicName())) {
                            exists = true;
                            break;
                        }
                    }
                    return CompletableFuture.completedFuture(exists);
                });
    }

    private CompletableFuture<Void> provisionPartitionedTopicPath(int numPartitions,
                                                                  boolean createLocalTopicOnly,
                                                                  Map<String, String> properties) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        namespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopicAsync(topicName, new PartitionedTopicMetadata(numPartitions, properties))
                .whenComplete((ignored, ex) -> {
                    if (ex != null) {
                        if (ex instanceof AlreadyExistsException) {
                            if (createLocalTopicOnly) {
                                future.complete(null);
                                return;
                            }
                            log.warn("[{}] Failed to create already existing partitioned topic {}",
                                    clientAppId(), topicName);
                            future.completeExceptionally(
                                    new RestException(Status.CONFLICT, "Partitioned topic already exists"));
                        } else if (ex instanceof BadVersionException) {
                            log.warn("[{}] Failed to create partitioned topic {}: concurrent modification",
                                    clientAppId(), topicName);
                            future.completeExceptionally(
                                    new RestException(Status.CONFLICT, "Concurrent modification"));
                        } else {
                            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, ex);
                            future.completeExceptionally(new RestException(ex.getCause()));
                        }
                        return;
                    }
                    log.info("[{}] Successfully created partitioned topic {}", clientAppId(), topicName);
                    future.complete(null);
                });
        return future;
    }

    protected CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync() {
        return validateTopicPolicyOperationAsync(topicName,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ)
                .thenCompose((__) -> getSchemaCompatibilityStrategyAsyncWithoutAuth()).whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("[{}] Failed to get schema compatibility strategy of topic {} {}",
                                clientAppId(), topicName, ex);
                    }
                });
    }

    protected CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsyncWithoutAuth() {
        CompletableFuture<SchemaCompatibilityStrategy> future = CompletableFuture.completedFuture(null);
        if (config().isTopicLevelPoliciesEnabled()) {
            future = getTopicPoliciesAsyncWithRetry(topicName)
                    .thenApply(op -> op.map(TopicPolicies::getSchemaCompatibilityStrategy).orElse(null));
        }

        return future.thenCompose((topicSchemaCompatibilityStrategy) -> {
            if (!SchemaCompatibilityStrategy.isUndefined(topicSchemaCompatibilityStrategy)) {
                return CompletableFuture.completedFuture(topicSchemaCompatibilityStrategy);
            }
            return getNamespacePoliciesAsync(namespaceName).thenApply(policies -> {
                SchemaCompatibilityStrategy schemaCompatibilityStrategy =
                        policies.schema_compatibility_strategy;
                if (SchemaCompatibilityStrategy.isUndefined(schemaCompatibilityStrategy)) {
                    schemaCompatibilityStrategy = SchemaCompatibilityStrategy.fromAutoUpdatePolicy(
                            policies.schema_auto_update_compatibility_strategy);
                    if (SchemaCompatibilityStrategy.isUndefined(schemaCompatibilityStrategy)) {
                        schemaCompatibilityStrategy = pulsar().getConfig().getSchemaCompatibilityStrategy();
                    }
                }
                return schemaCompatibilityStrategy;
            });
        });
    }

    @CanIgnoreReturnValue
    public static <T> T checkNotNull(T reference) {
        return Objects.requireNonNull(reference);
    }

    protected void checkNotNull(Object o, String errorMessage) {
        if (o == null) {
            throw new RestException(Status.BAD_REQUEST, errorMessage);
        }
    }

    protected boolean isManagedLedgerNotFoundException(Throwable cause) {
        return cause instanceof ManagedLedgerException.MetadataNotFoundException
                || cause instanceof MetadataStoreException.NotFoundException;
    }

    protected void checkArgument(boolean b, String errorMessage) {
        if (!b) {
            throw new RestException(Status.BAD_REQUEST, errorMessage);
        }
    }

    protected void validatePersistencePolicies(PersistencePolicies persistence) {
        checkNotNull(persistence, "persistence policies should not be null");
        final ServiceConfiguration config = pulsar().getConfiguration();
        checkArgument(persistence.getBookkeeperEnsemble() <= config.getManagedLedgerMaxEnsembleSize(),
                "Bookkeeper-Ensemble must be <= " + config.getManagedLedgerMaxEnsembleSize());
        checkArgument(persistence.getBookkeeperWriteQuorum() <= config.getManagedLedgerMaxWriteQuorum(),
                "Bookkeeper-WriteQuorum must be <= " + config.getManagedLedgerMaxWriteQuorum());
        checkArgument(persistence.getBookkeeperAckQuorum() <= config.getManagedLedgerMaxAckQuorum(),
                "Bookkeeper-AckQuorum must be <= " + config.getManagedLedgerMaxAckQuorum());
        checkArgument(
                (persistence.getBookkeeperEnsemble() >= persistence.getBookkeeperWriteQuorum())
                        && (persistence.getBookkeeperWriteQuorum() >= persistence.getBookkeeperAckQuorum()),
                String.format("Bookkeeper Ensemble (%s) >= WriteQuorum (%s) >= AckQuorum (%s)",
                        persistence.getBookkeeperEnsemble(), persistence.getBookkeeperWriteQuorum(),
                        persistence.getBookkeeperAckQuorum()));

    }

    /**
     * Check current exception whether is redirect exception.
     *
     * @param ex The throwable.
     * @return Whether is redirect exception
     */
    protected static boolean isRedirectException(Throwable ex) {
        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
        return realCause instanceof WebApplicationException
                && ((WebApplicationException) realCause).getResponse().getStatus()
                == Status.TEMPORARY_REDIRECT.getStatusCode();
    }

    protected static boolean isNotFoundException(Throwable ex) {
        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
        return realCause instanceof WebApplicationException
                && ((WebApplicationException) realCause).getResponse().getStatus()
                == Status.NOT_FOUND.getStatusCode();
    }

    protected static String getTopicNotFoundErrorMessage(String topic) {
        return String.format("Topic %s not found", topic);
    }

    protected static String getPartitionedTopicNotFoundErrorMessage(String topic) {
        return String.format("Partitioned Topic %s not found", topic);
    }

    protected static String getSubNotFoundErrorMessage(String topic, String subscription) {
        return String.format("Subscription %s not found for topic %s", subscription, topic);
    }

    protected List<String> filterSystemTopic(List<String> topics, boolean includeSystemTopic) {
        return topics.stream()
                .filter(topic -> includeSystemTopic ? true : !pulsar().getBrokerService().isSystemTopic(topic))
                .collect(Collectors.toList());
    }
}
