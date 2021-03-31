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
package org.apache.pulsar.broker.admin;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.common.util.Codec.decode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AdminResource extends PulsarWebResource {
    private static final Logger log = LoggerFactory.getLogger(AdminResource.class);
    public static final String POLICIES_READONLY_FLAG_PATH = "/admin/flags/policies-readonly";
    public static final String PARTITIONED_TOPIC_PATH_ZNODE = "partitioned-topics";
    private static final String MANAGED_LEDGER_PATH_ZNODE = "/managed-ledgers";

    protected BookKeeper bookKeeper() {
        return pulsar().getBookKeeperClient();
    }

    protected ZooKeeper localZk() {
        return pulsar().getZkClient();
    }

    protected ZooKeeperCache localZkCache() {
        return pulsar().getLocalZkCache();
    }

    protected LocalZooKeeperCacheService localCacheService() {
        return pulsar().getLocalZkCacheService();
    }

    protected void localZKCreate(String path, byte[] content) throws Exception {
        localZk().create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
    protected void validateNamespaceOwnershipWithBundles(String property, String cluster, String namespace,
            boolean authoritative, boolean readOnly, BundlesData bundleData) {
        super.validateNamespaceOwnershipWithBundles(property, cluster, namespace, authoritative, readOnly, bundleData);
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
        boolean arePoliciesReadOnly = true;

        try {
            arePoliciesReadOnly = pulsar().getPulsarResources().getNamespaceResources()
                    .exists(POLICIES_READONLY_FLAG_PATH);
        } catch (Exception e) {
            log.warn("Unable to fetch contents of [{}] from global zookeeper", POLICIES_READONLY_FLAG_PATH, e);
            throw new RestException(e);
        }

        if (arePoliciesReadOnly) {
            log.debug("Policies are read-only. Broker cannot do read-write operations");
            throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
        } else {
            // Do nothing, just log the message.
            log.debug("Broker is allowed to make read-write operations");
        }
    }

    protected CompletableFuture<Void> tryCreatePartitionsAsync(int numPartitions) {
        if (!topicName.isPersistent()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            futures.add(tryCreatePartitionAsync(i, null));
        }
        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> tryCreatePartitionAsync(final int partition, CompletableFuture<Void> reuseFuture) {
        CompletableFuture<Void> result = reuseFuture == null ? new CompletableFuture<>() : reuseFuture;
        Optional<MetadataStoreExtended> localStore = getPulsarResources().getLocalMetadataStore();
        if (!localStore.isPresent()) {
            result.completeExceptionally(new IllegalStateException("metadata store not initialized"));
            return result;
        }
        localStore.get()
                .put(ZkAdminPaths.managedLedgerPath(topicName.getPartition(partition)), new byte[0], Optional.of(-1L))
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

    protected NamespaceName namespaceName;

    protected void validateNamespaceName(String property, String namespace) {
        try {
            this.namespaceName = NamespaceName.get(property, namespace);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create namespace with invalid name {}", clientAppId(), namespace, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace name is not valid");
        }
    }

    protected void validateGlobalNamespaceOwnership(String property, String namespace) {
        try {
            this.namespaceName = NamespaceName.get(property, namespace);
            validateGlobalNamespaceOwnership(this.namespaceName);
        } catch (IllegalArgumentException e) {
            throw new RestException(Status.PRECONDITION_FAILED, "Tenant name or namespace is not valid");
        } catch (RestException re) {
            if (re.getResponse().getStatus() == Status.NOT_FOUND.getStatusCode()) {
                throw new RestException(Status.NOT_FOUND, "Namespace not found");
            }
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace does not have any clusters configured");
        } catch (Exception e) {
            log.warn("Failed to validate global cluster configuration : ns={}  emsg={}", namespace, e.getMessage());
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Failed to validate global cluster configuration");
        }
    }
    @Deprecated
    protected void validateNamespaceName(String property, String cluster, String namespace) {
        try {
            this.namespaceName = NamespaceName.get(property, cluster, namespace);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create namespace with invalid name {}", clientAppId(), namespace, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Namespace name is not valid");
        }
    }

    protected TopicName topicName;

    protected void validateTopicName(String property, String namespace, String encodedTopic) {
        String topic = Codec.decode(encodedTopic);
        try {
            this.namespaceName = NamespaceName.get(property, namespace);
            this.topicName = TopicName.get(domain(), namespaceName, topic);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to validate topic name {}://{}/{}/{}", clientAppId(), domain(), property, namespace,
                    topic, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Topic name is not valid");
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

    protected void validatePartitionedTopicMetadata(String tenant, String namespace, String encodedTopic) {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata =
                    pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).get();
            if (partitionedTopicMetadata.partitions < 1) {
                throw new RestException(Status.CONFLICT, "Topic is not partitioned topic");
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to validate partitioned topic metadata {}://{}/{}/{}",
                    domain(), tenant, namespace, topicName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Check topic partition meta failed.");
        }
    }

    protected void validateTopicExistedAndCheckAllowAutoCreation(String tenant, String namespace,
                                                                 String encodedTopic, boolean checkAllowAutoCreation) {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata =
                    pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).get();
            if (partitionedTopicMetadata.partitions < 1) {
                if (!pulsar().getNamespaceService().checkTopicExists(topicName).get()
                        && checkAllowAutoCreation
                        && !pulsar().getBrokerService().isAllowAutoTopicCreation(topicName)) {
                    throw new RestException(Status.NOT_FOUND,
                            new PulsarClientException.NotFoundException("Topic not exist"));
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to validate topic existed {}://{}/{}/{}",
                    domain(), tenant, namespace, topicName, e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Check topic partition meta failed.");
        }
    }

    @Deprecated
    protected void validateTopicName(String property, String cluster, String namespace, String encodedTopic) {
        String topic = Codec.decode(encodedTopic);
        try {
            this.namespaceName = NamespaceName.get(property, cluster, namespace);
            this.topicName = TopicName.get(domain(), namespaceName, topic);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to validate topic name {}://{}/{}/{}/{}", clientAppId(), domain(), property, cluster,
                    namespace, topic, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Topic name is not valid");
        }
    }

    protected Policies getNamespacePolicies(NamespaceName namespaceName) {
        try {
            final String namespace = namespaceName.toString();
            final String policyPath = AdminResource.path(POLICIES, namespace);
            Policies policies = namespaceResources().get(policyPath)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            // fetch bundles from LocalZK-policies
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            BundlesData bundleData = NamespaceBundleFactory.getBundlesData(bundles);
            policies.bundles = bundleData != null ? bundleData : policies.bundles;

            return policies;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }

    }

    protected CompletableFuture<Policies> getNamespacePoliciesAsync(NamespaceName namespaceName) {
        final String namespace = namespaceName.toString();
        final String policyPath = AdminResource.path(POLICIES, namespace);

        return namespaceResources().getAsync(policyPath).thenCompose(policies -> {
            if (policies.isPresent()) {
                return pulsar()
                        .getNamespaceService()
                        .getNamespaceBundleFactory()
                        .getBundlesAsync(namespaceName)
                        .thenCompose(bundles -> {
                    BundlesData bundleData = null;
                    try {
                        bundleData = NamespaceBundleFactory.getBundlesData(bundles);
                    } catch (Exception e) {
                        log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
                        return FutureUtil.failedFuture(new RestException(e));
                    }
                    policies.get().bundles = bundleData != null ? bundleData : policies.get().bundles;
                    return CompletableFuture.completedFuture(policies.get());
                });
            } else {
                return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            }
        });
    }

    protected void mergeNamespaceWithDefaults(Policies policies, String namespace, String namespacePath) {
        final ServiceConfiguration config = pulsar().getConfiguration();

        if (policies.max_consumers_per_subscription < 1) {
            policies.max_consumers_per_subscription = config.getMaxConsumersPerSubscription();
        }

        final String cluster = config.getClusterName();

    }

    protected BacklogQuota namespaceBacklogQuota(String namespace, String namespacePath) {
        return pulsar().getBrokerService().getBacklogQuotaManager().getBacklogQuota(namespace, namespacePath);
    }

    protected Optional<TopicPolicies> getTopicPolicies(TopicName topicName) {
        try {
            checkTopicLevelPolicyEnable();
            return Optional.ofNullable(pulsar().getTopicPoliciesService().getTopicPolicies(topicName));
        } catch (RestException re) {
            throw re;
        } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e){
            log.error("Topic {} policies have not been initialized yet.", topicName);
            throw new RestException(e);
        } catch (Exception e) {
            log.error("[{}] Failed to get topic policies {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    protected boolean checkBacklogQuota(BacklogQuota quota, RetentionPolicies retention) {
        if (retention == null || retention.getRetentionSizeInMB() == 0
                || retention.getRetentionSizeInMB() == -1) {
            return true;
        }
        if (quota == null) {
            quota = pulsar().getBrokerService().getBacklogQuotaManager().getDefaultQuota();
        }
        if (quota.getLimit() >= (retention.getRetentionSizeInMB() * 1024 * 1024)) {
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

    protected DispatchRate dispatchRate() {
        return new DispatchRate(
                pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInMsg(),
                pulsar().getConfiguration().getDispatchThrottlingRatePerTopicInByte(),
                1
        );
    }

    protected DispatchRate subscriptionDispatchRate() {
        return new DispatchRate(
                pulsar().getConfiguration().getDispatchThrottlingRatePerSubscriptionInMsg(),
                pulsar().getConfiguration().getDispatchThrottlingRatePerSubscriptionInByte(),
                1
        );
    }

    protected DispatchRate replicatorDispatchRate() {
        return new DispatchRate(
                pulsar().getConfiguration().getDispatchThrottlingRatePerReplicatorInMsg(),
                pulsar().getConfiguration().getDispatchThrottlingRatePerReplicatorInByte(),
                1
        );
    }

    protected SubscribeRate subscribeRate() {
        return new SubscribeRate(
                pulsar().getConfiguration().getSubscribeThrottlingRatePerConsumer(),
                pulsar().getConfiguration().getSubscribeRatePeriodPerConsumerInSecond()
        );
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

    protected void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    protected CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(
            TopicName topicName, boolean authoritative, boolean checkAllowAutoCreation) {
        try {
            validateClusterOwnership(topicName.getCluster());
            // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
            // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
            // producer/consumer
            validateGlobalNamespaceOwnership(topicName.getNamespaceObject());
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }

        try {
            checkConnect(topicName);
        } catch (WebApplicationException e) {
            try {
                validateAdminAccessForTenant(topicName.getTenant());
            } catch (Exception ex) {
                return FutureUtil.failedFuture(ex);
            }
        } catch (Exception e) {
            // unknown error marked as internal server error
            log.warn("Unexpected error while authorizing lookup. topic={}, role={}. Error: {}", topicName,
                    clientAppId(), e.getMessage(), e);
            return FutureUtil.failedFuture(e);
        }

        if (checkAllowAutoCreation) {
            return pulsar().getBrokerService().fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName);
        } else {
            return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName);
        }
    }

    protected PartitionedTopicMetadata getPartitionedTopicMetadata(TopicName topicName,
            boolean authoritative, boolean checkAllowAutoCreation) {
        validateClusterOwnership(topicName.getCluster());
        // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
        // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
        // producer/consumer
        validateGlobalNamespaceOwnership(topicName.getNamespaceObject());

        try {
            checkConnect(topicName);
        } catch (WebApplicationException e) {
            validateAdminAccessForTenant(topicName.getTenant());
        } catch (Exception e) {
            // unknown error marked as internal server error
            log.warn("Unexpected error while authorizing lookup. topic={}, role={}. Error: {}", topicName,
                    clientAppId(), e.getMessage(), e);
            throw new RestException(e);
        }

        PartitionedTopicMetadata partitionMetadata;
        if (checkAllowAutoCreation) {
            partitionMetadata = fetchPartitionedTopicMetadataCheckAllowAutoCreation(pulsar(), topicName);
        } else {
            partitionMetadata = fetchPartitionedTopicMetadata(pulsar(), topicName);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId(), topicName,
                    partitionMetadata.partitions);
        }
        return partitionMetadata;
    }

    protected static PartitionedTopicMetadata fetchPartitionedTopicMetadata(PulsarService pulsar, TopicName topicName) {
        try {
            return pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            throw new RestException(e);
        }
    }

    protected static PartitionedTopicMetadata fetchPartitionedTopicMetadataCheckAllowAutoCreation(
            PulsarService pulsar, TopicName topicName) {
        try {
            return pulsar.getBrokerService().fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName)
                    .get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e.getCause();
            }
            throw new RestException(e);
        }
    }

   protected void validateClusterExists(String cluster) {
        try {
            if (!clusterResources().get(path("clusters", cluster)).isPresent()) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected Policies getNamespacePolicies(String property, String cluster, String namespace) {
        try {
            Policies policies = namespaceResources().get(AdminResource.path(POLICIES, property, cluster, namespace))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            // fetch bundles from LocalZK-policies
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(NamespaceName.get(property, cluster, namespace));
            BundlesData bundleData = NamespaceBundleFactory.getBundlesData(bundles);
            policies.bundles = bundleData != null ? bundleData : policies.bundles;
            return policies;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}/{}/{}", clientAppId(), property, cluster, namespace, e);
            throw new RestException(e);
        }
    }

    protected boolean isNamespaceReplicated(NamespaceName namespaceName) {
        return getNamespaceReplicatedClusters(namespaceName).size() > 1;
    }

    protected Set<String> getNamespaceReplicatedClusters(NamespaceName namespaceName) {
        try {
            final Policies policies = namespaceResources().get(ZkAdminPaths.namespacePoliciesPath(namespaceName))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            return policies.replication_clusters;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected List<String> getPartitionedTopicList(TopicDomain topicDomain) {
        List<String> partitionedTopics = Lists.newArrayList();

        try {
            String partitionedTopicPath = path(PARTITIONED_TOPIC_PATH_ZNODE,
                    namespaceName.toString(), topicDomain.value());
            List<String> topics = namespaceResources().getChildren(partitionedTopicPath);
            partitionedTopics = topics.stream()
                    .map(s -> String.format("%s://%s/%s", topicDomain.value(), namespaceName.toString(), decode(s)))
                    .collect(Collectors.toList());
        } catch (NotFoundException e) {
            // NoNode means there are no partitioned topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }

        partitionedTopics.sort(null);
        return partitionedTopics;
    }

    protected List<String> getTopicPartitionList(TopicDomain topicDomain) {
        List<String> topicPartitions = Lists.newArrayList();

        try {
            String topicPartitionPath = joinPath(MANAGED_LEDGER_PATH_ZNODE,
                    namespaceName.toString(), topicDomain.value());
            List<String> topics = localZk().getChildren(topicPartitionPath, false);
            topicPartitions = topics.stream()
                    .map(s -> String.format("%s://%s/%s", topicDomain.value(), namespaceName.toString(), decode(s)))
                    .collect(Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get topic partition list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }

        topicPartitions.sort(null);
        return topicPartitions;
    }

    protected void internalCreatePartitionedTopic(AsyncResponse asyncResponse, int numPartitions) {
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

            if (maxTopicsPerNamespace > 0) {
                List<String> partitionedTopics = getTopicPartitionList(TopicDomain.persistent);
                if (partitionedTopics.size() + numPartitions > maxTopicsPerNamespace) {
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
            validateAdminAccessForTenant(topicName.getTenant());
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
        checkTopicExistsAsync(topicName).thenAccept(exists -> {
            if (exists) {
                log.warn("[{}] Failed to create already existing topic {}", clientAppId(), topicName);
                asyncResponse.resume(new RestException(Status.CONFLICT, "This topic already exists"));
            } else {

                try {
                    String path = ZkAdminPaths.partitionedTopicPath(topicName);
                    namespaceResources().getPartitionedTopicResources()
                            .createAsync(path, new PartitionedTopicMetadata(numPartitions)).thenAccept(r -> {
                                log.info("[{}] Successfully created partitioned topic {}", clientAppId(), topicName);
                                tryCreatePartitionsAsync(numPartitions).thenAccept(v -> {
                                    log.info("[{}] Successfully created partitions for topic {}", clientAppId(),
                                            topicName);
                                    asyncResponse.resume(Response.noContent().build());
                                }).exceptionally(e -> {
                                    log.error("[{}] Failed to create partitions for topic {}", clientAppId(),
                                            topicName);
                                    // The partitioned topic is created but there are some partitions create failed
                                    asyncResponse.resume(new RestException(e));
                                    return null;
                                });
                            }).exceptionally(ex -> {
                                if (ex.getCause() instanceof AlreadyExistsException) {
                                    log.warn("[{}] Failed to create already existing partitioned topic {}",
                                            clientAppId(), topicName);
                                    asyncResponse.resume(
                                            new RestException(Status.CONFLICT, "Partitioned topic already exists"));
                                } else if (ex.getCause() instanceof BadVersionException) {
                                    log.warn("[{}] Failed to create partitioned topic {}: concurrent modification",
                                            clientAppId(), topicName);
                                    asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
                                } else {
                                    log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName,
                                            ex.getCause());
                                    asyncResponse.resume(new RestException(ex.getCause()));
                                }
                                return null;
                            });
                } catch (Exception e) {
                    log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                }
            }
        }).exceptionally(ex -> {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, ex);
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
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

    protected void resumeAsyncResponseExceptionally(AsyncResponse asyncResponse, Throwable throwable) {
        if (throwable instanceof WebApplicationException) {
            asyncResponse.resume((WebApplicationException) throwable);
        } else {
            asyncResponse.resume(new RestException(throwable));
        }
    }

    @CanIgnoreReturnValue
    public static <T> T checkNotNull(T reference) {
        return com.google.common.base.Preconditions.checkNotNull(reference);
    }

    protected void checkNotNull(Object o, String errorMessage) {
        if (o == null) {
            throw new RestException(Status.BAD_REQUEST, errorMessage);
        }
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
                String.format("Bookkeeper Ensemble (%s) >= WriteQuorum (%s) >= AckQuoru (%s)",
                        persistence.getBookkeeperEnsemble(), persistence.getBookkeeperWriteQuorum(),
                        persistence.getBookkeeperAckQuorum()));

    }
}
