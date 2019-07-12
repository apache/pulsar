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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.common.util.Codec.decode;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.Constants;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public abstract class AdminResource extends PulsarWebResource {
    private static final Logger log = LoggerFactory.getLogger(AdminResource.class);
    private static final String POLICIES_READONLY_FLAG_PATH = "/admin/flags/policies-readonly";
    public static final String PARTITIONED_TOPIC_PATH_ZNODE = "partitioned-topics";

    protected ZooKeeper globalZk() {
        return pulsar().getGlobalZkCache().getZooKeeper();
    }

    protected ZooKeeperCache globalZkCache() {
        return pulsar().getGlobalZkCache();
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

    protected void zkCreate(String path, byte[] content) throws Exception {
        globalZk().create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    protected void zkCreateOptimistic(String path, byte[] content) throws Exception {
        ZkUtils.createFullPathOptimistic(globalZk(), path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    protected boolean zkPathExists(String path) throws KeeperException, InterruptedException {
        Stat stat = globalZk().exists(path, false);
        if (null != stat) {
            return true;
        }
        return false;
    }

    /**
     * Get the domain of the topic (whether it's persistent or non-persistent)
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
    protected void validateSuperUserAccess() {
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
     * Checks whether the broker is allowed to do read-write operations based on the existence of a node in global
     * zookeeper.
     *
     * @throws WebApplicationException
     *             if broker has a read only access if broker is not connected to the global zookeeper
     */
    public void validatePoliciesReadOnlyAccess() {
        boolean arePoliciesReadOnly = true;

        try {
            arePoliciesReadOnly = globalZkCache().exists(POLICIES_READONLY_FLAG_PATH);
        } catch (Exception e) {
            log.warn("Unable to fetch contents of [{}] from global zookeeper", POLICIES_READONLY_FLAG_PATH, e);
            throw new RestException(e);
        }

        if (arePoliciesReadOnly) {
            log.debug("Policies are read-only. Broker cannot do read-write operations");
            throw new RestException(Status.FORBIDDEN, "Broker is forbidden to do read-write operations");
        } else {
            // Make sure the broker is connected to the global zookeeper before writing. If not, throw an exception.
            if (globalZkCache().getZooKeeper().getState() != States.CONNECTED) {
                log.debug("Broker is not connected to the global zookeeper");
                throw new RestException(Status.PRECONDITION_FAILED,
                        "Broker needs to be connected to global zookeeper before making a read-write operation");
            } else {
                // Do nothing, just log the message.
                log.debug("Broker is allowed to make read-write operations");
            }
        }
    }

    /**
     * Get the list of namespaces (on every cluster) for a given property
     *
     * @param property
     *            the property name
     * @return the list of namespaces
     */
    protected List<String> getListOfNamespaces(String property) throws Exception {
        List<String> namespaces = Lists.newArrayList();

        // this will return a cluster in v1 and a namespace in v2
        for (String clusterOrNamespace : globalZk().getChildren(path(POLICIES, property), false)) {
            // Then get the list of namespaces
            try {
                final List<String> children = globalZk().getChildren(path(POLICIES, property, clusterOrNamespace), false);
                if (children == null || children.isEmpty()) {
                    String namespace = NamespaceName.get(property, clusterOrNamespace).toString();
                    // if the length is 0 then this is probably a leftover cluster from namespace created
                    // with the v1 admin format (prop/cluster/ns) and then deleted, so no need to add it to the list
                    if (globalZk().getData(path(POLICIES, namespace), false, null).length != 0) {
                        namespaces.add(namespace);
                    }
                } else {
                    children.forEach(ns -> {
                        namespaces.add(NamespaceName.get(property, clusterOrNamespace, ns).toString());
                    });
                }
            } catch (KeeperException.NoNodeException e) {
                // A cluster was deleted between the 2 getChildren() calls, ignoring
            }
        }

        namespaces.sort(null);
        return namespaces;
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

        this.topicName = TopicName.get(domain(), namespaceName, topic);
    }

    protected void validatePartitionedTopicName(String tenant, String namespace, String encodedTopic) {
        // first, it has to be a validate topic name
        validateTopicName(tenant, namespace, encodedTopic);
        // second, "-partition-" is not allowed
        if (encodedTopic.contains(TopicName.PARTITIONED_TOPIC_SUFFIX)) {
            throw new RestException(Status.PRECONDITION_FAILED, "Partitioned Topic Name should not contain '-partition-'");
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

    /**
     * Redirect the call to the specified broker
     *
     * @param broker
     *            Broker name
     * @throws MalformedURLException
     *             In case the redirect happens
     */
    protected void validateBrokerName(String broker) throws MalformedURLException {
        String brokerUrl = String.format("http://%s", broker);
        String brokerUrlTls = String.format("https://%s", broker);
        if (!brokerUrl.equals(pulsar().getSafeWebServiceAddress())
                && !brokerUrlTls.equals(pulsar().getWebServiceAddressTls())) {
            String[] parts = broker.split(":");
            checkArgument(parts.length == 2, "Invalid broker url %s", broker);
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(host).port(port).build();
            log.debug("[{}] Redirecting the rest call to {}: broker={}", clientAppId(), redirect, broker);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    protected Policies getNamespacePolicies(NamespaceName namespaceName) {
        try {
            final String namespace = namespaceName.toString();
            final String policyPath = AdminResource.path(POLICIES, namespace);
            Policies policies = policiesCache().get(policyPath)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            // fetch bundles from LocalZK-policies
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            BundlesData bundleData = NamespaceBundleFactory.getBundlesData(bundles);
            policies.bundles = bundleData != null ? bundleData : policies.bundles;

            // hydrate the namespace polices
            mergeNamespaceWithDefaults(policies, namespace, policyPath);

            return policies;
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace policies {}", clientAppId(), namespaceName, e);
            throw new RestException(e);
        }
    }

    protected void mergeNamespaceWithDefaults(Policies policies, String namespace, String namespacePath) {
        if (policies.backlog_quota_map.isEmpty()) {
            Policies.setStorageQuota(policies, namespaceBacklogQuota(namespace, namespacePath));
        }

        final ServiceConfiguration config = pulsar().getConfiguration();
        if (policies.max_producers_per_topic < 1) {
            policies.max_producers_per_topic = config.getMaxProducersPerTopic();
        }

        if (policies.max_consumers_per_topic < 1) {
            policies.max_consumers_per_topic = config.getMaxConsumersPerTopic();
        }

        if (policies.max_consumers_per_subscription < 1) {
            policies.max_consumers_per_subscription = config.getMaxConsumersPerSubscription();
        }

        final String cluster = config.getClusterName();
        // attach default dispatch rate polices
        if (policies.topicDispatchRate.isEmpty()) {
            policies.topicDispatchRate.put(cluster, dispatchRate());
        }

        if (policies.subscriptionDispatchRate.isEmpty()) {
            policies.subscriptionDispatchRate.put(cluster, subscriptionDispatchRate());
        }

        if (policies.clusterSubscribeRate.isEmpty()) {
            policies.clusterSubscribeRate.put(cluster, subscribeRate());
        }
    }

    protected BacklogQuota namespaceBacklogQuota(String namespace, String namespacePath) {
        return pulsar().getBrokerService().getBacklogQuotaManager().getBacklogQuota(namespace, namespacePath);
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

    protected SubscribeRate subscribeRate() {
        return new SubscribeRate(
                pulsar().getConfiguration().getSubscribeThrottlingRatePerConsumer(),
                pulsar().getConfiguration().getSubscribeRatePeriodPerConsumerInSecond()
        );
    }

    public static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    public ZooKeeperDataCache<TenantInfo> tenantsCache() {
        return pulsar().getConfigurationCache().propertiesCache();
    }

    protected ZooKeeperDataCache<Policies> policiesCache() {
        return pulsar().getConfigurationCache().policiesCache();
    }

    protected ZooKeeperDataCache<LocalPolicies> localPoliciesCache() {
        return pulsar().getLocalZkCacheService().policiesCache();
    }

    protected ZooKeeperDataCache<ClusterData> clustersCache() {
        return pulsar().getConfigurationCache().clustersCache();
    }

    protected ZooKeeperChildrenCache managedLedgerListCache() {
        return pulsar().getLocalZkCacheService().managedLedgerListCache();
    }

    protected Set<String> clusters() {
        try {
            Set<String> clusters = pulsar().getConfigurationCache().clustersListCache().get();

            // Remove "global" cluster from returned list
            clusters.remove(Constants.GLOBAL_CLUSTER);
            return clusters;
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected ZooKeeperChildrenCache clustersListCache() {
        return pulsar().getConfigurationCache().clustersListCache();
    }

    protected void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    protected ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache() {
        return pulsar().getConfigurationCache().namespaceIsolationPoliciesCache();
    }

    protected ZooKeeperDataCache<FailureDomain> failureDomainCache() {
        return pulsar().getConfigurationCache().failureDomainCache();
    }

    protected ZooKeeperChildrenCache failureDomainListCache() {
        return pulsar().getConfigurationCache().failureDomainListCache();
    }

    protected PartitionedTopicMetadata getPartitionedTopicMetadata(TopicName topicName,
            boolean authoritative) {
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

        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(), topicName.getEncodedLocalName());
        PartitionedTopicMetadata partitionMetadata = fetchPartitionedTopicMetadata(pulsar(), path);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId(), topicName,
                    partitionMetadata.partitions);
        }
        return partitionMetadata;
    }

    protected static PartitionedTopicMetadata fetchPartitionedTopicMetadata(PulsarService pulsar, String path) {
        try {
            return fetchPartitionedTopicMetadataAsync(pulsar, path).get();
        } catch (Exception e) {
            if (e.getCause() instanceof RestException) {
                throw (RestException) e;
            }
            throw new RestException(e);
        }
    }

    protected static CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(
            PulsarService pulsar, String path) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        try {
            // gets the number of partitions from the zk cache
            pulsar.getGlobalZkCache().getDataAsync(path, new Deserializer<PartitionedTopicMetadata>() {
                @Override
                public PartitionedTopicMetadata deserialize(String key, byte[] content) throws Exception {
                    return jsonMapper().readValue(content, PartitionedTopicMetadata.class);
                }
            }).thenAccept(metadata -> {
                // if the partitioned topic is not found in zk, then the topic is not partitioned
                if (metadata.isPresent()) {
                    metadataFuture.complete(metadata.get());
                } else {
                    metadataFuture.complete(new PartitionedTopicMetadata());
                }
            }).exceptionally(ex -> {
                metadataFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            metadataFuture.completeExceptionally(e);
        }
        return metadataFuture;
    }

    protected void validateClusterExists(String cluster) {
        try {
            if (!clustersCache().get(path("clusters", cluster)).isPresent()) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    protected Policies getNamespacePolicies(String property, String cluster, String namespace) {
        try {
            Policies policies = policiesCache().get(AdminResource.path(POLICIES, property, cluster, namespace))
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
        try {
            final Policies policies = policiesCache().get(ZkAdminPaths.namespacePoliciesPath(namespaceName))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            return policies.replication_clusters.size() > 1;
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
            String partitionedTopicPath = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), topicDomain.value());
            List<String> topics = globalZk().getChildren(partitionedTopicPath, false);
            partitionedTopics = topics.stream()
                    .map(s -> String.format("%s://%s/%s", topicDomain.value(), namespaceName.toString(), decode(s)))
                    .collect(Collectors.toList());
        } catch (KeeperException.NoNodeException e) {
            // NoNode means there are no partitioned topics in this domain for this namespace
        } catch (Exception e) {
            log.error("[{}] Failed to get partitioned topic list for namespace {}", clientAppId(),
                    namespaceName.toString(), e);
            throw new RestException(e);
        }

        partitionedTopics.sort(null);
        return partitionedTopics;
    }
}
