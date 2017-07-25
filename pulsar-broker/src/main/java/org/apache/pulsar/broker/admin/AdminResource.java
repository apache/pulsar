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

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public abstract class AdminResource extends PulsarWebResource {
    private static final Logger log = LoggerFactory.getLogger(AdminResource.class);
    private static final String POLICIES_READONLY_FLAG_PATH = "/admin/flags/policies-readonly";
    public static final String LOAD_SHEDDING_UNLOAD_DISABLED_FLAG_PATH = "/admin/flags/load-shedding-unload-disabled";
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

    /**
     * Get the domain of the destination (whether it's queue or topic)
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
    protected void validateAdminAccessOnProperty(String property) {
        super.validateAdminAccessOnProperty(property);
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
        // First get the list of cluster nodes
        log.info("Children of {} : {}", path("policies", property),
                globalZk().getChildren(path("policies", property), null));

        for (String cluster : globalZk().getChildren(path("policies", property), false)) {
            // Then get the list of namespaces
            try {
                for (String namespace : globalZk().getChildren(path("policies", property, cluster), false)) {
                    namespaces.add(String.format("%s/%s/%s", property, cluster, namespace));
                }
            } catch (KeeperException.NoNodeException e) {
                // A cluster was deleted between the 2 getChildren() calls, ignoring
            }
        }

        namespaces.sort(null);
        return namespaces;
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
        if (!pulsar().getWebServiceAddress().equals(brokerUrl)) {
            String[] parts = broker.split(":");
            checkArgument(parts.length == 2);
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(host).port(port).build();
            log.debug("[{}] Redirecting the rest call to {}: broker={}", clientAppId(), redirect, broker);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    protected Policies getNamespacePolicies(String property, String cluster, String namespace) {
        try {
            Policies policies = policiesCache().get(AdminResource.path("policies", property, cluster, namespace))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Namespace does not exist"));
            // fetch bundles from LocalZK-policies
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(new NamespaceName(property, cluster, namespace));
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

    public static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    ZooKeeperDataCache<PropertyAdmin> propertiesCache() {
        return pulsar().getConfigurationCache().propertiesCache();
    }

    ZooKeeperDataCache<Policies> policiesCache() {
        return pulsar().getConfigurationCache().policiesCache();
    }

    ZooKeeperDataCache<ClusterData> clustersCache() {
        return pulsar().getConfigurationCache().clustersCache();
    }

    ZooKeeperChildrenCache managedLedgerListCache() {
        return pulsar().getLocalZkCacheService().managedLedgerListCache();
    }

    Set<String> clusters() {
        try {
            return pulsar().getConfigurationCache().clustersListCache().get();
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    ZooKeeperChildrenCache clustersListCache() {
        return pulsar().getConfigurationCache().clustersListCache();
    }

    protected void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    protected ZooKeeperDataCache<NamespaceIsolationPolicies> namespaceIsolationPoliciesCache() {
        return pulsar().getConfigurationCache().namespaceIsolationPoliciesCache();
    }

    protected PartitionedTopicMetadata getPartitionedTopicMetadata(String property, String cluster, String namespace,
            String destination, boolean authoritative) {
        DestinationName dn = DestinationName.get(domain(), property, cluster, namespace, destination);
        validateClusterOwnership(dn.getCluster());

        try {
            checkConnect(dn);
        } catch (WebApplicationException e) {
            validateAdminAccessOnProperty(dn.getProperty());
        } catch (Exception e) {
            // unknown error marked as internal server error
            log.warn("Unexpected error while authorizing lookup. destination={}, role={}. Error: {}", destination,
                    clientAppId(), e.getMessage(), e);
            throw new RestException(e);
        }

        String path = path(PARTITIONED_TOPIC_PATH_ZNODE, property, cluster, namespace, domain(),
                dn.getEncodedLocalName());
        PartitionedTopicMetadata partitionMetadata = fetchPartitionedTopicMetadata(pulsar(), path);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId(), dn,
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

    protected static CompletableFuture<PartitionedTopicMetadata> fetchPartitionedTopicMetadataAsync(PulsarService pulsar,
            String path) {
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
    
}
