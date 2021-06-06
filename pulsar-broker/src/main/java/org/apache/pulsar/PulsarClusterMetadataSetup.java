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
package org.apache.pulsar;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;
import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterInitializer;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.pulsar.broker.admin.ZkAdminPaths;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreLifecycle;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Setup the metadata for a new Pulsar cluster.
 */
public class PulsarClusterMetadataSetup {

    private static class Arguments {
        @Parameter(names = { "-c", "--cluster" }, description = "Cluster name", required = true)
        private String cluster;

        @Parameter(names = { "-uw",
                "--web-service-url" }, description = "Web-service URL for new cluster", required = true)
        private String clusterWebServiceUrl;

        @Parameter(names = {"-tw",
                "--web-service-url-tls"},
                description = "Web-service URL for new cluster with TLS encryption", required = false)
        private String clusterWebServiceUrlTls;

        @Parameter(names = { "-ub",
                "--broker-service-url" }, description = "Broker-service URL for new cluster", required = false)
        private String clusterBrokerServiceUrl;

        @Parameter(names = {"-tb",
                "--broker-service-url-tls"},
                description = "Broker-service URL for new cluster with TLS encryption", required = false)
        private String clusterBrokerServiceUrlTls;

        @Parameter(names = { "-zk",
                "--zookeeper" }, description = "Local ZooKeeper quorum connection string", required = true)
        private String zookeeper;

        @Parameter(names = {
            "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Parameter(names = {"-gzk",
                "--global-zookeeper"},
                description = "Global ZooKeeper quorum connection string", required = false, hidden = true)
        private String globalZookeeper;

        @Parameter(names = { "-cs",
            "--configuration-store" }, description = "Configuration Store connection string", required = true)
        private String configurationStore;

        @Parameter(names = {
            "--initial-num-stream-storage-containers"
        }, description = "Num storage containers of BookKeeper stream storage")
        private int numStreamStorageContainers = 16;

        @Parameter(names = {
                "--initial-num-transaction-coordinators"
        }, description = "Num transaction coordinators will assigned in cluster")
        private int numTransactionCoordinators = 16;

        @Parameter(names = {
                "--existing-bk-metadata-service-uri"},
                description = "The metadata service URI of the existing BookKeeper cluster that you want to use")
        private String existingBkMetadataServiceUri;

        // Hide and marked as deprecated this flag because we use the new name '--existing-bk-metadata-service-uri' to
        // pass the service url. For compatibility of the command, we should keep both to avoid the exceptions.
        @Deprecated
        @Parameter(names = {
            "--bookkeeper-metadata-service-uri"},
            description = "The metadata service URI of the existing BookKeeper cluster that you want to use",
            hidden = true)
        private String bookieMetadataServiceUri;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    /**
     * a wrapper for creating a persistent node with store.put but ignore exception of node exists.
     */
    private static void createMetadataNode(MetadataStore store, String path, byte[] data)
            throws InterruptedException, ExecutionException {
        try {
            store.put(path, data, Optional.of(-1L)).get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof MetadataStoreException.BadVersionException)) {
                throw e;
            }
            // Ignore
        }
    }

    private static void initialDlogNamespaceMetadata(String configurationStore, String bkMetadataServiceUri)
            throws IOException {
        InternalConfigurationData internalConf = new InternalConfigurationData(
                configurationStore,
                configurationStore,
                null,
                bkMetadataServiceUri,
                null
        );
        WorkerUtils.initializeDlogNamespace(internalConf);
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(arguments);
            jcommander.parse(args);
            if (arguments.help) {
                jcommander.usage();
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            throw e;
        }

        if (arguments.configurationStore == null && arguments.globalZookeeper == null) {
            System.err.println("Configuration store address argument is required (--configuration-store)");
            jcommander.usage();
            System.exit(1);
        }

        if (arguments.configurationStore != null && arguments.globalZookeeper != null) {
            System.err.println("Configuration store argument (--configuration-store) "
                    + "supersedes the deprecated (--global-zookeeper) argument");
            jcommander.usage();
            System.exit(1);
        }

        if (arguments.configurationStore == null) {
            arguments.configurationStore = arguments.globalZookeeper;
        }

        if (arguments.numTransactionCoordinators <= 0) {
            System.err.println("Number of transaction coordinators must greater than 0");
            System.exit(1);
        }

        log.info("Setting up cluster {} with zk={} configuration-store={}", arguments.cluster, arguments.zookeeper,
                arguments.configurationStore);

        MetadataStoreExtended localStore = initMetadataStore(arguments.zookeeper, arguments.zkSessionTimeoutMillis);
        MetadataStoreExtended configStore = initMetadataStore(arguments.configurationStore,
                arguments.zkSessionTimeoutMillis);

        // Format BookKeeper ledger storage metadata
        ServerConfiguration bkConf = new ServerConfiguration();
        if (arguments.existingBkMetadataServiceUri == null && arguments.bookieMetadataServiceUri == null) {
            bkConf.setZkServers(arguments.zookeeper);
            bkConf.setZkTimeout(arguments.zkSessionTimeoutMillis);
            if (!localStore.exists("/ledgers").get() // only format if /ledgers doesn't exist
                && !BookKeeperAdmin.format(bkConf, false /* interactive */, false /* force */)) {
                throw new IOException("Failed to initialize BookKeeper metadata");
            }
        }


        String uriStr = bkConf.getMetadataServiceUri();
        if (arguments.existingBkMetadataServiceUri != null) {
            uriStr = arguments.existingBkMetadataServiceUri;
        } else if (arguments.bookieMetadataServiceUri != null) {
            uriStr = arguments.bookieMetadataServiceUri;
        }
        ServiceURI bkMetadataServiceUri = ServiceURI.create(uriStr);

        // initial distributed log metadata
        initialDlogNamespaceMetadata(arguments.configurationStore, uriStr);

        // Format BookKeeper stream storage metadata
        if (arguments.numStreamStorageContainers > 0) {
            ClusterInitializer initializer = new ZkClusterInitializer(arguments.zookeeper);
            initializer.initializeCluster(bkMetadataServiceUri.getUri(), arguments.numStreamStorageContainers);
        }

        if (!localStore.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH).get()) {
            createMetadataNode(localStore, ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes());
        }

        createMetadataNode(localStore, "/managed-ledgers", new byte[0]);

        createMetadataNode(localStore, "/namespace", new byte[0]);

        createMetadataNode(configStore, POLICIES_ROOT, new byte[0]);

        createMetadataNode(configStore, "/admin/clusters", new byte[0]);

        ClusterData clusterData = ClusterData.builder()
                .serviceUrl(arguments.clusterWebServiceUrl)
                .serviceUrlTls(arguments.clusterWebServiceUrlTls)
                .brokerServiceUrl(arguments.clusterBrokerServiceUrl)
                .brokerServiceUrlTls(arguments.clusterBrokerServiceUrlTls)
                .build();
        byte[] clusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(clusterData);

        createMetadataNode(configStore, "/admin/clusters/" + arguments.cluster, clusterDataJson);

        // Create marker for "global" cluster
        ClusterData globalClusterData = ClusterData.builder().build();
        byte[] globalClusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(globalClusterData);

        createMetadataNode(configStore, "/admin/clusters/global", globalClusterDataJson);

        // Create public tenant, whitelisted to use the this same cluster, along with other clusters
        createTenantIfAbsent(configStore, TopicName.PUBLIC_TENANT, arguments.cluster);

        // Create system tenant
        createTenantIfAbsent(configStore, NamespaceName.SYSTEM_NAMESPACE.getTenant(), arguments.cluster);

        // Create default namespace
        createNamespaceIfAbsent(configStore, NamespaceName.get(TopicName.PUBLIC_TENANT, TopicName.DEFAULT_NAMESPACE),
                arguments.cluster);

        // Create system namespace
        createNamespaceIfAbsent(configStore, NamespaceName.SYSTEM_NAMESPACE, arguments.cluster);

        // Create transaction coordinator assign partitioned topic
        createPartitionedTopic(configStore, TopicName.TRANSACTION_COORDINATOR_ASSIGN,
                arguments.numTransactionCoordinators);

        localStore.close();
        configStore.close();

        log.info("Cluster metadata for '{}' setup correctly", arguments.cluster);
    }

    static void createTenantIfAbsent(MetadataStore configStore, String tenant, String cluster) throws IOException,
            InterruptedException, ExecutionException {

        String tenantPath = POLICIES_ROOT + "/" + tenant;

        Optional<GetResult> getResult = configStore.get(tenantPath).get();
        if (!getResult.isPresent()) {
            TenantInfoImpl publicTenant = new TenantInfoImpl(Collections.emptySet(), Collections.singleton(cluster));

            createMetadataNode(configStore, tenantPath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant));
        } else {
            // Update existing public tenant with new cluster
            byte[] content = getResult.get().getValue();
            TenantInfoImpl publicTenant = ObjectMapperFactory.getThreadLocal().readValue(content, TenantInfoImpl.class);

            // Only update z-node if the list of clusters should be modified
            if (!publicTenant.getAllowedClusters().contains(cluster)) {
                publicTenant.getAllowedClusters().add(cluster);

                configStore.put(tenantPath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant),
                        Optional.of(getResult.get().getStat().getVersion()));
            }
        }
    }

    static void createNamespaceIfAbsent(MetadataStore configStore, NamespaceName namespaceName, String cluster)
            throws InterruptedException, IOException, ExecutionException {
        String namespacePath = POLICIES_ROOT + "/" + namespaceName.toString();
        Policies policies;
        Optional<GetResult> getResult = configStore.get(namespacePath).get();
        if (!getResult.isPresent()) {
            policies = new Policies();
            policies.bundles = getBundles(16);
            policies.replication_clusters = Collections.singleton(cluster);

            createMetadataNode(configStore, namespacePath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies));
        } else {
            byte[] content = getResult.get().getValue();
            policies = ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);

            // Only update z-node if the list of clusters should be modified
            if (!policies.replication_clusters.contains(cluster)) {
                policies.replication_clusters.add(cluster);

                configStore.put(namespacePath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                        Optional.of(getResult.get().getStat().getVersion()));
            }
        }
    }

    static void createPartitionedTopic(MetadataStore configStore, TopicName topicName, int numPartitions)
            throws InterruptedException, IOException, ExecutionException {
        String partitionedTopicPath = ZkAdminPaths.partitionedTopicPath(topicName);
        Optional<GetResult> getResult = configStore.get(partitionedTopicPath).get();
        PartitionedTopicMetadata metadata = new PartitionedTopicMetadata(numPartitions);
        if (!getResult.isPresent()) {
            createMetadataNode(configStore, partitionedTopicPath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(metadata));
        } else {
            byte[] content = getResult.get().getValue();
            PartitionedTopicMetadata existsMeta =
                    ObjectMapperFactory.getThreadLocal().readValue(content, PartitionedTopicMetadata.class);

            // Only update z-node if the partitions should be modified
            if (existsMeta.partitions < numPartitions) {
                configStore.put(
                        partitionedTopicPath,
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(metadata),
                        Optional.of(getResult.get().getStat().getVersion()));
            }
        }
    }

    public static MetadataStoreExtended initMetadataStore(String connection, int sessionTimeout) throws Exception {
        MetadataStoreExtended store = MetadataStoreExtended.create(connection, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeout)
                .build());
        if (store instanceof MetadataStoreLifecycle) {
            ((MetadataStoreLifecycle) store).initializeCluster().get();
        }
        return store;
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataSetup.class);
}
