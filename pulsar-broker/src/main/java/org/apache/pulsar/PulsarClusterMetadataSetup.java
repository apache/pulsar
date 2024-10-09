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
package org.apache.pulsar;

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterInitializer;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreLifecycle;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * Setup the metadata for a new Pulsar cluster.
 */
public class PulsarClusterMetadataSetup {

    private static final int DEFAULT_BUNDLE_NUMBER = 16;

    @Command(name = "initialize-cluster-metadata", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class Arguments {
        @Option(names = {"-c", "--cluster"}, description = "Cluster name", required = true)
        private String cluster;

        @Option(names = {"-bn",
                "--default-namespace-bundle-number"},
                description = "The bundle numbers for the default namespaces(public/default), default is 16",
                required = false)
        private int numberOfDefaultNamespaceBundles;

        @Option(names = {"-uw",
                "--web-service-url"}, description = "Web-service URL for new cluster", required = true)
        private String clusterWebServiceUrl;

        @Option(names = {"-tw",
                "--web-service-url-tls"},
                description = "Web-service URL for new cluster with TLS encryption", required = false)
        private String clusterWebServiceUrlTls;

        @Option(names = {"-ub",
                "--broker-service-url"}, description = "Broker-service URL for new cluster", required = false)
        private String clusterBrokerServiceUrl;

        @Option(names = {"-tb",
                "--broker-service-url-tls"},
                description = "Broker-service URL for new cluster with TLS encryption", required = false)
        private String clusterBrokerServiceUrlTls;

        @Option(names = {"-te",
                "--tls-enable"},
                description = "Enable TLS connection for new cluster")
        private Boolean clusterBrokerClientTlsEnabled;

        @Option(names = "--auth-plugin",
                description = "The authentication plugin for new cluster")
        protected String clusterAuthenticationPlugin;

        @Option(names = "--auth-parameters",
                description = "The authentication parameters for new cluster")
        protected String clusterAuthenticationParameters;

        @Option(names = {"-zk",
                "--zookeeper"}, description = "Local ZooKeeper quorum connection string",
                required = false,
                hidden = true
        )
        private String zookeeper;

        @Option(names = {"-md",
                "--metadata-store"}, description = "Metadata Store service url. eg: zk:my-zk:2181", required = false)
        private String metadataStoreUrl;

        @Option(names = {
                "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Option(names = {"-gzk",
                "--global-zookeeper"},
                description = "Global ZooKeeper quorum connection string", required = false, hidden = true)
        private String globalZookeeper;

        @Option(names = {"-cs",
                "--configuration-store"}, description = "Configuration Store connection string", hidden = true)
        private String configurationStore;

        @Option(names = {"-cms",
                "--configuration-metadata-store"}, description = "Configuration Metadata Store connection string",
                hidden = false)
        private String configurationMetadataStore;

        @Option(names = {"-mscp",
                "--metadata-store-config-path"}, description = "Metadata Store config path", hidden = false)
        private String metadataStoreConfigPath;

        @Option(names = {"-cmscp",
                "--configuration-metadata-store-config-path"}, description = "Configuration Metadata Store config path",
                hidden = false)
        private String configurationStoreConfigPath;

        @Option(names = {
                "--initial-num-stream-storage-containers"
        }, description = "Num storage containers of BookKeeper stream storage")
        private int numStreamStorageContainers = 16;

        @Option(names = {
                "--initial-num-transaction-coordinators"
        }, description = "Num transaction coordinators will assigned in cluster")
        private int numTransactionCoordinators = 16;

        @Option(names = {
                "--existing-bk-metadata-service-uri"},
                description = "The metadata service URI of the existing BookKeeper cluster that you want to use")
        private String existingBkMetadataServiceUri;

        // Hide and marked as deprecated this flag because we use the new name '--existing-bk-metadata-service-uri' to
        // pass the service url. For compatibility of the command, we should keep both to avoid the exceptions.
        @Deprecated
        @Option(names = {
                "--bookkeeper-metadata-service-uri"},
                description = "The metadata service URI of the existing BookKeeper cluster that you want to use",
                hidden = true)
        private String bookieMetadataServiceUri;

        @Option(names = {"-pp",
                "--proxy-protocol"},
                description = "Proxy protocol to select type of routing at proxy. Possible Values: [SNI]",
                required = false)
        private ProxyProtocol clusterProxyProtocol;

        @Option(names = {"-pu",
                "--proxy-url"}, description = "Proxy-server URL to which to connect.", required = false)
        private String clusterProxyUrl;

        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
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
        System.setProperty("bookkeeper.metadata.bookie.drivers", PulsarMetadataBookieDriver.class.getName());
        System.setProperty("bookkeeper.metadata.client.drivers", PulsarMetadataClientDriver.class.getName());

        Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        try {
            commander.parseArgs(args);
            if (arguments.help) {
                commander.usage(commander.getOut());
                return;
            }
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("initialize-cluster-metadata", commander);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            commander.getErr().println(e);
            throw e;
        }

        if (arguments.metadataStoreUrl == null && arguments.zookeeper == null) {
            System.err.println("Metadata store address argument is required (--metadata-store)");
            commander.usage(commander.getOut());
            System.exit(1);
        }

        if (arguments.configurationMetadataStore == null && arguments.configurationStore == null
                && arguments.globalZookeeper == null) {
            System.err.println(
                    "Configuration metadata store address argument is required (--configuration-metadata-store)");
            commander.usage(commander.getOut());
            System.exit(1);
        }

        if (arguments.configurationMetadataStore != null && (arguments.configurationStore != null
                || arguments.globalZookeeper != null)) {
            System.err.println("Configuration metadata store argument (--configuration-metadata-store) "
                    + "supersedes the deprecated (--global-zookeeper and --configuration-store) argument");
            commander.usage(commander.getOut());
            System.exit(1);
        }

        if (arguments.configurationMetadataStore == null) {
            arguments.configurationMetadataStore = arguments.configurationStore == null ? arguments.globalZookeeper :
                    arguments.configurationStore;
        }

        if (arguments.metadataStoreUrl == null) {
            arguments.metadataStoreUrl = ZKMetadataStore.ZK_SCHEME_IDENTIFIER + arguments.zookeeper;
        }

        if (arguments.numTransactionCoordinators <= 0) {
            System.err.println("Number of transaction coordinators must greater than 0");
            System.exit(1);
        }
        int bundleNumberForDefaultNamespace =
                arguments.numberOfDefaultNamespaceBundles > 0 ? arguments.numberOfDefaultNamespaceBundles
                        : DEFAULT_BUNDLE_NUMBER;
        try {
            initializeCluster(arguments, bundleNumberForDefaultNamespace);
        } catch (Exception e) {
            System.err.println("Unexpected error occured.");
            e.printStackTrace(System.err);
            System.err.println("Terminating JVM...");
            ShutdownUtil.triggerImmediateForcefulShutdown();
        }
    }

    private static void initializeCluster(Arguments arguments, int bundleNumberForDefaultNamespace) throws Exception {
        log.info("Setting up cluster {} with metadata-store={} configuration-metadata-store={}", arguments.cluster,
                arguments.metadataStoreUrl, arguments.configurationMetadataStore);

        MetadataStoreExtended localStore = initLocalMetadataStore(arguments.metadataStoreUrl,
                arguments.metadataStoreConfigPath,
                arguments.zkSessionTimeoutMillis);
        MetadataStoreExtended configStore = initConfigMetadataStore(arguments.configurationMetadataStore,
                arguments.configurationStoreConfigPath,
                arguments.zkSessionTimeoutMillis);

        final String metadataStoreUrlNoIdentifer = MetadataStoreFactoryImpl
                .removeIdentifierFromMetadataURL(arguments.metadataStoreUrl);
        // Format BookKeeper ledger storage metadata
        if (arguments.existingBkMetadataServiceUri == null && arguments.bookieMetadataServiceUri == null) {
            ServerConfiguration bkConf = new ServerConfiguration();
            bkConf.setDelimiterParsingDisabled(true);
            bkConf.setMetadataServiceUri("metadata-store:" + arguments.metadataStoreUrl);
            bkConf.setZkTimeout(arguments.zkSessionTimeoutMillis);
            // only format if /ledgers doesn't exist
            if (!localStore.exists(BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH).get()
                && !BookKeeperAdmin.format(bkConf, false /* interactive */, false /* force */)) {
                throw new IOException("Failed to initialize BookKeeper metadata");
            }
        }

        if (localStore instanceof ZKMetadataStore && configStore instanceof ZKMetadataStore) {
            String uriStr;
            if (arguments.existingBkMetadataServiceUri != null) {
                uriStr = arguments.existingBkMetadataServiceUri;
            } else if (arguments.bookieMetadataServiceUri != null) {
                uriStr = arguments.bookieMetadataServiceUri;
            } else {
                uriStr = "zk+null://" + metadataStoreUrlNoIdentifer + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
            }

            // initial distributed log metadata
            initialDlogNamespaceMetadata(arguments.configurationMetadataStore, uriStr);

            ServiceURI bkMetadataServiceUri = ServiceURI.create(uriStr);
            // Format BookKeeper stream storage metadata
            if (arguments.numStreamStorageContainers > 0) {
                ClusterInitializer initializer = new ZkClusterInitializer(metadataStoreUrlNoIdentifer);
                initializer.initializeCluster(bkMetadataServiceUri.getUri(), arguments.numStreamStorageContainers);
            }
        }

        if (!localStore.exists(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH).get()) {
            createMetadataNode(localStore, BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes());
        }

        PulsarResources resources = new PulsarResources(localStore, configStore);

        ClusterData.Builder clusterDataBuilder = ClusterData.builder();
        if (arguments.clusterWebServiceUrl != null) {
            clusterDataBuilder.serviceUrl(arguments.clusterWebServiceUrl);
        }
        if (arguments.clusterWebServiceUrlTls != null) {
            clusterDataBuilder.serviceUrlTls(arguments.clusterWebServiceUrlTls);
        }
        if (arguments.clusterBrokerServiceUrl != null) {
            clusterDataBuilder.brokerServiceUrl(arguments.clusterBrokerServiceUrl);
        }
        if (arguments.clusterBrokerServiceUrlTls != null) {
            clusterDataBuilder.brokerServiceUrlTls(arguments.clusterBrokerServiceUrlTls);
        }
        if (arguments.clusterBrokerClientTlsEnabled != null) {
            clusterDataBuilder.brokerClientTlsEnabled(arguments.clusterBrokerClientTlsEnabled);
        }
        if (arguments.clusterAuthenticationPlugin != null) {
            clusterDataBuilder.authenticationPlugin(arguments.clusterAuthenticationPlugin);
        }
        if (arguments.clusterAuthenticationParameters != null) {
            clusterDataBuilder.authenticationParameters(arguments.clusterAuthenticationParameters);
        }
        if (arguments.clusterProxyUrl != null) {
            clusterDataBuilder.proxyServiceUrl(arguments.clusterProxyUrl);
        }
        if (arguments.clusterProxyProtocol != null) {
            clusterDataBuilder.proxyProtocol(arguments.clusterProxyProtocol);
        }

        ClusterData clusterData = clusterDataBuilder.build();
        if (!resources.getClusterResources().clusterExists(arguments.cluster)) {
            resources.getClusterResources().createCluster(arguments.cluster, clusterData);
        }

        // Create marker for "global" cluster
        ClusterData globalClusterData = ClusterData.builder().build();
        if (!resources.getClusterResources().clusterExists("global")) {
            resources.getClusterResources().createCluster("global", globalClusterData);
        }

        // Create public tenant, allowed to use this same cluster, along with other clusters
        createTenantIfAbsent(resources, TopicName.PUBLIC_TENANT, arguments.cluster);

        // Create system tenant
        createTenantIfAbsent(resources, NamespaceName.SYSTEM_NAMESPACE.getTenant(), arguments.cluster);

        // Create default namespace
        createNamespaceIfAbsent(resources, NamespaceName.get(TopicName.PUBLIC_TENANT, TopicName.DEFAULT_NAMESPACE),
                arguments.cluster, bundleNumberForDefaultNamespace);

        // Create system namespace
        createNamespaceIfAbsent(resources, NamespaceName.SYSTEM_NAMESPACE, arguments.cluster);

        // Create transaction coordinator assign partitioned topic
        createPartitionedTopic(configStore, SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                arguments.numTransactionCoordinators);

        localStore.close();
        configStore.close();

        log.info("Cluster metadata for '{}' setup correctly", arguments.cluster);
    }

    public static void createTenantIfAbsent(PulsarResources resources, String tenant, String cluster)
            throws IOException, InterruptedException, ExecutionException {

        TenantResources tenantResources = resources.getTenantResources();

        if (!tenantResources.tenantExists(tenant)) {
            TenantInfoImpl publicTenant = new TenantInfoImpl(Collections.emptySet(), Collections.singleton(cluster));
            tenantResources.createTenant(tenant, publicTenant);
        } else {
            // Update existing public tenant with new cluster
            tenantResources.updateTenantAsync(tenant, ti -> {
                ti.getAllowedClusters().add(cluster);
                return ti;
            }).get();
        }
    }

    public static void createNamespaceIfAbsent(PulsarResources resources, NamespaceName namespaceName,
                                               String cluster, int bundleNumber) throws IOException {
        NamespaceResources namespaceResources = resources.getNamespaceResources();

        if (!namespaceResources.namespaceExists(namespaceName)) {
            Policies policies = new Policies();
            policies.bundles = getBundles(bundleNumber);
            policies.replication_clusters = Collections.singleton(cluster);

            namespaceResources.createPolicies(namespaceName, policies);
        } else {
            log.info("Namespace {} already exists.", namespaceName);
            var replicaClusterFound = false;
            var policiesOptional = namespaceResources.getPolicies(namespaceName);
            if (policiesOptional.isPresent() && policiesOptional.get().replication_clusters.contains(cluster)) {
                replicaClusterFound = true;
            }
            if (!replicaClusterFound) {
                namespaceResources.setPolicies(namespaceName, policies -> {
                    policies.replication_clusters.add(cluster);
                    return policies;
                });
                log.info("Updated namespace:{} policies. Added the replication cluster:{}",
                        namespaceName, cluster);
            }
        }
    }

    public static void createNamespaceIfAbsent(PulsarResources resources, NamespaceName namespaceName,
                                               String cluster) throws IOException {
        createNamespaceIfAbsent(resources, namespaceName, cluster, DEFAULT_BUNDLE_NUMBER);
    }

    static void createPartitionedTopic(MetadataStore configStore, TopicName topicName, int numPartitions)
            throws InterruptedException, IOException, ExecutionException {
        PulsarResources resources = new PulsarResources(null, configStore);
        NamespaceResources.PartitionedTopicResources partitionedTopicResources =
                resources.getNamespaceResources().getPartitionedTopicResources();

        Optional<PartitionedTopicMetadata> getResult =
                partitionedTopicResources.getPartitionedTopicMetadataAsync(topicName).get();
        if (!getResult.isPresent()) {
            partitionedTopicResources.createPartitionedTopic(topicName, new PartitionedTopicMetadata(numPartitions));
        } else {
            PartitionedTopicMetadata existsMeta = getResult.get();

            // Only update metadata if the partitions should be modified
            if (existsMeta.partitions < numPartitions) {
                partitionedTopicResources.updatePartitionedTopicAsync(topicName,
                        __ -> new PartitionedTopicMetadata(numPartitions)).get();
            }
        }
    }

    public static MetadataStoreExtended initLocalMetadataStore(String connection,
                                                               int sessionTimeout) throws Exception {
        return initLocalMetadataStore(connection, null, sessionTimeout);
    }

    public static MetadataStoreExtended initLocalMetadataStore(String connection,
                                                               String configPath,
                                                               int sessionTimeout) throws Exception {
        MetadataStoreExtended store = MetadataStoreExtended.create(connection, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeout)
                .configFilePath(configPath)
                .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                .build());
        if (store instanceof MetadataStoreLifecycle) {
            ((MetadataStoreLifecycle) store).initializeCluster().get();
        }
        return store;
    }

    public static MetadataStoreExtended initConfigMetadataStore(String connection,
                                                                int sessionTimeout)
            throws Exception {
        return initConfigMetadataStore(connection, null, sessionTimeout);
    }

    public static MetadataStoreExtended initConfigMetadataStore(String connection,
                                                                String configPath,
                                                                int sessionTimeout)
            throws Exception {
        MetadataStoreExtended store = MetadataStoreExtended.create(connection, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeout)
                .configFilePath(configPath)
                .metadataStoreName(MetadataStoreConfig.CONFIGURATION_METADATA_STORE)
                .build());
        if (store instanceof MetadataStoreLifecycle) {
            ((MetadataStoreLifecycle) store).initializeCluster().get();
        }
        return store;
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataSetup.class);
}
