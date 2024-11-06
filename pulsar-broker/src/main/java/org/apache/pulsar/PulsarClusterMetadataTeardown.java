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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat.SchemaLocator;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * Teardown the metadata for a existed Pulsar cluster.
 */
public class PulsarClusterMetadataTeardown {

    @Command(name = "delete-cluster-metadata", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class Arguments {
        @Option(names = { "-zk",
                "--zookeeper"}, description = "Local ZooKeeper quorum connection string")
        private String zookeeper;

        @Option(names = {"-md",
                "--metadata-store"}, description = "Metadata Store service url. eg: zk:my-zk:2181")
        private String metadataStoreUrl;

        @Option(names = {"-mscp",
                "--metadata-store-config-path"}, description = "Metadata Store config path")
        private String metadataStoreConfigPath;

        @Option(names = {
                "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Option(names = { "-c", "-cluster", "--cluster" }, description = "Cluster name")
        private String cluster;

        @Option(names = { "-cs", "--configuration-store" }, description = "Configuration Store connection string")
        private String configurationStore;

        @Option(names = {"-cmscp",
                "--configuration-metadata-store-config-path"}, description = "Configuration Metadata Store config path",
                hidden = false)
        private String configurationStoreConfigPath;

        @Option(names = { "--bookkeeper-metadata-service-uri" }, description = "Metadata service uri of BookKeeper")
        private String bkMetadataServiceUri;

        @Option(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static String[] localZkNodes = {
            "bookies", "counters", "loadbalance", "managed-ledgers", "namespace", "schemas", "stream" };

    public static void main(String[] args) throws Exception {
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
                cmd.addCommand("delete-cluster-metadata", commander);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            commander.getErr().println(e);
            throw e;
        }

        if (arguments.metadataStoreUrl == null && arguments.zookeeper == null) {
            commander.usage(commander.getOut());
            throw new IllegalArgumentException("Metadata store address argument is required (--metadata-store)");
        }

        if (arguments.metadataStoreUrl == null) {
            arguments.metadataStoreUrl = ZKMetadataStore.ZK_SCHEME_IDENTIFIER + arguments.zookeeper;
        }

        @Cleanup
        MetadataStoreExtended metadataStore = MetadataStoreExtended.create(arguments.metadataStoreUrl,
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis(arguments.zkSessionTimeoutMillis)
                        .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                        .configFilePath(arguments.metadataStoreConfigPath)
                        .build());

        if (arguments.bkMetadataServiceUri != null) {
            @Cleanup
            BookKeeper bookKeeper =
                    new BookKeeper(new ClientConfiguration().setMetadataServiceUri(arguments.bkMetadataServiceUri));

            @Cleanup("shutdown")
            ManagedLedgerFactory managedLedgerFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);

            deleteManagedLedgers(metadataStore, managedLedgerFactory);
            deleteSchemaLedgers(metadataStore, bookKeeper);
        }

        for (String localZkNode : localZkNodes) {
            deleteRecursively(metadataStore, "/" + localZkNode).join();
        }

        if (arguments.configurationStore != null && arguments.cluster != null) {
            // Should it be done by REST API before broker is down?
            @Cleanup
            MetadataStore configMetadataStore = MetadataStoreFactory.create(arguments.configurationStore,
                    MetadataStoreConfig.builder().sessionTimeoutMillis(arguments.zkSessionTimeoutMillis)
                            .configFilePath(arguments.configurationStoreConfigPath)
                            .metadataStoreName(MetadataStoreConfig.CONFIGURATION_METADATA_STORE).build());
            PulsarResources resources = new PulsarResources(metadataStore, configMetadataStore);
            // Cleanup replication cluster from all tenants and namespaces
            TenantResources tenantResources = resources.getTenantResources();
            NamespaceResources namespaceResources = resources.getNamespaceResources();
            List<String> tenants = tenantResources.listTenants();
            for (String tenant : tenants) {
                List<String> namespaces = namespaceResources.listNamespacesAsync(tenant).get();
                for (String namespace : namespaces) {
                    namespaceResources.setPolicies(NamespaceName.get(tenant, namespace), policies -> {
                        policies.replication_clusters.remove(arguments.cluster);
                        return policies;
                    });
                }
                removeCurrentClusterFromAllowedClusters(tenantResources, tenant, arguments.cluster);
            }
            try {
                resources.getClusterResources().deleteCluster(arguments.cluster);
            } catch (MetadataStoreException.NotFoundException ex) {
                // Ignore if the cluster does not exist
                log.info("Cluster metadata for '{}' does not exist.", arguments.cluster);
            }
        }

        log.info("Cluster metadata for '{}' teardown.", arguments.cluster);
    }

    private static void removeCurrentClusterFromAllowedClusters(
            TenantResources tenantResources, String tenant, String curCluster)
            throws MetadataStoreException, InterruptedException, ExecutionException {
        Optional<TenantInfo> tenantInfoOptional = tenantResources.getTenant(tenant);
        if (tenantInfoOptional.isEmpty()) {
            return;
        }
        tenantResources.updateTenantAsync(tenant, ti -> {
            ti.getAllowedClusters().remove(curCluster);
            return ti;
        }).get();
    }

    private static CompletableFuture<Void> deleteRecursively(MetadataStore metadataStore, String path) {
        return metadataStore.getChildren(path)
                .thenCompose(children -> FutureUtil.waitForAll(
                        children.stream()
                                .map(child -> deleteRecursively(metadataStore, path + "/" + child))
                                .collect(Collectors.toList())))
                .thenCompose(__ -> metadataStore.exists(path))
                .thenCompose(exists -> {
                    if (exists) {
                        return metadataStore.delete(path, Optional.empty());
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private static void deleteLedger(BookKeeper bookKeeper, long ledgerId) {
        try {
            bookKeeper.deleteLedger(ledgerId);
            if (log.isDebugEnabled()) {
                log.debug("Delete ledger id: {}", ledgerId);
            }
        } catch (InterruptedException | BKException e) {
            log.error("Failed to delete ledger {}: {}", ledgerId, e);
            throw new RuntimeException(e);
        }
    }

    private static void deleteManagedLedgers(MetadataStore metadataStore, ManagedLedgerFactory managedLedgerFactory) {
        final String managedLedgersRoot = "/managed-ledgers";
        metadataStore.getChildren(managedLedgersRoot).join().forEach(tenant -> {
            final String tenantRoot = managedLedgersRoot + "/" + tenant;
            metadataStore.getChildren(tenantRoot).join().forEach(namespace -> {
                final String namespaceRoot = String.join("/", tenantRoot, namespace, "persistent");
                metadataStore.getChildren(namespaceRoot).join().forEach(topic -> {
                    final TopicName topicName = TopicName.get(String.join("/", tenant, namespace, topic));
                    try {
                        managedLedgerFactory.delete(topicName.getPersistenceNamingEncoding());
                    } catch (InterruptedException | ManagedLedgerException e) {
                        log.error("Failed to delete ledgers of {}: {}", topicName, e);
                        throw new RuntimeException(e);
                    }
                });
            });
        });
    }

    private static void deleteSchemaLedgers(MetadataStore metadataStore, BookKeeper bookKeeper) {
        final String schemaLedgersRoot = "/schemas";
        metadataStore.getChildren(schemaLedgersRoot).join().forEach(tenant -> {
            final String tenantRoot = schemaLedgersRoot + "/" + tenant;
            metadataStore.getChildren(tenantRoot).join().forEach(namespace -> {
                final String namespaceRoot = tenantRoot + "/" + namespace;
                metadataStore.getChildren(namespaceRoot).join().forEach(topic -> {
                    final String topicRoot = namespaceRoot + "/" + topic;
                    try {
                        SchemaLocator.parseFrom(metadataStore.get(topicRoot).join().get().getValue())
                                .getIndexList().stream()
                                .map(indexEntry -> indexEntry.getPosition().getLedgerId())
                                .forEach(ledgerId -> deleteLedger(bookKeeper, ledgerId));
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Invalid data format from {}: {}", topicRoot, e);
                    }
                });
            });
        });
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataTeardown.class);
}
