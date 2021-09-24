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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat.SchemaLocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Teardown the metadata for a existed Pulsar cluster.
 */
public class PulsarClusterMetadataTeardown {

    private static class Arguments {
        @Parameter(names = { "-zk",
                "--zookeeper"}, description = "Local ZooKeeper quorum connection string", required = true)
        private String zookeeper;

        @Parameter(names = {
                "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Parameter(names = { "-c", "-cluster", "--cluster" }, description = "Cluster name")
        private String cluster;

        @Parameter(names = { "-cs", "--configuration-store" }, description = "Configuration Store connection string")
        private String configurationStore;

        @Parameter(names = { "--bookkeeper-metadata-service-uri" }, description = "Metadata service uri of BookKeeper")
        private String bkMetadataServiceUri;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static String[] localZkNodes = {
            "bookies", "counters", "loadbalance", "managed-ledgers", "namespace", "schemas", "stream" };

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
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("delete-cluster-metadata", arguments);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            jcommander.usage();
            throw e;
        }

        @Cleanup
        MetadataStoreExtended metadataStore = MetadataStoreExtended.create(arguments.zookeeper,
                MetadataStoreConfig.builder().sessionTimeoutMillis(arguments.zkSessionTimeoutMillis).build());

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
                    MetadataStoreConfig.builder().sessionTimeoutMillis(arguments.zkSessionTimeoutMillis).build());
            deleteRecursively(configMetadataStore, "/admin/clusters/" + arguments.cluster).join();
        }

        log.info("Cluster metadata for '{}' teardown.", arguments.cluster);
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
