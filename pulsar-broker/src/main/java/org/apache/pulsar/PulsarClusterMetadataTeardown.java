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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat.SchemaLocator;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Teardown the metadata for a existed Pulsar cluster
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

        @Parameter(names = { "-c", "-cluster" }, description = "Cluster name")
        private String cluster;

        @Parameter(names = { "-cs", "--configuration-store" }, description = "Configuration Store connection string")
        private String configurationStore;

        @Parameter(names = { "--bookkeeper-metadata-service-uri" }, description = "Metadata service uri of BookKeeper", required = true)
        private String bkMetadataServiceUri;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) throws InterruptedException, IOException, BKException {
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

        ZooKeeper localZk = initZk(arguments.zookeeper, arguments.zkSessionTimeoutMillis);

        List<Long> ledgers = new ArrayList<>();
        ledgers.addAll(getManagedLedgers(localZk));
        ledgers.addAll(getSchemaLedgers(localZk));
        BookKeeper bookKeeper = new BookKeeper(new ClientConfiguration().setMetadataServiceUri(arguments.bkMetadataServiceUri));
        ledgers.forEach(ledger -> {
            try {
                bookKeeper.deleteLedger(ledger);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (BKException e) {
                log.warn("Failed to delete ledger {}: {}", ledger, e);
            }
        });
        bookKeeper.close();

        deleteZkNodeRecursively(localZk, "/bookies");
        deleteZkNodeRecursively(localZk, "/counters");
        deleteZkNodeRecursively(localZk, "/loadbalance");
        deleteZkNodeRecursively(localZk, "/managed-ledgers");
        deleteZkNodeRecursively(localZk, "/namespace");
        deleteZkNodeRecursively(localZk, "/schemas");
        deleteZkNodeRecursively(localZk, "/stream");

        if (arguments.configurationStore != null && arguments.cluster != null) {
            // Should it be done by REST API before broker is down?
            ZooKeeper configStoreZk = initZk(arguments.configurationStore, arguments.zkSessionTimeoutMillis);
            deleteZkNodeRecursively(configStoreZk, "/admin/clusters/" + arguments.cluster);
            configStoreZk.close();
        }

        localZk.close();
        log.info("Cluster metadata for '{}' teardown.", arguments.cluster);
    }

    public static ZooKeeper initZk(String connection, int sessionTimeout) throws InterruptedException {
        ZooKeeperClientFactory zkFactory = new ZookeeperClientFactoryImpl();
        try {
            return zkFactory.create(connection, ZooKeeperClientFactory.SessionType.ReadWrite, sessionTimeout).get();
        } catch (ExecutionException e) {
            log.error("Failed to connect to '{}': {}", connection, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void deleteZkNodeRecursively(ZooKeeper zooKeeper, String path) throws InterruptedException {
        try {
            ZKUtil.deleteRecursive(zooKeeper, path);
        } catch (KeeperException e) {
            log.warn("Failed to delete node {} from ZK [{}]: {}", path, zooKeeper, e);
        }
    }

    private static List<String> getChildren(ZooKeeper zooKeeper, String path) {
        try {
            return zooKeeper.getChildren(path, null);
        } catch (InterruptedException | KeeperException e) {
            log.error("Failed to get children of {}: {}", path, e);
            throw new RuntimeException(e);
        }
    }

    public static byte[] getData(ZooKeeper zooKeeper, String path) {
        try {
            return zooKeeper.getData(path, null, null);
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get data from {}: {}", path, e);
            throw new RuntimeException(e);
        }
    }

    public static List<Long> getManagedLedgers(ZooKeeper zooKeeper) {
        List<Long> ledgers = Collections.synchronizedList(new ArrayList<>());
        final String managedLedgersRoot = "/managed-ledgers";
        getChildren(zooKeeper, managedLedgersRoot).forEach(tenant -> {
            final String tenantRoot = managedLedgersRoot + "/" + tenant;
            getChildren(zooKeeper, tenantRoot).forEach(namespace -> {
                final String namespaceRoot = String.join("/", tenantRoot, namespace, "persistent");
                getChildren(zooKeeper, namespaceRoot).forEach(topic -> {
                    final String topicRoot = namespaceRoot + "/" + topic;
                    byte[] topicData = getData(zooKeeper, topicRoot);
                    try {
                        ManagedLedgerInfo ledgerInfo = ManagedLedgerInfo.parseFrom(topicData);
                        List<Long> topicLedgers = ledgerInfo.getLedgerInfoList().stream()
                                .map(ManagedLedgerInfo.LedgerInfo::getLedgerId)
                                .collect(Collectors.toList());
                        log.info("Detect topic ledgers: {} of {}", topicLedgers, topic);
                        ledgers.addAll(topicLedgers);

                        List<Long> cursorLedgers = getChildren(zooKeeper, topicRoot).stream().map(subscription -> {
                            final String subscriptionRoot = topicRoot + "/" +subscription;
                            try {
                                return ManagedCursorInfo.parseFrom(getData(zooKeeper, subscriptionRoot)).getCursorsLedgerId();
                            } catch (InvalidProtocolBufferException e) {
                                log.warn("Invalid data format from {}: {}", subscriptionRoot, e);
                                return null;
                            }
                            // some cursor ledger id could be set to -1 to mark deleted
                        }).filter(ledgerId -> (ledgerId != null && ledgerId >= 0)).collect(Collectors.toList());
                        if (!cursorLedgers.isEmpty()) {
                            log.info("Detect cursor ledgers: {} of {}", cursorLedgers, topicRoot);
                            ledgers.addAll(cursorLedgers);
                        }
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Invalid data format from {}: {}", topicRoot, e);
                    }
                });
            });
        });
        return ledgers;
    }

    public static List<Long> getSchemaLedgers(ZooKeeper zooKeeper) {
        List<Long> ledgers = Collections.synchronizedList(new ArrayList<>());
        final String schemaLedgersRoot = "/schemas";
        getChildren(zooKeeper, schemaLedgersRoot).forEach(tenant -> {
            final String tenantRoot = schemaLedgersRoot + "/" + tenant;
            getChildren(zooKeeper, tenantRoot).forEach(namespace -> {
                final String namespaceRoot = tenantRoot + "/" + namespace;
                getChildren(zooKeeper, namespaceRoot).forEach(topic -> {
                    final String topicRoot = namespaceRoot + "/" + topic;
                    byte[] data = getData(zooKeeper, topicRoot);
                    try {
                        SchemaLocator locator = SchemaLocator.parseFrom(data);
                        List<Long> schemaLedgers = locator.getIndexList().stream()
                                .map(indexEntry -> indexEntry.getPosition().getLedgerId())
                                .collect(Collectors.toList());
                        log.info("Detect schema ledgers: {} of {}", schemaLedgers, topicRoot);
                        ledgers.addAll(schemaLedgers);
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("Invalid data format from {}: {}", topicRoot, e);
                    }
                });
            });
        });
        return ledgers;
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataTeardown.class);
}
