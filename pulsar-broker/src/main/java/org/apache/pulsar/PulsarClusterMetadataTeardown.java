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
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

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

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) throws InterruptedException {
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
        }

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

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataTeardown.class);
}
