/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar;

import java.io.IOException;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.util.ZkUtils;
import static org.apache.commons.lang3.StringUtils.isBlank;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

/**
 * Setup the metadata for a new Pulsar cluster
 */
public class PulsarClusterMetadataSetup {

    private static class Arguments {
        @Parameter(names = { "-c", "--cluster" }, description = "Cluster name", required = true)
        private String cluster;

        @Parameter(names = { "-u", "--service-url" }, description = "Service URL for new cluster", required = true)
        private String clusterServiceUrl;

        @Parameter(names = { "-t",
                "--service-url-tls" }, description = "Service URL for new cluster with TLS encryption", required = false)
        private String clusterServiceUrlTls;

        @Parameter(names = { "-zk",
                "--zookeeper" }, description = "Local ZooKeeper quorum connection string", required = true)
        private String zookeeper;
        
        @Parameter(names = { "-dzk",
                "--data-zookeeper" }, description = "Data ZooKeeper quorum connection string (Optional if data-zookeeper is not configured", required = false)
        private String dataZookeeper;

        @Parameter(names = { "-gzk",
                "--global-zookeeper" }, description = "Global ZooKeeper quorum connection string", required = true)
        private String globalZookeeper;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
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
            return;
        }

        arguments.dataZookeeper = isBlank(arguments.dataZookeeper) ? arguments.zookeeper : arguments.dataZookeeper;
        log.info("Setting up cluster {} with zk={} dzk={} global-zk={}", arguments.cluster, arguments.zookeeper,
                arguments.dataZookeeper, arguments.globalZookeeper);

        // Format BookKeeper metadata
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
        bkConf.setZkServers(arguments.dataZookeeper);
        if (!BookKeeperAdmin.format(bkConf, false /* interactive */, false /* force */)) {
            throw new IOException("Failed to initialize BookKeeper metadata");
        }

        ZooKeeperClientFactory zkfactory = new ZookeeperClientFactoryImpl();
        ZooKeeper localZk = zkfactory.create(arguments.zookeeper, SessionType.ReadWrite, 30000).get();
        // use localZk for dataZookeeper if dataZk is not provided or it is same as localZk
        ZooKeeper dataZk = arguments.zookeeper.equals(arguments.dataZookeeper) ? localZk
                : zkfactory.create(arguments.dataZookeeper, SessionType.ReadWrite, 30000).get();
        ZooKeeper globalZk = zkfactory.create(arguments.globalZookeeper, SessionType.ReadWrite, 30000).get();

        dataZk.create("/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        localZk.create("/namespace", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZkUtils.createFullPathOptimistic(globalZk, "/admin/policies", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        ZkUtils.createFullPathOptimistic(globalZk, "/admin/clusters", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        ClusterData clusterData = new ClusterData(arguments.clusterServiceUrl, arguments.clusterServiceUrlTls);
        byte[] clusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(clusterData);

        globalZk.create("/admin/clusters/" + arguments.cluster, clusterDataJson, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // Create marker for "global" cluster
        ClusterData globalClusterData = new ClusterData(null, null);
        byte[] globalClusterDataJson = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(globalClusterData);

        globalZk.create("/admin/clusters/global", globalClusterDataJson, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        log.info("Cluster metadata for '{}' setup correctly", arguments.cluster);
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarClusterMetadataSetup.class);
}
